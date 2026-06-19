#!/usr/bin/env python3

from __future__ import annotations

import asyncio
import json
import os
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo

import httpx

from api_log import API_LOG_PATH, log_request, log_response, reset_api_log
from config import Config
from errors import RateLimited, TokenExpired


# ──────────────────────────────────────────────────────────────────────────────
# 2. Endpoints  (>>> VERIFY versions: find is /4 today, was /2 historically)
# ──────────────────────────────────────────────────────────────────────────────

BASE = "https://api.resy.com"
URL_FIND = f"{BASE}/4/find"          # POST (JSON body) — confirmed from capture
URL_DETAILS = f"{BASE}/3/details"    # POST (JSON body) — returns book_token
URL_BOOK = f"{BASE}/3/book"          # POST (JSON body) — returns resy_token


# ──────────────────────────────────────────────────────────────────────────────
# 3. Helpers
# ──────────────────────────────────────────────────────────────────────────────


def _drop_datetime(cfg: Config) -> datetime:
    tz = ZoneInfo(cfg.drop_tz)
    if cfg.now:
        return datetime.now(tz)
    d = datetime.strptime(cfg.drop_date, "%Y-%m-%d").date()
    return datetime(
        d.year, d.month, d.day,
        cfg.drop_hour, cfg.drop_minute, cfg.drop_second, tzinfo=tz,
    )


def _parse_slot_start(slot: dict) -> datetime | None:
    """
    Pull the slot's start time. Confirmed shape:
        slot["date"]["start"] == "2026-06-13 20:00:00"
    """
    try:
        raw = slot["date"]["start"]
        return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
    except (KeyError, TypeError, ValueError):
        return None


def _slot_matches(slot_start: datetime, cfg: Config) -> int | None:
    """Return priority index (lower = better) if slot is within tolerance, else None."""
    tol = timedelta(minutes=cfg.time_tolerance_min)
    for priority, pref in enumerate(cfg.preferred_times):
        h, m = map(int, pref.split(":"))
        target = slot_start.replace(hour=h, minute=m, second=0, microsecond=0)
        if abs(slot_start - target) <= tol:
            return priority
    return None


def _pick_best_slot(find_json: dict, cfg: Config) -> dict | None:
    """
    Walk find() results, return the highest-priority matching slot dict.
    Confirmed path: results.venues[].slots[]
    """
    try:
        venues = find_json["results"]["venues"]
    except (KeyError, TypeError):
        return None

    best: tuple[int, dict] | None = None
    for venue in venues:
        for slot in venue.get("slots", []):
            start = _parse_slot_start(slot)
            if start is None:
                continue
            pri = _slot_matches(start, cfg)
            if pri is None:
                continue
            if best is None or pri < best[0]:
                best = (pri, slot)
    return best[1] if best else None


def _slot_token(slot: dict) -> str | None:
    # Confirmed: slot["config"]["token"] is the rgs:// config_id passed to /details.
    # The token encodes the slot time, so each time has a distinct token.
    return (slot.get("config") or {}).get("token")


def _build_config_token(cfg: Config) -> str:
    # build rgs://resy/<venue>/<config_id>/<mid_field>/<day>/<day>/<time>/<party>/<seating>
    return (
        f"rgs://resy/{cfg.venue_id}/{cfg.config_id}/{cfg.mid_field}/"
        f"{cfg.day}/{cfg.day}/{cfg.time}/{cfg.party_size}/{cfg.seating}"
    )


def _direct_slot_start(cfg: Config) -> datetime | None:
    # build the slot's start datetime from the run's own params
    try:
        return datetime.strptime(f"{cfg.day} {cfg.time}", "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _raise_for_resy(resp: httpx.Response) -> None:
    if resp.status_code == 429:
        raise RateLimited(f"429 from {resp.url}")
    if resp.status_code in (401, 403):
        # Cloudflare often returns 403 with an HTML challenge body.
        if "text/html" in resp.headers.get("content-type", ""):
            raise RateLimited(f"Cloudflare challenge ({resp.status_code}) at {resp.url}")
        raise TokenExpired(f"{resp.status_code} from {resp.url}: {resp.text[:200]}")




def _find_body(cfg: Config) -> dict:
    return {
        "lat": cfg.lat,
        "long": cfg.long,
        "day": cfg.day,
        "party_size": cfg.party_size,
        "venue_id": cfg.venue_id,
    }


async def find_slot(client: httpx.AsyncClient, cfg: Config) -> dict | None:
    resp = await client.post(URL_FIND, json=_find_body(cfg))
    _raise_for_resy(resp)
    if not (200 <= resp.status_code < 300):  # accept any 2xx, not just 200
        return None
    return _pick_best_slot(resp.json(), cfg)


@dataclass
class DetailsResult:
    """What /details tells us, beyond the bare token."""
    book_token: str
    payment_required: bool          # venue takes a card hold/deposit at booking
    total_due: float | None         # payment.amounts.total (e.g. 108.88), if any
    default_payment_method_id: int | None  # user's default card from this response
    policy: list[str] = field(default_factory=list)  # human-readable cancel/deposit text


def _parse_details(payload: dict) -> DetailsResult:
    # CONFIRMED from capture: book_token is a top-level object {"value": ..., "date_expires"}.
    # commit:0 DOES return it (no need for commit:1).
    bt = payload.get("book_token")
    token = bt.get("value") if isinstance(bt, dict) else bt
    if not token:
        raise RuntimeError(
            "No book_token in /details response. Top-level keys were: "
            f"{sorted(payload.keys())}."
        )

    # payment.amounts.total > 0 => a deposit/charge is taken at booking (e.g. Carbone).
    amounts = (payload.get("payment") or {}).get("amounts") or {}
    total = amounts.get("total")
    charge = amounts.get("reservation_charge")
    payment_required = bool(total) or bool(charge)

    # The user's saved cards live under user.payment_methods[]; pick the default.
    methods = (payload.get("user") or {}).get("payment_methods") or []
    default_pm = next((m.get("id") for m in methods if m.get("is_default")), None)
    if default_pm is None and methods:
        default_pm = methods[0].get("id")

    policy = ((payload.get("cancellation") or {}).get("display") or {}).get("policy") or []
    return DetailsResult(
        book_token=token,
        payment_required=payment_required,
        total_due=total,
        default_payment_method_id=default_pm,
        policy=policy,
    )


async def get_book_token(
    client: httpx.AsyncClient,
    cfg: Config,
    config_token: str,
    *,
    dump_on_missing: bool = True,
    debug_sink: dict | None = None,
) -> DetailsResult | None:
    # details body — JSON. `commit: 0` matches the real browser capture (preview,
    # don't lock). config_id is the rgs:// slot token from find().
    body = {
        "commit": cfg.commit,
        "config_id": config_token,
        "day": cfg.day,
        "party_size": cfg.party_size,
    }
    resp = await client.post(URL_DETAILS, json=body)
    _raise_for_resy(resp)

    # CONFIRMED from capture: /details with commit:1 returns 201 Created (it mints a
    # booking hold), and commit:0 returns 200 — so accept any 2xx. When polling directly
    # (no /find), a not-yet-live or already-gone config comes back as a 4xx/5xx (e.g.
    # 400/404/410). In that mode we treat "no token yet" as a retry signal (return None)
    # rather than a hard failure, so the caller's poll loop keeps trying until inventory
    # appears. We still record the raw response in debug_sink so a window that closes
    # empty can tell us WHY (bad headers vs gating vs wrong token) instead of failing blind.
    if not (200 <= resp.status_code < 300):
        if debug_sink is not None:
            debug_sink.update(status=resp.status_code, body=resp.text[:2000], request_body=body)
        if dump_on_missing:
            resp.raise_for_status()
        return None

    payload = resp.json()
    try:
        return _parse_details(payload)
    except RuntimeError:
        if debug_sink is not None:
            debug_sink.update(status=200, body=payload, request_body=body)
        if not dump_on_missing:
            return None
        # Token missing — dump the full response so we can see what Resy actually
        # returned (commit semantics, gating, GDA, etc.) instead of guessing.
        dump = os.path.join(os.path.dirname(os.path.abspath(__file__)), "details_dump.json")
        with open(dump, "w") as f:
            json.dump({"request_body": body, "response": payload}, f, indent=2)
        print(f"[details] no book_token — full response written to {dump}")
        raise


async def book(client: httpx.AsyncClient, cfg: Config, book_token: str) -> dict:
    # CONFIRMED from capture: /book is application/x-www-form-urlencoded (data=, NOT
    # json=). struct_payment_method is a JSON-STRING field value — e.g. {"id":35973316} —
    # and is only sent for venues that take a card hold/deposit (payment_method_id != 0).
    body: dict = {
        "book_token": book_token,
        "source_id": "resy.com-venue-details",
        "venue_marketing_opt_in": 0,
    }
    if cfg.requires_payment or cfg.payment_method_id:
        # MUST be a JSON STRING literal field value, compact (no spaces) to match the
        # real /book capture exactly:  struct_payment_method={"id":35973316}
        body["struct_payment_method"] = json.dumps(
            {"id": cfg.payment_method_id}, separators=(",", ":")
        )

    resp = await client.post(URL_BOOK, data=body)
    _raise_for_resy(resp)
    if not (200 <= resp.status_code < 300):
        # Dump the raw response so a failed /book tells us WHY (stale token, bad
        # field, venue-side rejection) instead of a bare status-code traceback.
        dump = os.path.join(os.path.dirname(os.path.abspath(__file__)), "book_dump.json")
        with open(dump, "w") as f:
            json.dump(
                {"status": resp.status_code, "request_body": body, "response": resp.text},
                f, indent=2,
            )
        print(f"[book] HTTP {resp.status_code} — full response written to {dump}")
        resp.raise_for_status()
    payload = resp.json()
    # Confirmed response shape: {resy_token, reservation_id, venue_opt_in}
    if not payload.get("resy_token"):
        raise RuntimeError(f"Book returned no resy_token: {json.dumps(payload)[:300]}")
    return payload


# ──────────────────────────────────────────────────────────────────────────────
# 5. Orchestration: pre-warm → poll → handshake → book
# ──────────────────────────────────────────────────────────────────────────────


def _server_now(tz: ZoneInfo, offset_s: float) -> datetime:
    """Local wall clock corrected to the server's clock (server_time = local + offset)."""
    return datetime.now(tz) + timedelta(seconds=offset_s)


async def _sleep_until(when: datetime, offset_s: float = 0.0) -> None:
    """Sleep until the *server* clock reaches `when` (a server-time instant)."""
    delta = (when - _server_now(when.tzinfo, offset_s)).total_seconds()
    if delta > 0:
        await asyncio.sleep(delta)


async def calibrate_clock(client: httpx.AsyncClient, cfg: Config) -> float:
    """
    Estimate (server_time - local_time) in seconds so the drop schedule rides Resy's
    clock instead of ours.

    The HTTP `Date` header is whole-second resolution, so a single read only pins us to
    ±1s. We instead fire a tight burst of cheap probes and watch for the second value to
    *tick over*: the instant it crosses from N to N+1, the server clock is exactly N+1.000.
    That tick is bracketed between the two surrounding probes, so its local time is the
    midpoint of their midpoints — sub-RTT precision. We collect several ticks and take the
    median offset of the lowest-RTT (cleanest) samples.

    Returns 0.0 on any failure, which leaves the bot on the bare local clock (prior behavior).
    """
    loop = asyncio.get_event_loop()
    deadline = loop.time() + cfg.clock_sync_max_s
    samples: list[tuple[float, float]] = []  # (offset_s, rtt_s)
    prev_secs: int | None = None
    prev_mid: float | None = None

    while loop.time() < deadline:
        t0 = time.time()
        try:
            # HEAD against the API host: Cloudflare stamps a fresh `Date` on every
            # response (even errors), and HEAD carries no body — a near-free probe.
            resp = await client.head(BASE + "/")
        except Exception:  # noqa: BLE001 — a flaky probe just costs us one sample
            await asyncio.sleep(cfg.clock_sync_interval_s)
            continue
        t1 = time.time()
        mid = (t0 + t1) / 2

        date_hdr = resp.headers.get("date")
        secs: int | None = None
        if date_hdr:
            try:
                secs = int(parsedate_to_datetime(date_hdr).timestamp())
            except (TypeError, ValueError, OverflowError):
                secs = None

        if secs is not None:
            if prev_secs is not None and prev_mid is not None and secs == prev_secs + 1:
                # The tick into `secs`.000 happened between the two probes' midpoints.
                tick_local = (prev_mid + mid) / 2
                samples.append((secs - tick_local, t1 - t0))
            prev_secs, prev_mid = secs, mid

        await asyncio.sleep(cfg.clock_sync_interval_s)

    if len(samples) < cfg.clock_sync_min_edges:
        print(f"[clock] only caught {len(samples)} tick(s) "
              f"(< {cfg.clock_sync_min_edges}) — staying on local clock.")
        return 0.0

    # Prefer the lowest-RTT half (least path noise), then take the median offset.
    samples.sort(key=lambda s: s[1])
    offsets = sorted(o for o, _ in samples[: max(1, len(samples) // 2)])
    offset = offsets[len(offsets) // 2]
    print(f"[clock] offset = {offset * 1000:+.0f} ms (server - local), "
          f"from {len(samples)} tick(s); driving the drop schedule off server time.")
    if abs(offset) > 1.0:
        print(f"[clock] WARNING: local clock is off by {offset:+.2f}s — "
              "without this correction the bot would have been that far late/early.")
    return offset


async def prewarm(client: httpx.AsyncClient, cfg: Config) -> None:
    """
    Force the TCP + TLS handshake (and HTTP/2 session) to be live before the drop, so
    the first real request pays no connection-setup cost. The warm-up doubles as an auth
    check — a dead JWT fails here, before timing matters.

    In --config-id mode we warm the exact endpoint we'll hammer (/details, commit:0
    preview) so we never touch /find; otherwise we warm with a lightweight /find.
    """
    try:
        if cfg.config_token:
            # commit:0 = preview (no reserve-intent, no lock). Pre-drop this returns no
            # book_token, which is fine — we only need the connection warm + auth checked.
            resp = await client.post(URL_DETAILS, json={
                "commit": 0,
                "config_id": cfg.config_token,
                "day": cfg.day,
                "party_size": cfg.party_size,
            })
            _raise_for_resy(resp)
            print("[prewarm] connection established, auth accepted (warmed /details).")
        else:
            await client.post(URL_FIND, json=_find_body(cfg))
            print("[prewarm] connection established, auth accepted.")
    except TokenExpired:
        raise  # do not start the run with a dead token
    except RateLimited:
        print("[prewarm] rate-limited during warm-up — easing off before the drop.")
    except Exception as e:  # noqa: BLE001
        print(f"[prewarm] warm-up request failed (non-fatal): {e!r}")


async def poll_for_slot(client: httpx.AsyncClient, cfg: Config) -> dict:
    """
    Run a moderate-cadence polling loop across the timing window. Multiple finds
    may be in flight (capped by a semaphore); the first matching slot wins and
    cancels the rest.
    """
    drop = _drop_datetime(cfg)
    poll_start = drop - timedelta(seconds=cfg.poll_start_lead_s)
    deadline = drop + timedelta(seconds=cfg.poll_end_lag_s)

    await _sleep_until(poll_start, cfg.clock_offset_s)
    print(f"[poll] window open. drop={drop.isoformat()} deadline={deadline.time()}")

    found: asyncio.Future[dict] = asyncio.get_event_loop().create_future()
    sem = asyncio.Semaphore(cfg.max_inflight_finds)
    backoff_until = 0.0  # monotonic-ish guard set when we get 429'd

    async def one_attempt() -> None:
        nonlocal backoff_until
        async with sem:
            if found.done():
                return
            try:
                slot = await find_slot(client, cfg)
            except RateLimited:
                # Back off briefly; do NOT escalate request rate.
                backoff_until = asyncio.get_event_loop().time() + 0.75
                return
            except TokenExpired:
                if not found.done():
                    found.set_exception(TokenExpired("auth rejected mid-poll"))
                return
            except (httpx.TimeoutException, httpx.TransportError):
                return  # transient — let the next tick retry
            if slot and not found.done():
                token = _slot_token(slot)
                if token:
                    found.set_result(slot)

    tasks: list[asyncio.Task] = []
    tick = 0
    loop = asyncio.get_event_loop()
    while not found.done() and _server_now(drop.tzinfo, cfg.clock_offset_s) < deadline:
        if loop.time() >= backoff_until:
            tasks.append(asyncio.create_task(one_attempt()))
            tick += 1
        gap = cfg.poll_interval_s + random.uniform(-cfg.poll_jitter_s, cfg.poll_jitter_s)
        await asyncio.sleep(max(0.05, gap))

    # window closed or slot found — stop outstanding work
    for t in tasks:
        if not t.done():
            t.cancel()

    if found.done():
        if found.exception():
            raise found.exception()  # type: ignore[misc]
        return found.result()
    raise TimeoutError("No matching slot appeared inside the polling window.")


async def poll_book_token_direct(client: httpx.AsyncClient, cfg: Config) -> DetailsResult:
    """
    --config-id path: SKIP /find and hammer /details with the known config token until
    inventory goes live and it mints a book_token. Same window/cadence/backoff as
    poll_for_slot, but the unit of work is a /details call and the prize is a
    DetailsResult (not a slot dict). First success wins and cancels the rest.
    """
    drop = _drop_datetime(cfg)
    poll_start = drop - timedelta(seconds=cfg.poll_start_lead_s)
    deadline = drop + timedelta(seconds=cfg.poll_end_lag_s)

    await _sleep_until(poll_start, cfg.clock_offset_s)
    print(f"[poll] window open — DIRECT /details (no /find). "
          f"drop={drop.isoformat()} deadline={deadline.time()}")

    found: asyncio.Future[DetailsResult] = asyncio.get_event_loop().create_future()
    sem = asyncio.Semaphore(cfg.max_inflight_finds)
    backoff_until = 0.0
    last_debug: dict = {}  # raw response of the most recent token-less attempt

    async def one_attempt() -> None:
        nonlocal backoff_until
        async with sem:
            if found.done():
                return
            try:
                # dump_on_missing=False: "no token yet" is the expected pre-live state,
                # so it returns None and we just retry next tick instead of bailing.
                # debug_sink captures the raw response so an empty window isn't blind.
                details = await get_book_token(
                    client, cfg, cfg.config_token,
                    dump_on_missing=False, debug_sink=last_debug,
                )
            except RateLimited:
                backoff_until = asyncio.get_event_loop().time() + 0.75
                return
            except TokenExpired:
                if not found.done():
                    found.set_exception(TokenExpired("auth rejected mid-poll"))
                return
            except (httpx.TimeoutException, httpx.TransportError):
                return
            except Exception as e:  # noqa: BLE001 — record, don't let it vanish into the task
                last_debug.update(error=repr(e))
                return
            if details and not found.done():
                found.set_result(details)

    tasks: list[asyncio.Task] = []
    loop = asyncio.get_event_loop()
    while not found.done() and _server_now(drop.tzinfo, cfg.clock_offset_s) < deadline:
        if loop.time() >= backoff_until:
            tasks.append(asyncio.create_task(one_attempt()))
        gap = cfg.poll_interval_s + random.uniform(-cfg.poll_jitter_s, cfg.poll_jitter_s)
        await asyncio.sleep(max(0.05, gap))

    for t in tasks:
        if not t.done():
            t.cancel()

    if found.done():
        if found.exception():
            raise found.exception()  # type: ignore[misc]
        return found.result()

    # Window closed empty. Surface the last raw /details response so we can see WHY
    # (bad/missing auth header, gating, wrong token) instead of guessing.
    if last_debug:
        dump = os.path.join(os.path.dirname(os.path.abspath(__file__)), "details_dump.json")
        try:
            with open(dump, "w") as f:
                json.dump(last_debug, f, indent=2, default=str)
        except OSError:
            pass
        status = last_debug.get("status")
        body = last_debug.get("body")
        snippet = body if isinstance(body, str) else json.dumps(body, default=str)
        print(f"[direct] last /details response: HTTP {status} — {str(snippet)[:500]}")
        print(f"[direct] full response written to {dump}")
    raise TimeoutError(
        "No book_token from /details inside the polling window. See the [direct] response "
        "above — a 4xx usually means an auth header / token problem, a 200 without a "
        "book_token means gating. (No /find fallback in --config-id mode.)"
    )


async def probe_details(client: httpx.AsyncClient, cfg: Config) -> None:
    """
    One-shot diagnostic: fire a single /details for the constructed token RIGHT NOW
    (no drop wait, no booking) and report exactly what Resy returns. Use it against a
    currently-bookable slot to confirm the token pieces + auth headers before a drop.
    """
    print(f"[probe] config token: {cfg.config_token}")
    print(f"[probe] body: commit={cfg.commit} day={cfg.day} party_size={cfg.party_size}")
    sink: dict = {}
    try:
        details = await get_book_token(
            client, cfg, cfg.config_token, dump_on_missing=False, debug_sink=sink
        )
    except TokenExpired as e:
        print(f"[probe] AUTH REJECTED (401/403) — header/token problem: {e}")
        return
    except RateLimited as e:
        print(f"[probe] RATE LIMITED / Cloudflare challenge: {e}")
        return

    if details:
        print("[probe] SUCCESS ✓ — /details minted a book_token with this exact token.")
        print(f"        book_token      : {details.book_token[:60]}…")
        print(f"        payment_required: {details.payment_required}"
              + (f" (total ${details.total_due})" if details.payment_required else ""))
        return

    status = sink.get("status")
    body = sink.get("body")
    snippet = body if isinstance(body, str) else json.dumps(body, default=str)
    print(f"[probe] NO book_token — HTTP {status}")
    print(f"        response: {str(snippet)[:1000]}")
    if status and status != 200:
        print("        → 4xx/5xx with a correct body usually means an AUTH HEADER issue "
              "(api_key / JWT / a header the browser sends that we don't).")
    else:
        print("        → 200 but no token means gating (commit semantics, GDA, or the slot "
              "isn't actually bookable right now).")


async def run(cfg: Config) -> None:
    cfg.headers = cfg.build_headers()
    limits = httpx.Limits(max_keepalive_connections=8, max_connections=16)
    if cfg.log_requests:
        reset_api_log()
        print(f"[log] full request/response log: {API_LOG_PATH}")
    if cfg.now:
        print(f"[now] running immediately")
    drop = _drop_datetime(cfg)

    # HTTP/2 keeps one warm connection multiplexed for the in-flight finds. If the
    # `h2` package isn't installed, httpx raises at client creation — fall back to
    # HTTP/1.1 rather than crash (slightly worse during the drop, still works).
    try:
        import h2  # noqa: F401
        use_http2 = True
    except ImportError:
        use_http2 = False
        print("[setup] h2 not installed — using HTTP/1.1 (pip install 'httpx[http2]' for HTTP/2).")

    async with httpx.AsyncClient(
        headers=cfg.headers,
        http2=use_http2,
        timeout=cfg.request_timeout_s,
        limits=limits,
        event_hooks=(
            {"request": [log_request], "response": [log_response]}
            if cfg.log_requests else {}
        ),
    ) as client:
        # --- probe: one immediate /details, report, exit (no drop wait, no booking) ---
        if cfg.probe:
            if not cfg.config_token:
                raise SystemExit("--probe needs a constructed token (set config_id + "
                                 "mid_field + time + seating in the spec/flags).")
            await probe_details(client, cfg)
            return

        # --- calibrate, then re-calibrate ~1 min before the drop ---
        if cfg.clock_sync:
            print("Calibrating clock...")
            cfg.clock_offset_s = await calibrate_clock(client, cfg)

            if cfg.clock_sync_recalibrate_lead_s > 0 and not cfg.now:
                recal_at = drop - timedelta(seconds=cfg.clock_sync_recalibrate_lead_s)
                await _sleep_until(recal_at, cfg.clock_offset_s)
                print(f"[clock] re-calibrating ({cfg.clock_sync_recalibrate_lead_s:.0f}s before drop)...")
                cfg.clock_offset_s = await calibrate_clock(client, cfg)

        # --- pre-warm a few seconds before the drop ---
        await _sleep_until(drop - timedelta(seconds=cfg.prewarm_lead_s), cfg.clock_offset_s)
        await prewarm(client, cfg)

        if cfg.config_token:
            # --- DIRECT path: skip /find, poll /details with the constructed token ---
            config_token = cfg.config_token
            start = _direct_slot_start(cfg)
            print(f"[direct] constructed config token: {config_token}")
            print(f"[direct] POST {URL_DETAILS}")
            print(f"[direct] headers: {json.dumps(cfg.headers, indent=2)}")
            print(f"[direct] body: {json.dumps({'commit': cfg.commit, 'config_id': config_token, 'day': cfg.day, 'party_size': cfg.party_size}, indent=2)}")
            details = await poll_book_token_direct(client, cfg)
            print(f"[details] book_token minted for slot @ {start} (config_id supplied).")
        else:
            # --- poll → first matching slot, then one details handshake ---
            slot = await poll_for_slot(client, cfg)
            config_token = _slot_token(slot)
            start = _parse_slot_start(slot)
            print(f"[find] matched slot @ {start} — piping to details.")
            details = await get_book_token(client, cfg, config_token)

        assert details is not None  # both paths above only return on a real book_token
        book_token = details.book_token

        # If this venue takes a deposit, make sure we have a card to charge. Prefer
        # an explicitly configured payment_method_id; otherwise fall back to the
        # default card /details just handed us. Resolve this BEFORE /book so we
        # never lose a slot to a missing payment method.
        if details.payment_required:
            pm = cfg.payment_method_id or details.default_payment_method_id
            if not pm:
                raise RuntimeError(
                    "Venue requires a deposit but no payment method is available. "
                    "Set RESY_PAYMENT_METHOD_ID / --payment-method-id."
                )
            cfg.payment_method_id = pm
            print(f"[details] deposit required: total ${details.total_due} — "
                  f"charging payment method {pm}")
            for line in details.policy:
                print(f"          policy: {line}")

        if cfg.dry_run:
            print("\nDRY RUN — pipeline reached /book and stopped (no reservation made).")
            print(f"slot time      : {start}")
            print(f"config_id      : {config_token}")
            print(f"book_token     : {book_token[:60]}…")
            print(f"payment_required: {details.payment_required}"
                  + (f" (${details.total_due}, pm={cfg.payment_method_id})"
                     if details.payment_required else ""))
            return

        # --- finalize ---
        result = await book(client, cfg, book_token)
        print("\Booking Successful")
        print(f"slot time     : {start}")
        print(f"reservation_id: {result.get('reservation_id')}")
        print(f"resy_token    : {result.get('resy_token')}")


if __name__ == "__main__":
    # Imported here (not at module level) so cli.py's `from resy_booker import ...`
    # sees a fully-defined module instead of a circular import mid-initialization.
    from cli import main
    main()
