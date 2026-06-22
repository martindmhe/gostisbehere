#!/usr/bin/env python3

from __future__ import annotations

import asyncio
import json
import os
import random
from datetime import timedelta

import httpx

from api_log import API_LOG_PATH, LOGS_DIR, log_request, log_response, reset_api_log
from client import (
    URL_DETAILS,
    URL_FIND,
    DetailsResult,
    book,
    find_body,
    find_slot,
    get_book_token,
    raise_for_resy,
)
from clock import calibrate_clock, server_now, sleep_until
from config import Config
from errors import RateLimited, TokenExpired
from slots import (
    build_config_token,
    direct_slot_start,
    drop_datetime,
    parse_slot_start,
    slot_token,
)


# ──────────────────────────────────────────────────────────────────────────────
# 5. Orchestration: pre-warm → poll → handshake → book
# ──────────────────────────────────────────────────────────────────────────────


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
            raise_for_resy(resp)
            print("[prewarm] connection established, auth accepted (warmed /details).")
        else:
            await client.post(URL_FIND, json=find_body(cfg))
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
    drop = drop_datetime(cfg)
    poll_start = drop - timedelta(seconds=cfg.poll_start_lead_s)
    deadline = drop + timedelta(seconds=cfg.poll_end_lag_s)

    await sleep_until(poll_start, cfg.clock_offset_s)
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
                token = slot_token(slot)
                if token:
                    found.set_result(slot)

    tasks: list[asyncio.Task] = []
    tick = 0
    loop = asyncio.get_event_loop()
    while not found.done() and server_now(drop.tzinfo, cfg.clock_offset_s) < deadline:
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
    drop = drop_datetime(cfg)
    poll_start = drop - timedelta(seconds=cfg.poll_start_lead_s)
    deadline = drop + timedelta(seconds=cfg.poll_end_lag_s)

    await sleep_until(poll_start, cfg.clock_offset_s)
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
    while not found.done() and server_now(drop.tzinfo, cfg.clock_offset_s) < deadline:
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
        dump = os.path.join(LOGS_DIR, "details_dump.json")
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


async def poll_book(client: httpx.AsyncClient, cfg: Config, book_token: str) -> dict:
    # Retry /book until it succeeds or the polling window closes.
    drop = drop_datetime(cfg)
    deadline = drop + timedelta(seconds=cfg.poll_end_lag_s)
    last_debug: dict = {}

    while True:
        try:
            result = await book(
                client, cfg, book_token,
                dump_on_missing=False, debug_sink=last_debug,
            )
        except RateLimited:
            result = None
            await asyncio.sleep(0.75)
        if result is not None:
            return result
        if server_now(drop.tzinfo, cfg.clock_offset_s) >= deadline:
            break
        await asyncio.sleep(max(0.05, cfg.poll_interval_s))

    if last_debug:
        dump = os.path.join(LOGS_DIR, "book_dump.json")
        try:
            with open(dump, "w") as f:
                json.dump(last_debug, f, indent=2, default=str)
        except OSError:
            pass
        print(f"[book] last /book response: HTTP {last_debug.get('status')} — "
              f"{str(last_debug.get('body'))[:500]}")
        print(f"[book] full response written to {dump}")
    raise TimeoutError(
        "No successful /book inside the polling window — repeatedly got 404 (slot never "
        "went live in time) before the window closed."
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
    drop = drop_datetime(cfg)

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
                await sleep_until(recal_at, cfg.clock_offset_s)
                print(f"[clock] re-calibrating ({cfg.clock_sync_recalibrate_lead_s:.0f}s before drop)...")
                cfg.clock_offset_s = await calibrate_clock(client, cfg)

        # --- pre-warm a few seconds before the drop ---
        await sleep_until(drop - timedelta(seconds=cfg.prewarm_lead_s), cfg.clock_offset_s)
        await prewarm(client, cfg)

        if cfg.config_token:
            # --- DIRECT path: skip /find, poll /details with the constructed token ---
            config_token = cfg.config_token
            start = direct_slot_start(cfg)
            print(f"[direct] constructed config token: {config_token}")
            print(f"[direct] POST {URL_DETAILS}")
            print(f"[direct] headers: {json.dumps(cfg.headers, indent=2)}")
            print(f"[direct] body: {json.dumps({'commit': cfg.commit, 'config_id': config_token, 'day': cfg.day, 'party_size': cfg.party_size}, indent=2)}")
            details = await poll_book_token_direct(client, cfg)
            print(f"[details] book_token minted for slot @ {start} (config_id supplied).")
        else:
            # --- poll → first matching slot, then one details handshake ---
            slot = await poll_for_slot(client, cfg)
            config_token = slot_token(slot)
            start = parse_slot_start(slot)
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
        result = await poll_book(client, cfg, book_token)
        print("\Booking Successful")
        print(f"slot time     : {start}")
        print(f"reservation_id: {result.get('reservation_id')}")
        print(f"resy_token    : {result.get('resy_token')}")


if __name__ == "__main__":
    # Imported here (not at module level) so cli.py's `from resy_booker import ...`
    # sees a fully-defined module instead of a circular import mid-initialization.
    from cli import main
    main()
