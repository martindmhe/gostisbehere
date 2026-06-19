from __future__ import annotations

import json
import os
from dataclasses import dataclass, field

import httpx

from api_log import LOGS_DIR
from config import Config
from errors import RateLimited, TokenExpired
from slots import pick_best_slot

BASE = "https://api.resy.com"
URL_FIND = f"{BASE}/4/find"          # POST (JSON body) — confirmed from capture
URL_DETAILS = f"{BASE}/3/details"    # POST (JSON body) — returns book_token
URL_BOOK = f"{BASE}/3/book"          # POST (JSON body) — returns resy_token


def raise_for_resy(resp: httpx.Response) -> None:
    if resp.status_code == 429:
        raise RateLimited(f"429 from {resp.url}")
    if resp.status_code in (401, 403):
        # Cloudflare often returns 403 with an HTML challenge body.
        if "text/html" in resp.headers.get("content-type", ""):
            raise RateLimited(f"Cloudflare challenge ({resp.status_code}) at {resp.url}")
        raise TokenExpired(f"{resp.status_code} from {resp.url}: {resp.text[:200]}")


def find_body(cfg: Config) -> dict:
    return {
        "lat": cfg.lat,
        "long": cfg.long,
        "day": cfg.day,
        "party_size": cfg.party_size,
        "venue_id": cfg.venue_id,
    }


async def find_slot(client: httpx.AsyncClient, cfg: Config) -> dict | None:
    resp = await client.post(URL_FIND, json=find_body(cfg))
    raise_for_resy(resp)
    if not (200 <= resp.status_code < 300):  # accept any 2xx, not just 200
        return None
    return pick_best_slot(resp.json(), cfg)


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
    raise_for_resy(resp)

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
        dump = os.path.join(LOGS_DIR, "details_dump.json")
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
    raise_for_resy(resp)
    if not (200 <= resp.status_code < 300):
        # Dump the raw response so a failed /book tells us WHY (stale token, bad
        # field, venue-side rejection) instead of a bare status-code traceback.
        dump = os.path.join(LOGS_DIR, "book_dump.json")
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
