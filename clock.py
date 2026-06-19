from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo

import httpx

from client import BASE
from config import Config


def server_now(tz: ZoneInfo, offset_s: float) -> datetime:
    """Local wall clock corrected to the server's clock (server_time = local + offset)."""
    return datetime.now(tz) + timedelta(seconds=offset_s)


async def sleep_until(when: datetime, offset_s: float = 0.0) -> None:
    """Sleep until the *server* clock reaches `when` (a server-time instant)."""
    delta = (when - server_now(when.tzinfo, offset_s)).total_seconds()
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
