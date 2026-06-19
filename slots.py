from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from config import Config


def drop_datetime(cfg: Config) -> datetime:
    tz = ZoneInfo(cfg.drop_tz)
    if cfg.now:
        return datetime.now(tz)
    d = datetime.strptime(cfg.drop_date, "%Y-%m-%d").date()
    return datetime(
        d.year, d.month, d.day,
        cfg.drop_hour, cfg.drop_minute, cfg.drop_second, tzinfo=tz,
    )


def parse_slot_start(slot: dict) -> datetime | None:
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


def pick_best_slot(find_json: dict, cfg: Config) -> dict | None:
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
            start = parse_slot_start(slot)
            if start is None:
                continue
            pri = _slot_matches(start, cfg)
            if pri is None:
                continue
            if best is None or pri < best[0]:
                best = (pri, slot)
    return best[1] if best else None


def slot_token(slot: dict) -> str | None:
    # Confirmed: slot["config"]["token"] is the rgs:// config_id passed to /details.
    # The token encodes the slot time, so each time has a distinct token.
    return (slot.get("config") or {}).get("token")


def build_config_token(cfg: Config) -> str:
    # build rgs://resy/<venue>/<config_id>/<mid_field>/<day>/<day>/<time>/<party>/<seating>
    return (
        f"rgs://resy/{cfg.venue_id}/{cfg.config_id}/{cfg.mid_field}/"
        f"{cfg.day}/{cfg.day}/{cfg.time}/{cfg.party_size}/{cfg.seating}"
    )


def direct_slot_start(cfg: Config) -> datetime | None:
    # build the slot's start datetime from the run's own params
    try:
        return datetime.strptime(f"{cfg.day} {cfg.time}", "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
