from __future__ import annotations

import argparse
import asyncio
import os
import tomllib
from datetime import datetime, timedelta

from config import Config
from errors import RateLimited, TokenExpired
from resy_booker import _build_config_token, run


def _set_drop_when(cfg: Config, when: str) -> None:
    """Parse a 'YYYY-MM-DD HH:MM[:SS]' drop moment into the cfg.drop_* fields."""
    when = when.strip()
    fmt = "%Y-%m-%d %H:%M:%S" if when.count(":") == 2 else "%Y-%m-%d %H:%M"
    dt = datetime.strptime(when, fmt)
    cfg.drop_date = dt.strftime("%Y-%m-%d")
    cfg.drop_hour, cfg.drop_minute, cfg.drop_second = dt.hour, dt.minute, dt.second


def _load_spec(path: str) -> dict:
    """Read a TOML reservation spec. stdlib tomllib (3.11+), so no extra dependency."""
    try:
        with open(path, "rb") as f:
            return tomllib.load(f)
    except FileNotFoundError:
        raise SystemExit(f"Spec file not found: {path}")
    except tomllib.TOMLDecodeError as e:
        raise SystemExit(f"Invalid TOML in {path}: {e}")


def _apply_spec(cfg: Config, spec: dict) -> None:
    # map toml to cfg
    for key in ("venue_id", "party_size", "config_id", "mid_field",
                "tolerance", "commit", "payment_method_id"):
        if key in spec:
            target = "time_tolerance_min" if key == "tolerance" else key
            setattr(cfg, target, int(spec[key]))
    for key in ("day", "time", "seating"):
        if key in spec:
            setattr(cfg, key, str(spec[key]))
    if "times" in spec:
        cfg.preferred_times = tuple(str(t).strip() for t in spec["times"])
    if "requires_payment" in spec:
        cfg.requires_payment = bool(spec["requires_payment"])

    drop = spec.get("drop") or {}
    if "tz" in drop:
        cfg.drop_tz = str(drop["tz"])
    if "when" in drop:
        _set_drop_when(cfg, str(drop["when"]))
    elif "release_days_before" in drop:
        # Derive the drop moment from the dining day: release this many days before,
        # at release_time (default midnight). Only used when "when" isn't given.
        dine = datetime.strptime(cfg.day, "%Y-%m-%d").date()
        drop_day = dine - timedelta(days=int(drop["release_days_before"]))
        h, m = map(int, str(drop.get("release_time", "00:00")).split(":"))
        cfg.drop_date = drop_day.strftime("%Y-%m-%d")
        cfg.drop_hour, cfg.drop_minute, cfg.drop_second = h, m, 0


def _build_config(args: argparse.Namespace) -> Config:
    # build config from provided arguments
    cfg = Config()

    # --- layer 1: TOML reservation spec (the sole source of truth for these fields) ---
    _apply_spec(cfg, _load_spec(args.spec))

    # --- layer 2: secrets + remaining overrides from env ---
    cfg.api_key = os.environ.get("RESY_API_KEY", cfg.api_key)
    cfg.auth_token = os.environ.get("RESY_AUTH_TOKEN", cfg.auth_token)
    env_pm = os.environ.get("RESY_PAYMENT_METHOD_ID") or os.environ.get("PAYMENT_METHOD_ID")
    if env_pm:
        cfg.payment_method_id = int(env_pm)

    if args.requires_payment:  # store_true: a flag can only turn it on, not off the spec
        cfg.requires_payment = True

    if cfg.requires_payment and not cfg.payment_method_id:
        # Fail fast at config time — never discover a missing card mid-drop.
        raise SystemExit(
            "requires_payment is set but no payment method id found. Export "
            "RESY_PAYMENT_METHOD_ID (or PAYMENT_METHOD_ID), or set payment_method_id "
            "in the spec."
        )

    cfg.dry_run = args.dry_run
    cfg.probe = args.probe
    cfg.now = args.now
    cfg.log_requests = args.log_requests
    cfg.clock_sync = args.clock_sync
    if args.clock_recalibrate_lead is not None:
        cfg.clock_sync_recalibrate_lead_s = args.clock_recalibrate_lead

    # --- layer 3: construct the direct-mode token from the resolved pieces ---
    # config_id present => direct mode. We build the rgs:// token here so the dining
    # params have a single source of truth and never disagree with the /details body.
    if cfg.config_id is not None:
        missing = [name for name, val in (
            ("mid_field", cfg.mid_field), ("time", cfg.time), ("seating", cfg.seating),
        ) if val in (None, "")]
        if missing:
            raise SystemExit(
                "Direct mode (config_id set) also needs: " + ", ".join(missing)
                + ". Provide them in the spec file."
            )
        cfg.config_token = _build_config_token(cfg)

    return cfg


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Time-boxed Resy reservation grabber (personal use). The TOML spec "
                    "is the sole source of truth for reservation/timing params. Secrets "
                    "come from env vars: RESY_API_KEY, RESY_AUTH_TOKEN, "
                    "RESY_PAYMENT_METHOD_ID.",
    )
    # Canonical input: a TOML reservation spec file. Flags below only control run mode.
    p.add_argument("spec", help="Path to a TOML reservation spec file.")
    p.add_argument("--requires-payment", action="store_true",
                   help="Venue takes a deposit/card hold: require a payment method id "
                        "(RESY_PAYMENT_METHOD_ID / PAYMENT_METHOD_ID env, or payment_method_id "
                        "in the spec) and send it to /book")
    # Mode
    p.add_argument("--dry-run", action="store_true",
                   help="Run find + details but STOP before /book (no reservation made)")
    p.add_argument("--probe", action="store_true",
                   help="Diagnostic: fire ONE /details for the constructed token right now "
                        "(no drop wait, no booking) and print exactly what Resy returns. "
                        "Use against a currently-bookable slot to validate token + auth.")
    p.add_argument("--now", action="store_true",
                   help="Testing: treat the current moment as the drop and skip the wait — "
                        "runs the real find/direct -> details -> book pipeline immediately "
                        "against a currently-bookable slot. Combine with --dry-run to stop "
                        "before /book.")
    p.add_argument("--log-requests", action="store_true",
                   help="Write full request/response JSON to api_log.jsonl. Adds a "
                        "blocking disk write per request, so it's off by default during "
                        "real drops; turn on for --dry-run/--probe debugging.")
    p.add_argument("--no-clock-sync", dest="clock_sync", action="store_false",
                   help="Disable server-clock calibration and trust the local clock")
    p.add_argument("--clock-recalibrate-lead", type=float, metavar="SEC",
                   help="Re-calibrate this many seconds before drop (default 60; 0 to skip)")
    p.set_defaults(clock_sync=True)
    return p.parse_args()


def main() -> None:
    cfg = _build_config(_parse_args())
    # Fail fast on obvious misconfiguration before the clock matters.
    if "PASTE" in cfg.api_key or "PASTE" in cfg.auth_token:
        raise SystemExit(
            "Missing credentials. Set RESY_API_KEY and RESY_AUTH_TOKEN env vars."
        )
    mode = "DIRECT /details (config_id, no /find)" if cfg.config_token else f"times={cfg.preferred_times}"
    print(f"[config] venue={cfg.venue_id} day={cfg.day} party={cfg.party_size} "
          f"{mode} drop={cfg.drop_date} "
          f"{cfg.drop_hour:02d}:{cfg.drop_minute:02d} {cfg.drop_tz}"
          f"{'  (DRY RUN)' if cfg.dry_run else ''}")
    try:
        asyncio.run(run(cfg))
    except TokenExpired as e:
        print(f"\nERROR: Auth rejected — re-grab X-Resy-Auth-Token from the browser. ({e})")
    except RateLimited as e:
        print(f"\nERROR: Rate-limited / Cloudflare challenge. Slow down and retry next drop. ({e})")
    except TimeoutError as e:
        print(f"\nERROR: {e}")
    except KeyboardInterrupt:
        print("\nInterrupted.")
