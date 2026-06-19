from __future__ import annotations

import argparse
import asyncio
import os
import tomllib
from datetime import datetime, timedelta

from config import Config
from resy_booker import RateLimited, TokenExpired, _build_config_token, run


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


def _build_config(args: argparse.Namespace) -> Config:
    # build config from provided arguments
    cfg = Config()

    # --- layer 1: TOML reservation spec (the canonical input) ---
    if args.spec:
        _apply_spec(cfg, _load_spec(args.spec))

    # --- layer 2: secrets from env (CLI --api-key/--auth-token still win) ---
    cfg.api_key = args.api_key or os.environ.get("RESY_API_KEY", cfg.api_key)
    cfg.auth_token = args.auth_token or os.environ.get("RESY_AUTH_TOKEN", cfg.auth_token)
    env_pm = os.environ.get("RESY_PAYMENT_METHOD_ID") or os.environ.get("PAYMENT_METHOD_ID")
    if args.payment_method_id is not None:
        cfg.payment_method_id = args.payment_method_id
    elif env_pm:
        cfg.payment_method_id = int(env_pm)

    # --- layer 3: CLI flag overrides (only when explicitly given) ---
    if args.venue_id is not None:
        cfg.venue_id = args.venue_id
    if args.day is not None:
        cfg.day = args.day
    if args.party_size is not None:
        cfg.party_size = args.party_size
    if args.time is not None:
        cfg.time = args.time
    if args.seating is not None:
        cfg.seating = args.seating
    if args.config_id_num is not None:
        cfg.config_id = args.config_id_num
    if args.mid_field is not None:
        cfg.mid_field = args.mid_field
    if args.times is not None:
        cfg.preferred_times = tuple(t.strip() for t in args.times.split(",") if t.strip())
    if args.tolerance is not None:
        cfg.time_tolerance_min = args.tolerance
    if args.commit is not None:
        cfg.commit = args.commit
    if args.requires_payment:  # store_true: a flag can only turn it on, not off the spec
        cfg.requires_payment = True

    if cfg.requires_payment and not cfg.payment_method_id:
        # Fail fast at config time — never discover a missing card mid-drop.
        raise SystemExit(
            "requires_payment is set but no payment method id found. Export "
            "RESY_PAYMENT_METHOD_ID (or PAYMENT_METHOD_ID), set payment_method_id in the "
            "spec, or pass --payment-method-id."
        )

    # Drop timing override: explicit --drop, else derive from dining day. CLI beats spec.
    if args.drop is not None:
        _set_drop_when(cfg, args.drop)
    elif args.release_days_before is not None:
        dine = datetime.strptime(cfg.day, "%Y-%m-%d").date()
        drop_day = dine - timedelta(days=args.release_days_before)
        h, m = map(int, args.release_time.split(":"))
        cfg.drop_date = drop_day.strftime("%Y-%m-%d")
        cfg.drop_hour, cfg.drop_minute, cfg.drop_second = h, m, 0
    if args.drop_tz is not None:
        cfg.drop_tz = args.drop_tz

    cfg.dry_run = args.dry_run
    cfg.probe = args.probe
    cfg.now = args.now
    cfg.log_requests = args.log_requests
    cfg.clock_sync = args.clock_sync
    if args.clock_recalibrate_lead is not None:
        cfg.clock_sync_recalibrate_lead_s = args.clock_recalibrate_lead

    # --- layer 4: construct the direct-mode token from the resolved pieces ---
    # config_id present => direct mode. We build the rgs:// token here so the dining
    # params have a single source of truth and never disagree with the /details body.
    if cfg.config_id is not None:
        missing = [name for name, val in (
            ("mid_field", cfg.mid_field), ("time", cfg.time), ("seating", cfg.seating),
        ) if val in (None, "")]
        if missing:
            raise SystemExit(
                "Direct mode (config_id set) also needs: " + ", ".join(missing)
                + ". Provide them in the spec file or via flags "
                  "(--mid-field / --time / --seating)."
            )
        cfg.config_token = _build_config_token(cfg)

    return cfg


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Time-boxed Resy reservation grabber (personal use). "
                    "Secrets default to env vars: RESY_API_KEY, RESY_AUTH_TOKEN, "
                    "RESY_PAYMENT_METHOD_ID.",
    )
    # Canonical input: a TOML reservation spec file. Everything below is an override.
    p.add_argument("spec", nargs="?",
                   help="Path to a TOML reservation spec file (the canonical input). "
                        "CLI flags below override individual fields.")
    # Reservation params (override spec)
    p.add_argument("--venue-id", type=int, help="Resy venue id (e.g. 60029 = Le Gratin)")
    p.add_argument("--day", help="Dining date, YYYY-MM-DD")
    p.add_argument("--party-size", type=int, help="Number of guests")
    p.add_argument("--times", help="Preferred times, priority order, comma-sep 24h HH:MM "
                                   "(e.g. 19:00,19:30,18:30)  [find mode]")
    p.add_argument("--tolerance", type=int, help="+/- minutes around each preferred time")
    p.add_argument("--commit", type=int, choices=(0, 1),
                   help="/details commit flag: 0=preview (no token), 1=mint book_token (default 1)")
    # Direct mode: construct the rgs:// token from these pieces and SKIP /find
    p.add_argument("--config-id-num", type=int, metavar="N",
                   help="Opaque slot/template id from a captured token (e.g. 3593815). "
                        "Setting this enables direct mode.")
    p.add_argument("--mid-field", type=int, metavar="N",
                   help="Opaque field right after config_id in the token (e.g. 2 or 3)")
    p.add_argument("--time", help="Exact slot time HH:MM:SS (direct mode)")
    p.add_argument("--seating", help='Seating-type label (direct mode), e.g. "Indoor Dining"')
    # Drop timing
    p.add_argument("--drop", help='Explicit drop moment "YYYY-MM-DD HH:MM[:SS]"')
    p.add_argument("--release-days-before", type=int,
                   help="Derive drop from dining day: release this many days before")
    p.add_argument("--release-time", default="00:00",
                   help="Time-of-day for --release-days-before (HH:MM, default 00:00)")
    p.add_argument("--drop-tz", help="Drop timezone (default America/New_York)")
    # Secrets (prefer env vars; these override if given)
    p.add_argument("--api-key", help="Resy api_key (overrides RESY_API_KEY)")
    p.add_argument("--auth-token", help="X-Resy-Auth-Token JWT (overrides RESY_AUTH_TOKEN)")
    p.add_argument("--payment-method-id", type=int,
                   help="Payment profile id (only for venues with a card hold)")
    p.add_argument("--requires-payment", action="store_true",
                   help="Venue takes a deposit/card hold: require a payment method id "
                        "(RESY_PAYMENT_METHOD_ID / PAYMENT_METHOD_ID env) and send it to /book")
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
            "Missing credentials. Set RESY_API_KEY and RESY_AUTH_TOKEN (env) "
            "or pass --api-key / --auth-token."
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
