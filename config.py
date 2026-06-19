from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Config:
    # --- Auth (from browser DevTools → Network → any api.resy.com request) ---
    api_key: str = "PASTE_STATIC_API_KEY"          # the value inside Authorization
    auth_token: str = "PASTE_USER_JWT"             # X-Resy-Auth-Token / X-Resy-Universal-Auth
    # Set requires_payment (--requires-payment) for venues that take a card hold/deposit
    # at booking. When set, a payment_method_id is REQUIRED (from RESY_PAYMENT_METHOD_ID /
    # PAYMENT_METHOD_ID env) and sent to /book as struct_payment_method. Leave off for
    # free venues (most), where /book takes no payment field.
    requires_payment: bool = False
    payment_method_id: int = 0

    # --- Target reservation ---
    venue_id: int = 60029                           # Le Gratin
    party_size: int = 2
    day: str = "2026-07-21"                         # YYYY-MM-DD — the date you want to dine
    # /details commit: 0 = preview (NO book_token), 1 = reserve intent (mints book_token).
    # The browser's preview call returns no token; the token comes from the commit:1 call
    # fired on "Reserve". commit:1 does NOT finalize — only /book does — so it's dry-run safe.
    commit: int = 1
    # lat/long are ignored when venue_id is set; 0/0 matches the real browser payload.
    lat: float = 0
    long: float = 0

    # Direct-booking identity. When config_id is set we SKIP /find and construct the
    # rgs:// token ourselves from the known params + these opaque pieces, then poll
    # /details directly until inventory goes live. Shaves a round trip; the trade-off is
    # no self-correction — if the pieces are wrong/stale for this drop, no /find fallback.
    #   token = rgs://resy/<venue>/<config_id>/<mid_field>/<day>/<day>/<time>/<party>/<seating>
    # config_id and mid_field are read off a real captured token (we can't compute them);
    # everything else is your normal reservation params.
    config_id: int | None = None       # the opaque slot/template id, e.g. 3593815
    mid_field: int | None = None       # the opaque field right after config_id (e.g. 2 or 3)
    time: str = ""                     # exact slot time "HH:MM:SS" (direct mode)
    seating: str = ""                  # seating-type label, e.g. "Indoor Dining"
    config_token: str = ""             # constructed at runtime from the pieces above

    # Preferred dining time(s), 24h "HH:MM", in priority order.
    # The first slot whose start time falls in [pref - tolerance, pref + tolerance] wins.
    preferred_times: tuple[str, ...] = ("19:00", "19:30", "18:30", "20:00")
    time_tolerance_min: int = 69                    # +/- minutes around each preferred time

    # --- Drop timing (when reservations are released) ---
    # Le Gratin releases 30 days ahead at midnight ET, so dining 2026-07-21 drops
    # at 2026-06-21 00:00 America/New_York.
    drop_tz: str = "America/New_York"
    drop_hour: int = 0
    drop_minute: int = 0
    drop_second: int = 0
    drop_date: str = "2026-06-21"                   # YYYY-MM-DD the drop fires

    # --- Mode ---
    dry_run: bool = False                           # find + details, but STOP before /book
    probe: bool = False                             # one immediate /details, report, exit
    now: bool = False                               # run workflow immediately for testing
    log_requests: bool = False                      # write full req/resp log to api_log.jsonl

    # --- Timing window (seconds relative to drop) ---
    prewarm_lead_s: float = 5.0                     # open + warm the connection this early
    poll_start_lead_s: float = 1.0                  # begin polling this long before drop
    poll_end_lag_s: float = 8.0                     # give up this long after drop
    poll_interval_s: float = 0.22                   # base gap between find() calls
    poll_jitter_s: float = 0.06                     # +/- random jitter to avoid lockstep

    # --- Networking ---
    request_timeout_s: float = 4.0
    max_inflight_finds: int = 3                     # cap concurrent find() calls

    # --- Clock calibration ---
    # The drop fires on Resy's clock, not yours. A laptop clock can drift hundreds of
    # ms, which makes "start polling 1s before drop" silently late. We discipline our
    # schedule against the server's own time via HTTP Date-header edge detection, then
    # drive every sleep/deadline off the corrected (server) clock.
    clock_sync: bool = True
    clock_sync_max_s: float = 3.0                   # time budget for the calibration burst
    clock_sync_interval_s: float = 0.08             # spacing between probe requests
    clock_sync_min_edges: int = 2                   # second-ticks needed for a trusted estimate
    clock_sync_recalibrate_lead_s: float = 60.0     # fresh offset this many seconds before drop
    clock_offset_s: float = 0.0                     # measured at runtime: server_time - local_time

    headers: dict = field(default_factory=dict)

    def build_headers(self) -> dict:
        return {
            "Authorization": f'ResyAPI api_key="{self.api_key}"',
            "X-Resy-Auth-Token": self.auth_token,
            "X-Resy-Universal-Auth": self.auth_token,   # some endpoints want this too
            "Accept": "application/json, text/plain, */*",
            # Content-Type is set per-request: httpx's json= sets application/json,
            # which matches the real /find, /details and /book calls (all JSON now).
            "Origin": "https://resy.com",
            "Referer": "https://resy.com/",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            ),
            "X-Origin": "https://resy.com",
        }
