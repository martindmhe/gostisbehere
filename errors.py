class RateLimited(Exception):
    """Raised on HTTP 429 / Cloudflare challenge."""

class TokenExpired(Exception):
    """Auth token rejected (401/403) — JWT likely expired; re-grab from browser."""

