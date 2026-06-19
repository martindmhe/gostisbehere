from __future__ import annotations

import json
import os
import time
from datetime import datetime

import httpx

LOGS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

API_LOG_PATH = os.path.join(LOGS_DIR, "api_log.jsonl")


def reset_api_log() -> None:
    open(API_LOG_PATH, "w").close()


def _decode_body(content: bytes) -> object:
    if not content:
        return None
    try:
        return json.loads(content)
    except (json.JSONDecodeError, UnicodeDecodeError):
        try:
            return content.decode("utf-8")
        except UnicodeDecodeError:
            return repr(content)


async def log_request(request: httpx.Request) -> None:
    request.extensions["log_t0"] = time.time()


async def log_response(response: httpx.Response) -> None:
    await response.aread()
    request = response.request
    entry = {
        "ts": datetime.now().isoformat(timespec="milliseconds"),
        "method": request.method,
        "url": str(request.url),
        "request_headers": dict(request.headers),
        "request_body": _decode_body(request.content),
        "status": response.status_code,
        "response_headers": dict(response.headers),
        "response_body": _decode_body(response.content),
        "elapsed_ms": round((time.time() - request.extensions.get("log_t0", time.time())) * 1000, 1),
    }
    with open(API_LOG_PATH, "a") as f:
        json.dump(entry, f, separators=(",", ":"), default=str)
        f.write("\n")
