import time
from fastapi import Request, HTTPException

# In-memory store: { ip -> [timestamps] }
_RATE_LIMIT_STORE: dict[str, list[float]] = {}

REQUESTS_PER_MINUTE = 60
WINDOW_SECONDS = 60


def rate_limiter(request: Request):
    """
    Simple in-memory sliding window rate limiter.
    Limits requests per IP.
    P2 feature – API protection.
    """

    ip = request.client.host
    now = time.time()

    timestamps = _RATE_LIMIT_STORE.get(ip, [])

    # Keep only requests within the last WINDOW_SECONDS
    timestamps = [ts for ts in timestamps if now - ts < WINDOW_SECONDS]

    if len(timestamps) >= REQUESTS_PER_MINUTE:
        raise HTTPException(
            status_code=429,
            detail="Too many requests. Please try again later."
        )

    timestamps.append(now)
    _RATE_LIMIT_STORE[ip] = timestamps
