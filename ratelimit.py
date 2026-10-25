"""Global per-IP rate limiting.

The worker's SlowAPI limits are per process, so eight workers mean eight
independent buckets.  The dispatcher adds one shared sliding-window limit in
front of the fleet, mirroring the worker's public limits.
"""

from __future__ import annotations

import asyncio
import math
import time
from collections import deque

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from .config import Settings
from .proxy import client_ip


class SlidingWindowLimiter:
    def __init__(self, window_seconds: float):
        self.window_seconds = window_seconds
        self._events: dict[str, deque[float]] = {}
        self._lock = asyncio.Lock()
        self._checks = 0

    async def check(self, key: str, limit: int) -> tuple[bool, int]:
        """Return ``(allowed, retry_after_seconds)``."""
        if limit <= 0:
            return True, 0
        now = time.monotonic()
        cutoff = now - self.window_seconds
        async with self._lock:
            bucket = self._events.get(key)
            if bucket is None:
                bucket = deque()
                self._events[key] = bucket
            while bucket and bucket[0] <= cutoff:
                bucket.popleft()
            if len(bucket) >= limit:
                retry_after = max(1, math.ceil(bucket[0] + self.window_seconds - now))
                return False, retry_after
            bucket.append(now)
            self._checks += 1
            if self._checks % 512 == 0:
                self._prune(cutoff)
            return True, 0

    def _prune(self, cutoff: float) -> None:
        stale = [key for key, bucket in self._events.items()
                 if not bucket or bucket[-1] <= cutoff]
        for key in stale:
            self._events.pop(key, None)


_RUN_PATHS = frozenset({"/api/run", "/api/submit", "/api/synthesize"})
_ADVANCED_PATHS = frozenset({
    "/api/run-advanced", "/api/lint", "/api/dev/run-custom",
})


def _limit_for(request: Request, settings: Settings) -> tuple[str, int] | None:
    if not settings.enable_rate_limits or request.method.upper() != "POST":
        return None
    path = request.url.path
    if len(path) > 1:
        path = path.rstrip("/")
    if path in _RUN_PATHS:
        return "run", settings.run_rate_limit_per_minute
    if path in _ADVANCED_PATHS:
        return "advanced", settings.advanced_rate_limit_per_minute
    return None


class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, settings: Settings):
        super().__init__(app)
        self.settings = settings
        self.limiter = SlidingWindowLimiter(settings.rate_limit_window_seconds)

    async def dispatch(self, request: Request, call_next):
        rule = _limit_for(request, self.settings)
        if rule is not None:
            group, limit = rule
            caller = client_ip(request)
            allowed, retry_after = await self.limiter.check(
                f"{group}:{caller}", limit)
            if not allowed:
                return JSONResponse(
                    status_code=429,
                    content={
                        "error": (f"Rate limit exceeded. Try again in "
                                  f"{retry_after} seconds."),
                        "retry_after": retry_after,
                    },
                    headers={
                        "Retry-After": str(retry_after),
                        "Cache-Control": "no-store",
                    },
                )
        return await call_next(request)
