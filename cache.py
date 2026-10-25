"""Small in-process TTL cache for the problem catalogs.

The catalogs are read-mostly and change only when a worker is redeployed.  A
fresh entry avoids a worker wake-up entirely; a stale entry keeps the site
usable when every worker is cold, down or rate-limited.  Per-key locks collapse
concurrent misses into one upstream request.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class CacheEntry:
    body: bytes
    status_code: int
    content_type: str
    worker_tag: str
    stored_at: float

    def age(self, now: float | None = None) -> float:
        now = time.monotonic() if now is None else now
        return max(0.0, now - self.stored_at)


class ResponseCache:
    def __init__(self, ttl_seconds: float, stale_seconds: float):
        self.ttl_seconds = ttl_seconds
        self.stale_seconds = stale_seconds
        self._entries: dict[str, CacheEntry] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def lock_for(self, key: str) -> asyncio.Lock:
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        return lock

    def get_fresh(self, key: str) -> CacheEntry | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        if entry.age() <= self.ttl_seconds:
            return entry
        return None

    def get_stale(self, key: str) -> CacheEntry | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        if entry.age() <= self.stale_seconds:
            return entry
        self._entries.pop(key, None)
        return None

    def set(self, key: str, body: bytes, *, status_code: int,
            content_type: str, worker_tag: str) -> CacheEntry:
        entry = CacheEntry(
            body=body,
            status_code=status_code,
            content_type=content_type,
            worker_tag=worker_tag,
            stored_at=time.monotonic(),
        )
        self._entries[key] = entry
        return entry

    def summary(self) -> dict:
        now = time.monotonic()
        return {
            "keys": sorted(self._entries),
            "entries": {
                key: {
                    "age_seconds": round(entry.age(now), 1),
                    "fresh": entry.age(now) <= self.ttl_seconds,
                    "worker": entry.worker_tag,
                    "bytes": len(entry.body),
                }
                for key, entry in self._entries.items()
            },
            "ttl_seconds": self.ttl_seconds,
            "stale_seconds": self.stale_seconds,
        }
