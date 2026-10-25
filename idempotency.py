"""Idempotency records for non-idempotent compute requests.

The dispatcher never retries a POST after an ambiguous failure.  A client,
however, may retry because its connection dropped after the worker finished.
That retry is only safe if the dispatcher can replay the original result.

Records are scoped by caller and client key; the fingerprint covers method,
path, query and body so the same key cannot be reused for a different request.
A concurrent duplicate waits
for the first execution instead of starting a second one.  A failure that may
have reached the worker is recorded as ``ambiguous``: retrying with the same
key replays that error instead of running the work again.  Only failures that
provably happened before the request was sent clear the record, because those
are safe to retry.

The store is intentionally in-process.  The dispatcher is a single uvicorn
worker (see the Dockerfile); a multi-process dispatcher would need a shared
store to keep these guarantees.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from dataclasses import dataclass, field


class IdempotencyError(Exception):
    """Base class for local idempotency failures."""


class IdempotencyConflict(IdempotencyError):
    """The same key was used for a different request."""


class IdempotencyWaitTimeout(IdempotencyError):
    """The first request is still running past this caller's deadline."""


class IdempotencyAbandoned(IdempotencyError):
    """The first attempt failed before it could have reached a worker."""


@dataclass
class StoredResult:
    status_code: int
    headers: list[tuple[str, str]]
    body: bytes
    worker_tag: str = ""
    attempts: int = 0
    ambiguous: bool = False
    stored_at: float = field(default_factory=time.monotonic)


@dataclass
class _Entry:
    fingerprint: str
    created_at: float
    future: asyncio.Future | None = None
    result: StoredResult | None = None


def validate_key(raw: str | None) -> str | None:
    if raw is None:
        return None
    key = raw.strip()
    if not key:
        return None
    if not 8 <= len(key) <= 200:
        raise ValueError("Idempotency-Key must be 8 to 200 characters")
    if any(ord(char) < 33 or ord(char) > 126 for char in key):
        raise ValueError("Idempotency-Key must contain visible ASCII only")
    return key


def fingerprint(method: str, path: str, query: str, body: bytes | None) -> str:
    digest = hashlib.sha256()
    digest.update(method.upper().encode("ascii", "ignore"))
    digest.update(b"\n")
    digest.update(path.encode("utf-8", "surrogatepass"))
    digest.update(b"\n")
    digest.update(query.encode("utf-8", "surrogatepass"))
    digest.update(b"\n")
    digest.update(body or b"")
    return digest.hexdigest()


class IdempotencyStore:
    def __init__(self, ttl_seconds: float, max_entries: int):
        self.ttl_seconds = max(0.0, ttl_seconds)
        self.max_entries = max(1, max_entries)
        self._entries: dict[tuple[str, str], _Entry] = {}

    def __len__(self) -> int:
        return len(self._entries)

    @staticmethod
    def scope(caller: str, key: str) -> tuple[str, str]:
        # The client key identifies the operation. Two different keys must
        # never share a record, even for the same caller, method and path.
        return caller, key

    def begin(self, key: tuple[str, str], body_fingerprint: str
              ) -> tuple[_Entry | None, _Entry | None, StoredResult | None]:
        """Return ``(owner, follower, replay)``.

        Exactly one of the three is meaningful.  ``owner`` must execute and
        later call :meth:`complete` or :meth:`abandon`.  ``follower`` must
        await :meth:`wait`.  ``replay`` is returned immediately.
        """
        self._prune(time.monotonic())
        existing = self._entries.get(key)
        if existing is not None:
            if existing.fingerprint != body_fingerprint:
                raise IdempotencyConflict(
                    "Idempotency-Key was reused with a different request body")
            if existing.result is not None:
                return None, None, existing.result
            if existing.future is None:
                existing.future = asyncio.get_running_loop().create_future()
            return None, existing, None

        entry = _Entry(
            fingerprint=body_fingerprint, created_at=time.monotonic())
        self._entries[key] = entry
        return entry, None, None

    async def wait(self, entry: _Entry, timeout: float) -> StoredResult:
        if entry.future is None:
            raise IdempotencyAbandoned("Request is no longer in flight")
        try:
            return await asyncio.wait_for(
                asyncio.shield(entry.future), timeout=max(0.01, timeout))
        except asyncio.TimeoutError as exc:
            raise IdempotencyWaitTimeout(
                "The original request is still running") from exc

    def complete(self, key: tuple[str, str], entry: _Entry,
                 result: StoredResult) -> None:
        entry.result = result
        if entry.future is not None:
            if not entry.future.done():
                entry.future.set_result(result)

    def abandon(self, key: tuple[str, str], entry: _Entry,
                error: Exception) -> None:
        """Forget a request that provably never reached a worker."""
        self._entries.pop(key, None)
        if entry.future is not None and not entry.future.done():
            entry.future.set_exception(error)

    def _prune(self, now: float) -> None:
        if self.ttl_seconds > 0:
            expired = [
                key for key, entry in self._entries.items()
                if entry.result is not None
                and now - entry.result.stored_at > self.ttl_seconds
            ]
            for key in expired:
                self._entries.pop(key, None)

        if len(self._entries) < self.max_entries:
            return
        evictable = sorted(
            (key for key, entry in self._entries.items()
             if entry.result is not None),
            key=lambda key: self._entries[key].result.stored_at,
        )
        for key in evictable[:max(1, len(self._entries) - self.max_entries + 1)]:
            self._entries.pop(key, None)
