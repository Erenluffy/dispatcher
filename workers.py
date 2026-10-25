"""Worker fleet state, selection, concurrency reservation and circuit breaking.

The pool is entirely in-process.  That is deliberate: one uvicorn process gives
coherent circuit, queue and rate-limit state, and the dispatcher is a single
Render service.  Every mutation of worker state runs under one ``asyncio``
condition; network I/O never happens while that condition is held.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from .config import Settings, WorkerConfig


class PoolError(Exception):
    """Base class for local worker-selection failures."""


class QueueTimeout(PoolError):
    """No worker became available before the queue deadline."""


class NoWorkersAvailable(PoolError):
    """Every configured worker was already attempted (or none exist)."""


class AffinityWorkerNotFound(PoolError):
    """A tagged stateful ID points at a worker that is no longer configured."""


@dataclass
class WorkerState:
    config: WorkerConfig
    index: int
    inflight: int = 0
    heavy_inflight: int = 0
    verilator_inflight: int = 0
    consecutive_failures: int = 0
    open_count: int = 0
    circuit_open_until: float = 0.0
    half_open_trial: bool = False
    cooldown_until: float = 0.0
    last_success: float | None = None
    last_error: str | None = None
    last_error_at: float | None = None
    ewma_ms: float | None = None
    cold_lease: bool = False

    @property
    def tag(self) -> str:
        return self.config.tag

    @property
    def url(self) -> str:
        return self.config.url

    def is_warm(self, now: float, window: float) -> bool:
        return (self.last_success is not None
                and (now - self.last_success) <= window)

    def circuit_state(self, now: float) -> str:
        if self.open_count == 0:
            return "closed"
        if now < self.circuit_open_until:
            return "open"
        return "half-open"

    def circuit_available(self, now: float) -> bool:
        if self.open_count == 0:
            return True
        if now < self.circuit_open_until:
            return False
        return not self.half_open_trial

    def health_status(self, now: float, window: float) -> str:
        """Observational status, based only on traffic the dispatcher saw.

        ``unknown`` means no request has succeeded since this dispatcher
        process started; it does not prove the worker is asleep.  ``cold`` is
        a worker that answered before but has not been used inside the warm
        window, which on Render Free usually means it has since been slept.
        """
        if self.circuit_state(now) == "open":
            return "circuit_open"
        if self.is_warm(now, window):
            return "responsive"
        if self.last_success is not None:
            return "cold"
        return "unknown"

    def has_capacity(self, settings: Settings, *,
                     heavy: bool, verilator: bool) -> bool:
        if self.inflight >= settings.worker_max_inflight:
            return False
        if heavy and self.heavy_inflight >= settings.heavy_max_inflight:
            return False
        if verilator and self.verilator_inflight >= settings.verilator_max_inflight:
            return False
        return True


@dataclass
class Reservation:
    worker: WorkerState
    counted: bool
    heavy: bool = False
    verilator: bool = False
    half_open: bool = False
    started_at: float = 0.0
    released: bool = False


class WorkerPool:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.workers = [
            WorkerState(config=config, index=index)
            for index, config in enumerate(settings.workers)
        ]
        self._by_tag = {worker.tag: worker for worker in self.workers}
        self._condition = asyncio.Condition()
        self.queue_depth = 0

    # ---------------------------------------------------------------- selection

    def get_by_tag(self, tag: str) -> WorkerState:
        try:
            return self._by_tag[tag]
        except KeyError as exc:
            raise AffinityWorkerNotFound(
                f"Worker tag {tag!r} is not part of the configured fleet") from exc

    def has_alternative(self, exclude_tags: set[str] | frozenset[str]) -> bool:
        return any(worker.tag not in exclude_tags for worker in self.workers)

    async def acquire(self, *, heavy: bool = False, verilator: bool = False,
                      exclude_tags: set[str] | frozenset[str] = frozenset(),
                      deadline: float | None = None,
                      affinity_tag: str | None = None) -> Reservation:
        """Reserve a worker slot, waiting in the queue if necessary.

        ``affinity_tag`` bypasses load balancing and health gating: the data for
        a tagged waveform/share exists on exactly one worker, so waking and
        trying that worker is the only useful action.  Those requests do not
        consume a compute slot (the worker only reads its local file).
        """
        if affinity_tag is not None:
            worker = self.get_by_tag(affinity_tag)
            return Reservation(
                worker=worker, counted=False, started_at=time.monotonic())

        if not self.has_alternative(exclude_tags):
            raise NoWorkersAvailable("No alternative worker is available")

        now = time.monotonic()
        queue_end = now + self.settings.queue_timeout_seconds
        if deadline is not None:
            queue_end = min(queue_end, deadline)
        waited = False
        try:
            async with self._condition:
                while True:
                    now = time.monotonic()
                    if now >= queue_end:
                        raise QueueTimeout(
                            "All simulation workers are busy; retry shortly")

                    choice = self._choose(now, exclude_tags, heavy, verilator)
                    if choice is not None:
                        worker = choice
                        worker.inflight += 1
                        if heavy:
                            worker.heavy_inflight += 1
                        if verilator:
                            worker.verilator_inflight += 1
                        half_open = (worker.open_count > 0
                                     and now >= worker.circuit_open_until)
                        if half_open:
                            worker.half_open_trial = True
                        if not worker.is_warm(
                                now, self.settings.warm_window_seconds):
                            worker.cold_lease = True
                        return Reservation(
                            worker=worker,
                            counted=True,
                            heavy=heavy,
                            verilator=verilator,
                            half_open=half_open,
                            started_at=time.monotonic(),
                        )

                    if not waited:
                        self.queue_depth += 1
                        waited = True
                    wait_until = queue_end
                    next_event = self._next_state_change(
                        now, exclude_tags, heavy, verilator)
                    if next_event is not None:
                        wait_until = min(wait_until, next_event)
                    timeout = max(0.01, wait_until - time.monotonic())
                    try:
                        await asyncio.wait_for(
                            self._condition.wait(), timeout=timeout)
                    except asyncio.TimeoutError:
                        # Loop re-evaluates capacity, circuit cooldowns and the
                        # queue deadline.  A timeout is not itself an error.
                        continue
        finally:
            if waited:
                # queue_depth is only read for diagnostics, but keep it exact.
                self.queue_depth = max(0, self.queue_depth - 1)

    def _choose(self, now: float, exclude_tags: set[str] | frozenset[str],
                heavy: bool, verilator: bool) -> WorkerState | None:
        eligible: list[WorkerState] = []
        for worker in self.workers:
            if worker.tag in exclude_tags:
                continue
            if worker.cooldown_until > now:
                continue
            if not worker.circuit_available(now):
                continue
            if not worker.has_capacity(self.settings, heavy=heavy,
                                       verilator=verilator):
                continue
            eligible.append(worker)

        warm = [worker for worker in eligible if worker.is_warm(
            now, self.settings.warm_window_seconds)]
        if warm:
            warm.sort(key=lambda worker: (
                worker.inflight,
                worker.heavy_inflight,
                worker.ewma_ms if worker.ewma_ms is not None else float("inf"),
                worker.index,
            ))
            return warm[0]

        cold = [worker for worker in eligible if not worker.is_warm(
            now, self.settings.warm_window_seconds)]
        if not cold:
            return None
        leased = [worker for worker in cold if worker.cold_lease]
        if leased:
            leased.sort(key=lambda worker: (worker.inflight, worker.index))
            return leased[0]
        active_cold = sum(1 for worker in self.workers if worker.cold_lease)
        if active_cold >= self.settings.max_cold_workers:
            return None
        cold.sort(key=lambda worker: (worker.inflight, worker.index))
        return cold[0]

    def _next_state_change(self, now: float,
                           exclude_tags: set[str] | frozenset[str],
                           heavy: bool, verilator: bool) -> float | None:
        """Earliest future time a currently-blocked worker can be considered.

        Capacity, cold-wake leases and half-open trials all become available
        through ``release()``, which notifies the condition.  This helper only
        covers time-based blockers (circuit cooldowns, busy cooldowns) so the
        queue wakes up even when no request finishes.
        """
        candidates: list[float] = []
        for worker in self.workers:
            if worker.tag in exclude_tags:
                continue
            if not worker.has_capacity(self.settings, heavy=heavy,
                                       verilator=verilator):
                continue
            if worker.cooldown_until > now:
                candidates.append(worker.cooldown_until)
            if (worker.open_count > 0
                    and worker.circuit_open_until > now):
                candidates.append(worker.circuit_open_until)
        return min(candidates) if candidates else None

    # ------------------------------------------------------------------- release

    async def release(self, reservation: Reservation, outcome: str | None = None,
                      error: str | None = None) -> None:
        """Release a reservation and record the worker outcome.

        ``outcome`` is ``"success"`` (2xx/3xx), ``"neutral"`` (a real HTTP
        response that is not a transport failure, e.g. 4xx/429/500) or
        ``"failure"`` (transport error, timeout, 502/503/504).  ``None`` means
        the request was cancelled or the outcome is unknown; it must not punish
        the worker, but it does release a half-open trial for another caller.
        """
        if reservation.released:
            return
        reservation.released = True
        worker = reservation.worker
        now = time.monotonic()
        async with self._condition:
            if reservation.counted:
                worker.inflight = max(0, worker.inflight - 1)
                if reservation.heavy:
                    worker.heavy_inflight = max(0, worker.heavy_inflight - 1)
                if reservation.verilator:
                    worker.verilator_inflight = max(
                        0, worker.verilator_inflight - 1)

            latency_ms = max(0.0, (now - reservation.started_at) * 1000.0)
            if outcome in ("success", "neutral"):
                worker.last_success = now
                worker.last_error = None
                if worker.ewma_ms is None:
                    worker.ewma_ms = latency_ms
                else:
                    worker.ewma_ms = (0.7 * worker.ewma_ms
                                      + 0.3 * latency_ms)
                if outcome == "success":
                    worker.consecutive_failures = 0
                    worker.open_count = 0
                    worker.circuit_open_until = 0.0
                elif reservation.half_open or worker.half_open_trial:
                    # Any real response proves the worker is alive; close the
                    # half-open trial without treating a 4xx/500 as a failure.
                    worker.open_count = 0
                    worker.circuit_open_until = 0.0
                worker.half_open_trial = False
            elif outcome == "failure":
                worker.consecutive_failures += 1
                worker.last_error = (error or "worker request failed")[:300]
                worker.last_error_at = now
                was_half_open = reservation.half_open or worker.half_open_trial
                worker.half_open_trial = False
                if (was_half_open
                        or worker.consecutive_failures
                        >= self.settings.failure_threshold):
                    worker.open_count = max(1, worker.open_count + 1)
                    backoff = min(
                        self.settings.cooldown_base_seconds
                        * (2 ** (worker.open_count - 1)),
                        self.settings.cooldown_max_seconds,
                    )
                    worker.circuit_open_until = now + backoff
            else:
                worker.half_open_trial = False

            if worker.is_warm(now, self.settings.warm_window_seconds):
                worker.cold_lease = False
            elif worker.inflight == 0:
                # A wake that ended without a usable response must free a
                # cold-wake slot for the next worker.
                worker.cold_lease = False

            self._condition.notify_all()

    async def soft_cooldown(self, tag: str, seconds: float) -> None:
        """Briefly skip a worker that answered 503 "Server is busy"."""
        if seconds <= 0:
            return
        async with self._condition:
            worker = self.get_by_tag(tag)
            worker.cooldown_until = max(
                worker.cooldown_until, time.monotonic() + seconds)
            self._condition.notify_all()

    # --------------------------------------------------------------- diagnostics

    def snapshot(self) -> list[dict]:
        now = time.monotonic()
        result = []
        for worker in self.workers:
            last_success_age = (
                None if worker.last_success is None
                else round(max(0.0, now - worker.last_success), 1))
            cooldown_remaining = max(0.0, worker.cooldown_until - now)
            circuit_remaining = max(0.0, worker.circuit_open_until - now)
            result.append({
                "tag": worker.tag,
                "status": worker.health_status(
                    now, self.settings.warm_window_seconds),
                "warm": worker.is_warm(
                    now, self.settings.warm_window_seconds),
                "inflight": worker.inflight,
                "heavy_inflight": worker.heavy_inflight,
                "verilator_inflight": worker.verilator_inflight,
                "circuit": worker.circuit_state(now),
                "consecutive_failures": worker.consecutive_failures,
                "cooldown_seconds": round(cooldown_remaining, 1)
                if cooldown_remaining > 0 else 0,
                "circuit_retry_seconds": round(circuit_remaining, 1)
                if circuit_remaining > 0 else 0,
                "last_success_age_seconds": last_success_age,
                "last_error": worker.last_error,
                "ewma_ms": round(worker.ewma_ms, 1)
                if worker.ewma_ms is not None else None,
            })
        return result

    def warm_count(self) -> int:
        now = time.monotonic()
        return sum(1 for worker in self.workers if worker.is_warm(
            now, self.settings.warm_window_seconds))

    def status_counts(self) -> dict[str, int]:
        now = time.monotonic()
        counts = {
            "responsive": 0, "cold": 0, "unknown": 0, "circuit_open": 0,
        }
        cooling_down = 0
        for worker in self.workers:
            counts[worker.health_status(
                now, self.settings.warm_window_seconds)] += 1
            if worker.cooldown_until > now:
                cooling_down += 1
        counts["cooling_down"] = cooling_down
        return counts

    def all_circuits_open(self) -> bool:
        now = time.monotonic()
        return bool(self.workers) and all(
            worker.circuit_state(now) == "open" for worker in self.workers)
