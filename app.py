"""FastAPI dispatcher for the CHIPVERSITY Verilog worker fleet.

The public API is a reverse proxy with three additions:

* health-aware worker selection and bounded concurrency,
* worker affinity for waveform/share IDs created by a specific worker,
* synthetic/cached catalog and health endpoints that do not wake every worker.

No worker API contract changes are required.  Response status codes and JSON
shapes are passed through except for the worker tags in stateful IDs.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass

import httpx
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response, StreamingResponse

from . import affinity
from .cache import CacheEntry, ResponseCache
from .idempotency import (
    IdempotencyAbandoned,
    IdempotencyConflict,
    IdempotencyStore,
    IdempotencyWaitTimeout,
    StoredResult,
    fingerprint,
    validate_key,
)
from .config import (
    ADVANCED_PROFILE,
    CATALOG_PATHS,
    CATALOG_PROFILE,
    DEFAULT_GENERATIONS,
    DEV_AUDIT_PROFILE,
    GENERIC_READ_PROFILE,
    GENERIC_WRITE_PROFILE,
    LINT_PROFILE,
    RUN_PROFILE,
    SHARE_CREATE_PROFILE,
    STATE_READ_PROFILE,
    STATIC_ENGINES,
    SYNTH_PROFILE,
    WAVEFORM_DOWNLOAD_PROFILE,
    ConfigError,
    RouteProfile,
    Settings,
)
from .proxy import (
    InvalidUpstreamPath,
    ProxyError,
    ResponseTooLarge,
    UpstreamConnectError,
    UpstreamNotSent,
    UpstreamTimeout,
    UpstreamTransportError,
    build_target_url,
    classify_status,
    client_ip,
    content_type_of,
    filtered_response_headers,
    header_value,
    is_html_content_type,
    is_json_content_type,
    looks_busy,
    read_buffered,
    response_is_empty,
    send_upstream,
    worker_auth_headers,
)
from .ratelimit import RateLimitMiddleware
from .workers import (
    AffinityWorkerNotFound,
    NoWorkersAvailable,
    QueueTimeout,
    Reservation,
    WorkerPool,
    WorkerState,
)

logger = logging.getLogger("chipversity.dispatcher")

_LEGACY_PROBE_CACHE_SECONDS = 3600.0
_LEGACY_PROBE_CACHE_MAX = 512
_RETRY_AFTER_QUEUE = "15"
_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"]


class DispatcherError(Exception):
    """A locally generated error returned in the backend's JSON shape.

    ``execution_started`` is ``True`` when the worker may already have begun
    the work.  Idempotency records an ambiguous result in that case, so a
    client retry with the same key cannot silently run a compute job twice.
    """

    def __init__(self, status_code: int, detail: str,
                 headers: dict[str, str] | None = None, *,
                 execution_started: bool = False,
                 worker_tag: str = "", attempts: int = 0):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail
        self.headers = headers or {}
        self.execution_started = execution_started
        self.worker_tag = worker_tag
        self.attempts = attempts


@dataclass
class ProxyResult:
    status_code: int
    headers: list[tuple[str, str]]
    body: bytes
    worker_tag: str
    attempts: int


class DispatcherService:
    def __init__(self, settings: Settings, pool: WorkerPool,
                 cache: ResponseCache, http_client: httpx.AsyncClient):
        self.settings = settings
        self.pool = pool
        self.cache = cache
        self.http_client = http_client
        self.idempotency = IdempotencyStore(
            settings.idempotency_ttl_seconds,
            settings.idempotency_max_entries)
        self.problem_count = 0
        self._legacy_cache: dict[tuple[str, str], tuple[str, float]] = {}

    # ------------------------------------------------------------------ helpers

    @staticmethod
    def _deadline(profile: RouteProfile) -> float:
        return time.monotonic() + profile.total_timeout

    async def _read_body(self, request: Request, deadline: float) -> bytes | None:
        if request.method.upper() in ("GET", "HEAD"):
            return None

        declared = request.headers.get("content-length")
        if declared:
            try:
                if int(declared) > self.settings.max_request_bytes:
                    raise DispatcherError(
                        413,
                        f"Request body too large (max "
                        f"{self.settings.max_request_bytes} bytes)")
            except ValueError:
                pass

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise DispatcherError(408, "Request body was not received in time")
        try:
            body = await asyncio.wait_for(request.body(), timeout=remaining)
        except asyncio.TimeoutError as exc:
            raise DispatcherError(
                408, "Request body was not received in time") from exc
        if len(body) > self.settings.max_request_bytes:
            raise DispatcherError(
                413,
                f"Request body too large (max "
                f"{self.settings.max_request_bytes} bytes)")
        return body

    def _finalize(self, result: ProxyResult, request: Request, *,
                  cache_state: str | None = None,
                  cache_age: float | None = None,
                  idempotency: str | None = None) -> Response:
        headers = [(key, value) for key, value in result.headers
                   if key.lower() != "content-length"]
        no_body = response_is_empty(result.status_code, request.method)
        if not no_body:
            headers.append(("content-length", str(len(result.body))))
        headers.append(("x-dispatcher-worker", result.worker_tag))
        headers.append(("x-dispatcher-attempts", str(result.attempts)))
        if cache_state:
            headers.append(("x-dispatcher-cache", cache_state))
        if cache_age is not None:
            headers.append(("x-dispatcher-cache-age", str(round(cache_age, 1))))
        if idempotency:
            headers.append(("x-dispatcher-idempotency", idempotency))
        if (result.status_code == 503
                and not header_value(headers, "retry-after")):
            headers.append(("retry-after", "10"))
        return Response(
            content=b"" if no_body else result.body,
            status_code=result.status_code,
            headers=dict(headers),
        )

    @staticmethod
    def _replace_body(result: ProxyResult, body: bytes) -> ProxyResult:
        headers = [(key, value) for key, value in result.headers
                   if key.lower() not in ("content-length", "content-encoding")]
        return ProxyResult(
            status_code=result.status_code,
            headers=headers,
            body=body,
            worker_tag=result.worker_tag,
            attempts=result.attempts,
        )

    def _rewrite_json(self, result: ProxyResult, tag: str, *,
                      share: bool = False) -> ProxyResult:
        content_type = content_type_of(result.headers)
        if not is_json_content_type(content_type) or not result.body:
            return result
        try:
            payload = json.loads(result.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return result
        if not isinstance(payload, dict):
            return result
        changed = (affinity.retag_share_payload(payload, tag) if share
                   else affinity.retag_compute_payload(payload, tag))
        if not changed:
            return result
        return self._replace_body(result, affinity.dumps_compact(payload))

    def _error_from_pool(self, exc: Exception) -> DispatcherError:
        if isinstance(exc, AffinityWorkerNotFound):
            return DispatcherError(404, "Stateful data is no longer available")
        if isinstance(exc, NoWorkersAvailable):
            return DispatcherError(
                503, "No simulation worker is currently available",
                {"Retry-After": _RETRY_AFTER_QUEUE})
        if isinstance(exc, QueueTimeout):
            return DispatcherError(
                503, "All simulation workers are busy. Please try again shortly.",
                {"Retry-After": _RETRY_AFTER_QUEUE})
        return DispatcherError(503, "No simulation worker is currently available")

    # ------------------------------------------------------------- idempotency

    @staticmethod
    def _stored(result: ProxyResult, *, ambiguous: bool) -> StoredResult:
        return StoredResult(
            status_code=result.status_code,
            headers=list(result.headers),
            body=result.body,
            worker_tag=result.worker_tag,
            attempts=result.attempts,
            ambiguous=ambiguous,
        )

    @staticmethod
    def _from_stored(stored: StoredResult) -> ProxyResult:
        return ProxyResult(
            status_code=stored.status_code,
            headers=list(stored.headers),
            body=stored.body,
            worker_tag=stored.worker_tag,
            attempts=stored.attempts,
        )

    @staticmethod
    def _error_result(exc: DispatcherError) -> ProxyResult:
        headers = [("content-type", "application/json")]
        headers.extend(
            (name.lower(), value) for name, value in exc.headers.items())
        return ProxyResult(
            status_code=exc.status_code,
            headers=headers,
            body=json.dumps({"detail": exc.detail}).encode("utf-8"),
            worker_tag=exc.worker_tag,
            attempts=exc.attempts,
        )

    @staticmethod
    def _ambiguous_error(detail: str) -> DispatcherError:
        return DispatcherError(502, detail, execution_started=True)

    async def _idempotent(self, request: Request, path: str, body: bytes | None,
                          deadline: float, operation) -> Response:
        """Run a non-idempotent operation at most once per client key.

        Without a key the operation runs normally: the dispatcher itself still
        refuses ambiguous retries, so a missing key never causes duplicate
        execution inside the proxy. With a key, concurrent and repeated
        requests replay the first result, and an unknown outcome is recorded
        so a retry cannot silently run the job again.
        """
        try:
            key = validate_key(request.headers.get("idempotency-key"))
        except ValueError as exc:
            raise DispatcherError(400, str(exc)) from exc
        if key is None:
            result = await operation()
            return self._finalize(result, request)

        scope = self.idempotency.scope(client_ip(request), key)
        body_fingerprint = fingerprint(
            request.method, path, request.url.query, body)

        owner = None
        follower = None
        for _ in range(2):
            try:
                owner, follower, replay = self.idempotency.begin(
                    scope, body_fingerprint)
            except IdempotencyConflict as exc:
                raise DispatcherError(409, str(exc)) from exc

            if replay is not None:
                return self._finalize(
                    self._from_stored(replay), request,
                    idempotency="ambiguous" if replay.ambiguous else "replay")

            if owner is not None:
                break
            if follower is None:
                break
            remaining = deadline - time.monotonic()
            try:
                replay = await self.idempotency.wait(follower, remaining)
            except IdempotencyWaitTimeout as exc:
                raise DispatcherError(
                    409,
                    "A request with this Idempotency-Key is still running. "
                    "Retry with the same key shortly.",
                    {"Retry-After": "5"},
                ) from exc
            except IdempotencyAbandoned:
                # The first attempt provably never reached a worker, so it is
                # safe for this caller to try to become the new owner.
                continue
            return self._finalize(
                self._from_stored(replay), request,
                idempotency="ambiguous" if replay.ambiguous else "replay")

        if owner is None:
            raise DispatcherError(
                503,
                "The original attempt did not reach a worker. Retry with the "
                "same Idempotency-Key.",
                {"Retry-After": "2"},
            )

        try:
            result = await operation()
        except DispatcherError as exc:
            if not exc.execution_started:
                self.idempotency.abandon(scope, owner, exc)
                raise
            stored = self._stored(self._error_result(exc), ambiguous=True)
            self.idempotency.complete(scope, owner, stored)
            return self._finalize(
                self._from_stored(stored), request, idempotency="ambiguous")
        except asyncio.CancelledError:
            # The caller vanished after forwarding may have started. Record
            # the unknown outcome so a retry cannot execute the job again.
            error = self._ambiguous_error(
                "The previous request was interrupted before its outcome was "
                "known. Retry with a new Idempotency-Key to run it again.")
            self.idempotency.complete(
                scope, owner, self._stored(
                    self._error_result(error), ambiguous=True))
            raise
        except Exception as exc:
            error = self._ambiguous_error(
                "The dispatcher failed while handling the request. The "
                "outcome is unknown; retry with a new Idempotency-Key.")
            self.idempotency.complete(
                scope, owner, self._stored(
                    self._error_result(error), ambiguous=True))
            raise error from exc

        if result.status_code in (502, 503, 504):
            if (result.status_code == 503
                    and looks_busy(result.status_code, result.body)):
                # The worker refused the work before running it; a retry with
                # the same key is safe and should be allowed.
                self.idempotency.abandon(
                    scope, owner,
                    IdempotencyAbandoned("Worker was busy before execution"))
                return self._finalize(result, request)
            stored = self._stored(result, ambiguous=True)
            self.idempotency.complete(scope, owner, stored)
            return self._finalize(
                self._from_stored(stored), request, idempotency="ambiguous")

        self.idempotency.complete(
            scope, owner, self._stored(result, ambiguous=False))
        return self._finalize(result, request)

    # --------------------------------------------------------------- forwarding

    async def _forward_buffered(self, request: Request, path: str,
                                profile: RouteProfile, deadline: float, *,
                                body: bytes | None = None,
                                heavy: bool = False,
                                verilator: bool = False,
                                affinity_tag: str | None = None) -> ProxyResult:
        max_attempts = min(profile.max_attempts, self.settings.max_attempts)
        if affinity_tag is not None:
            max_attempts = 1
        safe_method = request.method.upper() in ("GET", "HEAD", "OPTIONS")
        exclude: set[str] = set()
        last_result: ProxyResult | None = None

        def retry_after_send() -> bool:
            # A non-idempotent request may only be retried before it reaches
            # the application. After that point a retry could execute compute
            # work twice, so the dispatcher refuses unless HTTP semantics make
            # the method safe (GET/HEAD/OPTIONS).
            return (attempt < max_attempts
                    and affinity_tag is None
                    and safe_method)

        for attempt in range(1, max_attempts + 1):
            if time.monotonic() >= deadline:
                if last_result is not None:
                    return last_result
                raise DispatcherError(
                    504, "The request timed out before a worker was available")

            try:
                reservation = await self.pool.acquire(
                    heavy=heavy,
                    verilator=verilator,
                    exclude_tags=exclude,
                    deadline=deadline,
                    affinity_tag=affinity_tag,
                )
            except (QueueTimeout, NoWorkersAvailable,
                    AffinityWorkerNotFound) as exc:
                if last_result is not None:
                    return last_result
                raise self._error_from_pool(exc) from exc

            worker = reservation.worker
            try:
                upstream = await send_upstream(
                    self.http_client, self.settings, worker, request, path,
                    body, profile, deadline)
            except UpstreamNotSent as exc:
                await self.pool.release(reservation, "neutral", str(exc))
                raise DispatcherError(
                    504, "The request timed out before a worker was contacted"
                ) from exc
            except UpstreamConnectError as exc:
                # Connection setup failed: the worker never saw the request,
                # so retrying a POST here cannot duplicate execution.
                await self.pool.release(reservation, "failure", str(exc))
                if attempt < max_attempts and affinity_tag is None:
                    exclude.add(worker.tag)
                    if self.pool.has_alternative(exclude):
                        continue
                raise DispatcherError(
                    502, "The simulation worker is unavailable. Please try again.",
                    worker_tag=worker.tag, attempts=attempt,
                ) from exc
            except UpstreamTimeout as exc:
                await self.pool.release(reservation, "failure", str(exc))
                if retry_after_send() and profile.retry_read_timeout:
                    exclude.add(worker.tag)
                    if self.pool.has_alternative(exclude):
                        continue
                raise DispatcherError(
                    504, "The simulation worker timed out. Please try again.",
                    execution_started=True,
                    worker_tag=worker.tag, attempts=attempt,
                ) from exc
            except UpstreamTransportError as exc:
                await self.pool.release(reservation, "failure", str(exc))
                if retry_after_send():
                    exclude.add(worker.tag)
                    if self.pool.has_alternative(exclude):
                        continue
                raise DispatcherError(
                    502, "The simulation worker is unavailable. Please try again.",
                    execution_started=True,
                    worker_tag=worker.tag, attempts=attempt,
                ) from exc
            except InvalidUpstreamPath as exc:
                await self.pool.release(reservation, None)
                raise DispatcherError(400, "Invalid upstream request path") from exc
            except asyncio.CancelledError:
                await self.pool.release(reservation, None)
                raise
            except Exception:
                await self.pool.release(reservation, None)
                raise

            try:
                response_body = await read_buffered(
                    upstream, deadline,
                    self.settings.max_buffered_response_bytes)
                status_code = upstream.status_code
                headers = filtered_response_headers(
                    upstream.headers, drop_length=True, drop_encoding=True)
            except UpstreamTimeout as exc:
                await upstream.aclose()
                await self.pool.release(reservation, "failure", str(exc))
                if retry_after_send() and profile.retry_read_timeout:
                    exclude.add(worker.tag)
                    if self.pool.has_alternative(exclude):
                        continue
                raise DispatcherError(
                    504, "The simulation worker timed out. Please try again.",
                    execution_started=True,
                    worker_tag=worker.tag, attempts=attempt,
                ) from exc
            except (UpstreamTransportError, httpx.HTTPError) as exc:
                await upstream.aclose()
                await self.pool.release(reservation, "failure", str(exc))
                if retry_after_send():
                    exclude.add(worker.tag)
                    if self.pool.has_alternative(exclude):
                        continue
                raise DispatcherError(
                    502, "The simulation worker is unavailable. Please try again.",
                    execution_started=True,
                    worker_tag=worker.tag, attempts=attempt,
                ) from exc
            except ResponseTooLarge as exc:
                await upstream.aclose()
                await self.pool.release(reservation, "neutral", str(exc))
                raise DispatcherError(
                    502, "The simulation worker returned an oversized response.",
                    execution_started=True,
                    worker_tag=worker.tag, attempts=attempt,
                ) from exc
            except asyncio.CancelledError:
                await upstream.aclose()
                await self.pool.release(reservation, None)
                raise
            except Exception:
                await upstream.aclose()
                await self.pool.release(reservation, None)
                raise
            else:
                await upstream.aclose()

            outcome = classify_status(status_code)
            result = ProxyResult(
                status_code=status_code,
                headers=headers,
                body=response_body,
                worker_tag=worker.tag,
                attempts=attempt,
            )
            busy = looks_busy(status_code, response_body)
            if busy:
                # The worker answered; it is saturated, not broken. A soft
                # cooldown sheds load for a moment without opening its circuit.
                await self.pool.soft_cooldown(
                    worker.tag, self.settings.busy_cooldown_seconds)
                outcome = "neutral"
            error = f"HTTP {status_code}" if outcome == "failure" else None
            await self.pool.release(reservation, outcome, error)

            # A busy worker explicitly refused the work before running it, so
            # that is safe to retry even for a POST. A 502/504 response could
            # mean the job ran, so only safe methods are retried.
            retryable_response = busy or (outcome == "failure" and safe_method)
            if (retryable_response and attempt < max_attempts
                    and affinity_tag is None):
                exclude.add(worker.tag)
                last_result = result
                if self.pool.has_alternative(exclude):
                    continue
            return result

        if last_result is not None:
            return last_result
        raise DispatcherError(
            503, "All simulation workers are busy. Please try again shortly.",
            {"Retry-After": _RETRY_AFTER_QUEUE})

    async def _forward_stream(self, request: Request, path: str,
                              profile: RouteProfile, deadline: float, *,
                              affinity_tag: str) -> Response:
        """Forward an affinity-scoped read, streaming successful bodies."""
        try:
            reservation = await self.pool.acquire(affinity_tag=affinity_tag)
        except (QueueTimeout, NoWorkersAvailable,
                AffinityWorkerNotFound) as exc:
            raise self._error_from_pool(exc) from exc

        worker = reservation.worker
        try:
            upstream = await send_upstream(
                self.http_client, self.settings, worker, request, path,
                None, profile, deadline)
        except UpstreamNotSent as exc:
            await self.pool.release(reservation, "neutral", str(exc))
            raise DispatcherError(
                504, "The request timed out before a worker was contacted"
            ) from exc
        except (UpstreamConnectError, UpstreamTransportError) as exc:
            await self.pool.release(reservation, "failure", str(exc))
            raise DispatcherError(
                502, "The simulation worker is unavailable. Please try again.",
                worker_tag=worker.tag, attempts=1,
            ) from exc
        except UpstreamTimeout as exc:
            await self.pool.release(reservation, "failure", str(exc))
            raise DispatcherError(
                504, "The simulation worker timed out. Please try again.",
                worker_tag=worker.tag, attempts=1,
            ) from exc
        except InvalidUpstreamPath as exc:
            await self.pool.release(reservation, None)
            raise DispatcherError(400, "Invalid upstream request path") from exc
        except asyncio.CancelledError:
            await self.pool.release(reservation, None)
            raise
        except Exception:
            await self.pool.release(reservation, None)
            raise

        status_code = upstream.status_code
        if status_code >= 400 or response_is_empty(status_code, request.method):
            try:
                body = await read_buffered(
                    upstream, deadline,
                    self.settings.max_buffered_response_bytes)
                headers = filtered_response_headers(
                    upstream.headers, drop_length=True, drop_encoding=True)
            except (UpstreamTimeout, UpstreamTransportError,
                    ResponseTooLarge, httpx.HTTPError) as exc:
                await upstream.aclose()
                await self.pool.release(reservation, "failure", str(exc))
                raise DispatcherError(
                    502, "The simulation worker returned an unreadable response.",
                    worker_tag=worker.tag, attempts=1,
                ) from exc
            except Exception:
                await upstream.aclose()
                await self.pool.release(reservation, None)
                raise
            else:
                await upstream.aclose()
            outcome = classify_status(status_code)
            await self.pool.release(
                reservation, outcome,
                f"HTTP {status_code}" if outcome == "failure" else None)
            result = ProxyResult(
                status_code=status_code,
                headers=headers,
                body=body,
                worker_tag=worker.tag,
                attempts=1,
            )
            return self._finalize(result, request)

        # Bodies are decoded and the upstream length is dropped, so the
        # framing headers must go too; Starlette frames the response itself.
        headers = filtered_response_headers(
            upstream.headers, drop_length=True, drop_encoding=True)
        headers.append(("x-dispatcher-worker", worker.tag))
        headers.append(("x-dispatcher-attempts", "1"))
        if status_code == 503 and not header_value(headers, "retry-after"):
            headers.append(("retry-after", "10"))

        async def body_stream():
            completed = False
            failure: str | None = None
            try:
                # aiter_bytes also works for transports that hand back an
                # already-buffered response (and decodes if a worker ignores
                # Accept-Encoding: identity), unlike aiter_raw.
                async for chunk in upstream.aiter_bytes():
                    yield chunk
                completed = True
            except Exception as exc:  # transport failure after headers
                failure = str(exc)
                raise
            finally:
                try:
                    await upstream.aclose()
                finally:
                    if completed:
                        await self.pool.release(reservation, "success")
                    elif failure is not None:
                        await self.pool.release(reservation, "failure", failure)
                    else:
                        # Client disconnect or cancellation: not the worker's
                        # fault, but the slot must still come back.
                        await self.pool.release(reservation, None)

        return StreamingResponse(
            body_stream(), status_code=status_code, headers=dict(headers))

    # -------------------------------------------------------------- affinity

    @staticmethod
    def _valid_waveform_segment(value: str) -> tuple[str | None, str] | None:
        return affinity.decode_waveform_id(value)

    async def _probe_legacy(self, kind: str, raw: str) -> str | None:
        cache_key = (kind, raw)
        now = time.monotonic()
        cached = self._legacy_cache.get(cache_key)
        if cached is not None:
            tag, expires = cached
            if expires > now:
                return tag
            self._legacy_cache.pop(cache_key, None)

        path = (f"/api/waveform/{raw}/data" if kind == "waveform"
                else f"/api/share/{raw}")
        timeout = max(1.0, self.settings.legacy_probe_timeout_seconds)

        async def probe(worker: WorkerState) -> WorkerState | None:
            try:
                target = build_target_url(worker.url, path)
                probe_request = self.http_client.build_request(
                    "GET", target,
                    headers={"accept-encoding": "identity",
                             "x-forwarded-for": "dispatcher-probe",
                             **worker_auth_headers(self.settings)},
                    timeout=httpx.Timeout(
                        connect=min(timeout, self.settings.connect_timeout_seconds),
                        read=timeout,
                        write=timeout,
                        pool=timeout,
                    ),
                )
                response = await asyncio.wait_for(
                    self.http_client.send(probe_request, stream=True),
                    timeout=timeout)
                try:
                    return worker if response.status_code == 200 else None
                finally:
                    await response.aclose()
            except Exception:
                return None

        results = await asyncio.gather(
            *(probe(worker) for worker in self.pool.workers),
            return_exceptions=False,
        )
        for worker in results:
            if worker is not None:
                if len(self._legacy_cache) >= _LEGACY_PROBE_CACHE_MAX:
                    self._legacy_cache.pop(next(iter(self._legacy_cache)))
                self._legacy_cache[cache_key] = (
                    worker.tag, now + _LEGACY_PROBE_CACHE_SECONDS)
                return worker.tag
        return None

    async def _resolve_waveform(self, value: str) -> tuple[str, str, str]:
        decoded = affinity.decode_waveform_id(value)
        if decoded is None:
            raise DispatcherError(
                404, "Waveform not found or expired; run the simulation again")
        tag, raw = decoded
        if tag is None:
            tag = await self._probe_legacy("waveform", raw)
            if tag is None:
                raise DispatcherError(
                    404, "Waveform not found or expired; run the simulation again")
        else:
            try:
                self.pool.get_by_tag(tag)
            except AffinityWorkerNotFound as exc:
                raise DispatcherError(
                    404, "Waveform not found or expired; run the simulation again"
                ) from exc
        return tag, raw, f"{tag}{raw}"

    async def _resolve_share(self, value: str) -> tuple[str, str]:
        decoded = affinity.decode_share_id(value)
        if decoded is None:
            raise DispatcherError(400, "Invalid share id")
        tag, raw = decoded
        if tag is None:
            tag = await self._probe_legacy("share", raw)
            if tag is None:
                raise DispatcherError(404, "Shared project not found")
        else:
            try:
                self.pool.get_by_tag(tag)
            except AffinityWorkerNotFound as exc:
                raise DispatcherError(404, "Shared project not found") from exc
        return tag, raw

    # ------------------------------------------------------------------ routes

    async def root(self) -> dict:
        return {
            "status": "VLSI Practice Dispatcher",
            "version": "1.0",
            "workers": len(self.pool.workers),
            "features": ["worker-pool", "circuit-breaker", "affinity", "cache"],
        }

    async def backend_health(self) -> JSONResponse:
        snapshot = self.pool.snapshot()
        counts = self.pool.status_counts()
        healthy = bool(self.pool.workers) and not self.pool.all_circuits_open()
        payload = {
            "status": "healthy" if healthy else "degraded",
            "waveforms": 0,
            "problems": self.problem_count,
            "engines": dict(STATIC_ENGINES),
            "dispatcher": {
                "workers_configured": len(self.pool.workers),
                "workers_recently_responsive": counts["responsive"],
                "workers_warm": self.pool.warm_count(),
                "workers_cold": counts["cold"],
                "workers_unknown": counts["unknown"],
                "workers_circuit_open": counts["circuit_open"],
                "workers_cooling_down": counts["cooling_down"],
                "queue_depth": self.pool.queue_depth,
                "workers": snapshot,
            },
        }
        return JSONResponse(payload)

    async def backend_engines(self) -> JSONResponse:
        return JSONResponse({
            "iverilog": STATIC_ENGINES["iverilog"],
            "verilator": STATIC_ENGINES["verilator"],
            "yosys": STATIC_ENGINES["yosys"],
            "generations": list(DEFAULT_GENERATIONS),
        })

    async def dispatcher_health(self, request: Request) -> JSONResponse:
        snapshot = self.pool.snapshot()
        counts = self.pool.status_counts()
        status = "degraded" if self.pool.all_circuits_open() else "ok"
        payload = {
            "status": status,
            "service": "chipversity-dispatcher",
            "version": "1.0",
            "workers_configured": len(self.pool.workers),
            "workers_recently_responsive": counts["responsive"],
            "workers_warm": self.pool.warm_count(),
            "workers_cold": counts["cold"],
            "workers_unknown": counts["unknown"],
            "workers_circuit_open": counts["circuit_open"],
            "workers_cooling_down": counts["cooling_down"],
            "queue_depth": self.pool.queue_depth,
            "workers": snapshot,
            "cache": self.cache.summary(),
            "active_probe_interval_seconds":
                self.settings.health_probe_interval_seconds,
            # This endpoint is observational. A worker counted as unknown has
            # simply not served traffic since this process started; it may be
            # asleep, starting, or broken. Only ?probe=1 actively checks them.
            "observation": (
                "Counts reflect traffic observed by this dispatcher process. "
                "Unknown/cold workers are not known to be down. Add ?probe=1 "
                "to actively check every worker."),
        }
        probe = request.query_params.get("probe", "").lower()
        if probe in ("1", "true", "yes", "on"):
            payload["probe"] = await self._manual_probe()
        return JSONResponse(payload)

    async def _manual_probe(self) -> dict:
        timeout = max(1.0, min(
            self.settings.legacy_probe_timeout_seconds, 20.0))

        async def probe(worker: WorkerState) -> tuple[str, dict]:
            started = time.monotonic()
            try:
                response = await self.http_client.get(
                    worker.url + "/api/health",
                    headers={"accept-encoding": "identity",
                             **worker_auth_headers(self.settings)},
                    timeout=httpx.Timeout(
                        connect=min(timeout,
                                    self.settings.connect_timeout_seconds),
                        read=timeout, write=timeout, pool=timeout),
                )
                await response.aread()
                return worker.tag, {
                    "ok": response.status_code == 200,
                    "status": response.status_code,
                    "latency_ms": round(
                        (time.monotonic() - started) * 1000.0, 1),
                }
            except Exception as exc:
                return worker.tag, {
                    "ok": False,
                    "error": type(exc).__name__,
                    "latency_ms": round(
                        (time.monotonic() - started) * 1000.0, 1),
                }

        pairs = await asyncio.gather(
            *(probe(worker) for worker in self.pool.workers))
        return dict(pairs)

    async def catalog(self, request: Request) -> Response:
        path = request.url.path
        key = path
        deadline = self._deadline(CATALOG_PROFILE)
        async with self.cache.lock_for(key):
            fresh = self.cache.get_fresh(key)
            if fresh is not None:
                return self._cached_response(fresh, request, "hit")

            try:
                result = await self._forward_buffered(
                    request, path, CATALOG_PROFILE, deadline)
            except DispatcherError as exc:
                stale = self.cache.get_stale(key)
                if stale is not None:
                    return self._cached_response(stale, request, "stale")
                raise exc from None
            except ProxyError as exc:
                stale = self.cache.get_stale(key)
                if stale is not None:
                    return self._cached_response(stale, request, "stale")
                raise DispatcherError(
                    502, "The problem catalog is temporarily unavailable"
                ) from exc

            if 200 <= result.status_code < 300:
                content_type = content_type_of(result.headers)
                if is_json_content_type(content_type):
                    self.cache.set(
                        key, result.body,
                        status_code=result.status_code,
                        content_type=content_type or "application/json",
                        worker_tag=result.worker_tag,
                    )
                    if key == "/api/problems":
                        self._update_problem_count(result.body)
                    return self._finalize(result, request, cache_state="miss")
                return self._finalize(result, request, cache_state="miss")

            if result.status_code >= 500:
                stale = self.cache.get_stale(key)
                if stale is not None:
                    return self._cached_response(stale, request, "stale")
            return self._finalize(result, request, cache_state="miss")

    def _cached_response(self, entry: CacheEntry, request: Request,
                         state: str) -> Response:
        result = ProxyResult(
            status_code=entry.status_code,
            headers=[("content-type", entry.content_type or "application/json")],
            body=entry.body,
            worker_tag=entry.worker_tag,
            attempts=0,
        )
        return self._finalize(
            result, request, cache_state=state, cache_age=entry.age())

    def _update_problem_count(self, body: bytes) -> None:
        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return
        # The shipped backend wraps the list in {"problems": [...]}, but the
        # frontend also accepts a bare array and some deployments serve one.
        if isinstance(payload, list):
            self.problem_count = len(payload)
        elif isinstance(payload, dict):
            problems = payload.get("problems")
            if isinstance(problems, list):
                self.problem_count = len(problems)

    async def run(self, request: Request) -> Response:
        deadline = self._deadline(RUN_PROFILE)
        body = await self._read_body(request, deadline)

        async def operation() -> ProxyResult:
            result = await self._forward_buffered(
                request, "/api/run", RUN_PROFILE, deadline, body=body)
            return self._rewrite_json(result, result.worker_tag)

        return await self._idempotent(
            request, "/api/run", body, deadline, operation)

    async def submit(self, request: Request) -> Response:
        deadline = self._deadline(RUN_PROFILE)
        body = await self._read_body(request, deadline)

        async def operation() -> ProxyResult:
            return await self._forward_buffered(
                request, "/api/submit", RUN_PROFILE, deadline, body=body)

        return await self._idempotent(
            request, "/api/submit", body, deadline, operation)

    async def run_custom(self, request: Request) -> Response:
        deadline = self._deadline(RUN_PROFILE)
        body = await self._read_body(request, deadline)

        async def operation() -> ProxyResult:
            result = await self._forward_buffered(
                request, "/api/dev/run-custom", RUN_PROFILE, deadline,
                body=body)
            return self._rewrite_json(result, result.worker_tag)

        return await self._idempotent(
            request, "/api/dev/run-custom", body, deadline, operation)

    async def run_advanced(self, request: Request) -> Response:
        deadline = self._deadline(ADVANCED_PROFILE)
        body = await self._read_body(request, deadline)
        engine = ""
        if body:
            try:
                payload = json.loads(body.decode("utf-8"))
                if isinstance(payload, dict):
                    engine = str(payload.get("engine", "")).lower()
            except (UnicodeDecodeError, json.JSONDecodeError):
                pass

        async def operation() -> ProxyResult:
            result = await self._forward_buffered(
                request, "/api/run-advanced", ADVANCED_PROFILE, deadline,
                body=body, verilator=(engine == "verilator"))
            return self._rewrite_json(result, result.worker_tag)

        return await self._idempotent(
            request, "/api/run-advanced", body, deadline, operation)

    async def lint(self, request: Request) -> Response:
        deadline = self._deadline(LINT_PROFILE)
        body = await self._read_body(request, deadline)

        async def operation() -> ProxyResult:
            return await self._forward_buffered(
                request, "/api/lint", LINT_PROFILE, deadline,
                body=body, heavy=True)

        return await self._idempotent(
            request, "/api/lint", body, deadline, operation)

    async def synthesize(self, request: Request) -> Response:
        deadline = self._deadline(SYNTH_PROFILE)
        body = await self._read_body(request, deadline)

        async def operation() -> ProxyResult:
            return await self._forward_buffered(
                request, "/api/synthesize", SYNTH_PROFILE, deadline,
                body=body, heavy=True)

        return await self._idempotent(
            request, "/api/synthesize", body, deadline, operation)

    async def dev_audit(self, request: Request) -> Response:
        if not self.settings.enable_dev_audit:
            raise DispatcherError(
                503,
                "The development audit is disabled through the dispatcher. "
                "Set ENABLE_DEV_AUDIT=true only for a deliberate one-off run.",
            )
        deadline = self._deadline(DEV_AUDIT_PROFILE)
        result = await self._forward_buffered(
            request, "/api/dev/audit", DEV_AUDIT_PROFILE, deadline, heavy=True)
        return self._finalize(result, request)

    async def waveform_data(self, request: Request, waveform_id: str) -> Response:
        tag, raw, _tagged = await self._resolve_waveform(waveform_id)
        deadline = self._deadline(STATE_READ_PROFILE)
        return await self._forward_stream(
            request, f"/api/waveform/{raw}/data", STATE_READ_PROFILE,
            deadline, affinity_tag=tag)

    async def waveform(self, request: Request, waveform_id: str) -> Response:
        tag, raw, tagged = await self._resolve_waveform(waveform_id)
        download = request.query_params.get("download", "").lower() in (
            "1", "true", "yes", "on")
        if download:
            deadline = self._deadline(WAVEFORM_DOWNLOAD_PROFILE)
            return await self._forward_stream(
                request, f"/api/waveform/{raw}", WAVEFORM_DOWNLOAD_PROFILE,
                deadline, affinity_tag=tag)

        deadline = self._deadline(STATE_READ_PROFILE)
        result = await self._forward_buffered(
            request, f"/api/waveform/{raw}", STATE_READ_PROFILE, deadline,
            affinity_tag=tag)
        if (200 <= result.status_code < 300
                and is_html_content_type(content_type_of(result.headers))):
            try:
                html = result.body.decode("utf-8")
            except UnicodeDecodeError:
                pass
            else:
                rewritten = affinity.rewrite_waveform_html(html, raw, tagged)
                if rewritten != html:
                    result = self._replace_body(
                        result, rewritten.encode("utf-8"))
        return self._finalize(result, request)

    async def share_create(self, request: Request) -> Response:
        deadline = self._deadline(SHARE_CREATE_PROFILE)
        body = await self._read_body(request, deadline)

        async def operation() -> ProxyResult:
            result = await self._forward_buffered(
                request, "/api/share", SHARE_CREATE_PROFILE, deadline,
                body=body)
            return self._rewrite_json(result, result.worker_tag, share=True)

        return await self._idempotent(
            request, "/api/share", body, deadline, operation)

    async def share_get(self, request: Request, sid: str) -> Response:
        tag, raw = await self._resolve_share(sid)
        deadline = self._deadline(STATE_READ_PROFILE)
        result = await self._forward_buffered(
            request, f"/api/share/{raw}", STATE_READ_PROFILE, deadline,
            affinity_tag=tag)
        return self._finalize(result, request)

    async def generic(self, request: Request, path: str) -> Response:
        # The catch-all pattern is /api/{path:path}, so the captured value is
        # relative to /api; the worker must still see the full public path.
        upstream_path = "/api/" + path.lstrip("/")
        read_route = request.method.upper() in ("GET", "HEAD", "OPTIONS")
        profile = (GENERIC_READ_PROFILE if read_route
                   else GENERIC_WRITE_PROFILE)
        deadline = self._deadline(profile)
        body = await self._read_body(request, deadline)

        async def operation() -> ProxyResult:
            return await self._forward_buffered(
                request, upstream_path, profile, deadline, body=body)

        if read_route:
            return self._finalize(await operation(), request)
        return await self._idempotent(
            request, upstream_path, body, deadline, operation)


def create_app(settings: Settings | None = None, *,
               upstream_transport: httpx.AsyncBaseTransport | None = None,
               http_client: httpx.AsyncClient | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    logger.info(
        "Starting dispatcher for %d workers: %s", len(settings.workers),
        ", ".join(worker.tag for worker in settings.workers))

    pool = WorkerPool(settings)
    cache = ResponseCache(
        settings.catalog_ttl_seconds, settings.catalog_stale_seconds)
    owns_client = http_client is None
    if http_client is None:
        http_client = httpx.AsyncClient(
            transport=upstream_transport,
            follow_redirects=False,
            trust_env=False,
            limits=httpx.Limits(
                max_connections=100, max_keepalive_connections=20,
                keepalive_expiry=30.0),
            timeout=httpx.Timeout(
                connect=settings.connect_timeout_seconds,
                read=60.0,
                write=settings.write_timeout_seconds,
                pool=settings.pool_timeout_seconds,
            ),
        )
    service = DispatcherService(settings, pool, cache, http_client)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        yield
        if owns_client:
            await http_client.aclose()

    app = FastAPI(
        title="CHIPVERSITY Verilog Dispatcher",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.pool = pool
    app.state.cache = cache
    app.state.service = service
    app.state.http_client = http_client

    # Added first so the CORS middleware (added last) wraps rate-limit errors.
    app.add_middleware(RateLimitMiddleware, settings=settings)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=[
            "Retry-After", "X-Dispatcher-Worker",
            "X-Dispatcher-Attempts", "X-Dispatcher-Cache",
            "X-Dispatcher-Idempotency",
        ],
    )

    @app.exception_handler(DispatcherError)
    async def dispatcher_error_handler(_request: Request,
                                       exc: DispatcherError) -> JSONResponse:
        headers = {name.lower(): value for name, value in exc.headers.items()}
        headers.setdefault("x-dispatcher-attempts", str(exc.attempts))
        if exc.worker_tag:
            headers.setdefault("x-dispatcher-worker", exc.worker_tag)
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail},
            headers=headers,
        )

    app.add_api_route("/", service.root, methods=["GET"])
    app.add_api_route(
        "/dispatcher/health", service.dispatcher_health, methods=["GET"])
    app.add_api_route("/api/health", service.backend_health, methods=["GET"])
    app.add_api_route("/api/engines", service.backend_engines, methods=["GET"])
    app.add_api_route("/api/dev/audit", service.dev_audit, methods=["GET"])

    for catalog_path in sorted(CATALOG_PATHS):
        app.add_api_route(catalog_path, service.catalog, methods=["GET"])

    app.add_api_route("/api/run", service.run, methods=["POST"])
    app.add_api_route("/api/submit", service.submit, methods=["POST"])
    app.add_api_route(
        "/api/dev/run-custom", service.run_custom, methods=["POST"])
    app.add_api_route(
        "/api/run-advanced", service.run_advanced, methods=["POST"])
    app.add_api_route("/api/lint", service.lint, methods=["POST"])
    app.add_api_route("/api/synthesize", service.synthesize, methods=["POST"])
    app.add_api_route("/api/share", service.share_create, methods=["POST"])
    app.add_api_route(
        "/api/share/{sid}", service.share_get, methods=["GET"])
    app.add_api_route(
        "/api/waveform/{waveform_id}/data", service.waveform_data,
        methods=["GET"])
    app.add_api_route(
        "/api/waveform/{waveform_id}", service.waveform, methods=["GET"])
    app.add_api_route(
        "/api/{path:path}", service.generic, methods=_METHODS)

    return app


def _misconfigured_app(error: ConfigError) -> FastAPI:
    app = FastAPI(title="CHIPVERSITY Verilog Dispatcher (misconfigured)")
    app.add_middleware(
        CORSMiddleware, allow_origins=["*"], allow_credentials=False,
        allow_methods=["*"], allow_headers=["*"])

    def payload() -> JSONResponse:
        return JSONResponse(
            status_code=503,
            content={
                "status": "error",
                "detail": f"Dispatcher is not configured: {error}",
            },
        )

    @app.get("/dispatcher/health")
    async def health() -> JSONResponse:
        return payload()

    @app.api_route("/{path:path}", methods=_METHODS)
    async def unavailable(path: str) -> JSONResponse:
        return payload()

    app.state.config_error = str(error)
    return app


def _module_app() -> FastAPI:
    try:
        return create_app()
    except ConfigError as exc:
        logger.error("Dispatcher configuration error: %s", exc)
        return _misconfigured_app(exc)


app = _module_app()
