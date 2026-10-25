"""HTTP forwarding primitives for the dispatcher.

Only configured worker origins are ever contacted.  Paths are validated before
they are concatenated onto a worker origin, redirects are never followed, and
hop-by-hop / upstream CORS headers are removed so the dispatcher remains the
single public API surface.
"""

from __future__ import annotations

import asyncio
import ipaddress
import re
import time
from urllib.parse import unquote, urlsplit

import httpx
from starlette.requests import Request

from .config import RouteProfile, Settings
from .workers import WorkerState


class ProxyError(Exception):
    """Base class for failures while talking to a worker."""


class InvalidUpstreamPath(ProxyError):
    pass


class UpstreamNotSent(ProxyError):
    """The request exhausted its budget before any connection was attempted."""


class UpstreamConnectError(ProxyError):
    """Connection setup failed, so the request provably never reached the app.

    Only these transport failures are safe to retry for a non-idempotent
    request without an idempotency record.
    """


class UpstreamTimeout(ProxyError):
    """The request may have been sent; its execution state is unknown."""


class UpstreamTransportError(ProxyError):
    """A transport failure after the request may have been sent."""


class ResponseTooLarge(ProxyError):
    pass


_HOP_BY_HOP = frozenset({
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailer", "trailers", "transfer-encoding", "upgrade",
    "proxy-connection",
})
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")
_ENCODED_SEPARATOR_RE = re.compile(r"%(?:2f|5c)", re.IGNORECASE)
_RETRYABLE_STATUS = frozenset({502, 503, 504})
# Never trust an inbound provenance header: the dispatcher writes its own.
_PROVENANCE_HEADERS = frozenset({
    "x-forwarded-for", "x-real-ip", "x-forwarded-proto",
    "x-forwarded-host", "forwarded", "x-dispatcher-token",
})
WORKER_AUTH_HEADER = "x-dispatcher-token"


def classify_status(status_code: int) -> str:
    if status_code in _RETRYABLE_STATUS:
        return "failure"
    if 200 <= status_code < 400:
        return "success"
    return "neutral"


def is_retryable_status(status_code: int) -> bool:
    return status_code in _RETRYABLE_STATUS


def looks_busy(status_code: int, body: bytes) -> bool:
    if status_code != 503:
        return False
    return b"busy" in body[:4096].lower()


def _parse_forwarded_address(value: str) -> str | None:
    candidate = value.strip()
    if candidate.startswith("["):
        bracket_end = candidate.find("]")
        if bracket_end == -1:
            return None
        candidate = candidate[1:bracket_end]
    elif candidate.count(":") == 1:
        host, _, port = candidate.partition(":")
        if port.isdigit():
            candidate = host
    try:
        return str(ipaddress.ip_address(candidate))
    except ValueError:
        return None


def client_ip(request: Request) -> str:
    """Return the caller's address as observed by Render.

    Render sets the first ``X-Forwarded-For`` entry to the real client IP and
    leaves any caller-supplied values after it, so only that first entry is
    trustworthy. Ignoring trailing entries stops a client from rotating a
    forged address past the rate limiter.
    """
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        parsed = _parse_forwarded_address(forwarded.split(",", 1)[0])
        if parsed is not None:
            return parsed
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def forwarded_scheme(request: Request) -> str:
    proto = request.headers.get("x-forwarded-proto", "")
    if proto:
        proto = proto.split(",", 1)[0].strip().lower()
    if proto in {"http", "https"}:
        return proto
    return request.url.scheme


def _connection_tokens(request: Request) -> set[str]:
    raw = request.headers.get("connection", "")
    return {token.strip().lower() for token in raw.split(",") if token.strip()}


def worker_auth_headers(settings: Settings) -> dict[str, str]:
    if not settings.worker_auth_token:
        return {}
    return {WORKER_AUTH_HEADER: settings.worker_auth_token}


def build_upstream_headers(request: Request,
                           settings: Settings) -> list[tuple[str, str]]:
    connection_tokens = _connection_tokens(request)
    forwarded: list[tuple[str, str]] = []
    for key, value in request.headers.items():
        lower = key.lower()
        if lower in _HOP_BY_HOP or lower in connection_tokens:
            continue
        if lower in {"host", "content-length", "accept-encoding"}:
            continue
        if lower in _PROVENANCE_HEADERS:
            continue
        forwarded.append((lower, value))

    ip = client_ip(request)
    forwarded.append(("accept-encoding", "identity"))
    # Client-supplied provenance headers were dropped above, so these are the
    # only values the worker sees.
    forwarded.append(("x-forwarded-for", ip))
    forwarded.append(("x-real-ip", ip))
    forwarded.append(("x-forwarded-proto", forwarded_scheme(request)))
    host = request.headers.get("host")
    if host:
        forwarded.append(("x-forwarded-host", host))
    if settings.worker_auth_token:
        forwarded.append(
            (WORKER_AUTH_HEADER, settings.worker_auth_token))
    return forwarded


def build_target_url(worker_url: str, path: str, query: str = "") -> str:
    if not path.startswith("/"):
        raise InvalidUpstreamPath("Upstream path must start with a slash")
    if _CONTROL_RE.search(path) or "\\" in path:
        raise InvalidUpstreamPath("Invalid character in upstream path")
    decoded = unquote(path)
    if ("\\" in decoded or decoded.startswith("//") or "://" in decoded
            or "//" in decoded
            or any(segment == ".." for segment in decoded.split("/"))):
        raise InvalidUpstreamPath("Refusing a non-local upstream path")
    if _ENCODED_SEPARATOR_RE.search(path):
        raise InvalidUpstreamPath("Encoded path separators are not allowed")

    target = worker_url + path
    if query:
        target += "?" + query

    parsed = urlsplit(target)
    worker = urlsplit(worker_url)
    if (parsed.scheme != worker.scheme
            or parsed.netloc.lower() != worker.netloc.lower()):
        raise InvalidUpstreamPath("Upstream URL escaped the worker origin")
    return target


def timeout_for(settings: Settings, profile: RouteProfile, *,
                cold: bool = False) -> httpx.Timeout:
    connect_timeout = settings.connect_timeout_seconds
    if cold:
        # Render Free holds a request while a sleeping instance starts.  A
        # cold worker therefore gets the whole wake budget to establish the
        # connection; the overall request deadline still caps it.
        connect_timeout = max(
            connect_timeout, settings.cold_start_budget_seconds)
    return httpx.Timeout(
        connect=connect_timeout,
        read=profile.read_timeout,
        write=settings.write_timeout_seconds,
        pool=settings.pool_timeout_seconds,
    )


async def send_upstream(client: httpx.AsyncClient, settings: Settings,
                        worker: WorkerState, request: Request, path: str,
                        body: bytes | None, profile: RouteProfile,
                        deadline: float) -> httpx.Response:
    """Send a request and return once response headers arrive."""
    target = build_target_url(worker.url, path, request.url.query)
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise UpstreamNotSent("Request budget exhausted before upstream call")
    headers = build_upstream_headers(request, settings)
    cold = not worker.is_warm(
        time.monotonic(), settings.warm_window_seconds)
    upstream_request = client.build_request(
        request.method,
        target,
        headers=headers,
        content=body,
        timeout=timeout_for(settings, profile, cold=cold),
    )
    try:
        return await asyncio.wait_for(
            client.send(upstream_request, stream=True), timeout=remaining)
    except (httpx.ConnectError, httpx.ConnectTimeout,
            httpx.PoolTimeout) as exc:
        # No application bytes were sent, so even a POST is safe to retry.
        raise UpstreamConnectError(str(exc) or "Worker connect failed") from exc
    except (asyncio.TimeoutError, httpx.TimeoutException) as exc:
        # Includes a wait_for cancellation while connecting: the kernel may or
        # may not have completed the connection, so execution is unknown.
        raise UpstreamTimeout(str(exc) or "Worker timed out") from exc
    except httpx.TransportError as exc:
        raise UpstreamTransportError(str(exc) or "Worker transport error") from exc
    except httpx.HTTPError as exc:
        raise UpstreamTransportError(str(exc) or "Worker request error") from exc


async def read_buffered(response: httpx.Response, deadline: float,
                        max_bytes: int) -> bytes:
    async def collect() -> bytes:
        chunks: list[bytes] = []
        total = 0
        async for chunk in response.aiter_bytes():
            total += len(chunk)
            if total > max_bytes:
                raise ResponseTooLarge(
                    f"Worker response exceeded {max_bytes} bytes")
            chunks.append(chunk)
        return b"".join(chunks)

    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise UpstreamTimeout("Request budget exhausted while reading response")
    try:
        return await asyncio.wait_for(collect(), timeout=remaining)
    except (asyncio.TimeoutError, httpx.TimeoutException) as exc:
        raise UpstreamTimeout(str(exc) or "Worker response timed out") from exc
    except httpx.TransportError as exc:
        raise UpstreamTransportError(
            str(exc) or "Worker response transport error") from exc


def filtered_response_headers(headers: httpx.Headers, *,
                              drop_length: bool = False,
                              drop_encoding: bool = False
                              ) -> list[tuple[str, str]]:
    connection_tokens: set[str] = set()
    for raw in headers.get_list("connection"):
        connection_tokens.update(
            token.strip().lower() for token in raw.split(",") if token.strip())

    result: list[tuple[str, str]] = []
    for key, value in headers.multi_items():
        lower = key.lower()
        if lower in _HOP_BY_HOP or lower in connection_tokens:
            continue
        if lower.startswith("access-control-"):
            continue
        if lower.startswith("x-dispatcher-"):
            continue
        if drop_length and lower == "content-length":
            continue
        if drop_encoding and lower == "content-encoding":
            continue
        result.append((lower, value))
    return result


def header_value(headers: list[tuple[str, str]], name: str,
                 default: str = "") -> str:
    lower = name.lower()
    for key, value in headers:
        if key.lower() == lower:
            return value
    return default


def content_type_of(headers: list[tuple[str, str]] | httpx.Headers) -> str:
    if isinstance(headers, httpx.Headers):
        return headers.get("content-type", "")
    return header_value(headers, "content-type", "")


def is_json_content_type(content_type: str) -> bool:
    base = content_type.split(";", 1)[0].strip().lower()
    return base == "application/json" or base.endswith("+json")


def is_html_content_type(content_type: str) -> bool:
    return content_type.split(";", 1)[0].strip().lower() == "text/html"


def response_is_empty(status_code: int, method: str) -> bool:
    return (method.upper() == "HEAD"
            or status_code in (204, 304)
            or 100 <= status_code < 200)
