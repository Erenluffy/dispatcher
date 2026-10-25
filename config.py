"""Configuration for the CHIPVERSITY Verilog dispatcher.

All runtime configuration comes from environment variables so the same image
can be deployed to Render without code changes.  Settings are validated once
at startup: a bad ``WORKER_URLS`` value must fail loudly instead of producing
requests against a half-configured fleet.
"""

from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from typing import Mapping
from urllib.parse import urlsplit, urlunsplit


class ConfigError(ValueError):
    """Raised when the dispatcher environment is invalid."""


_TAG_RE = re.compile(r"^[0-9a-f]{5}$")
_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}


@dataclass(frozen=True)
class WorkerConfig:
    """A single backend worker.

    ``url`` is always an origin (no path, query or fragment).  Keeping it
    origin-only is what makes URL construction unambiguous and closes off
    request-path tricks that could redirect traffic to another host.
    """

    url: str
    tag: str


@dataclass(frozen=True)
class RouteProfile:
    """Timeouts and retry behaviour for one class of request."""

    name: str
    read_timeout: float
    total_timeout: float
    retry_read_timeout: bool = False
    heavy: bool = False
    verilator: bool = False
    stream: bool = False
    max_attempts: int = 2


# Profiles are deliberately generous enough to cover a Render Free cold start
# (typically 30-90s) plus the worker's own documented budgets.  ``total_timeout``
# includes time spent queued inside the dispatcher.
# Budgets are worst-case sums: queue wait + cold start + worker read time,
# plus a 30 s margin for request/response framing.  ``test_route_budgets``
# enforces that relationship so a future profile edit cannot quietly leave a
# request without enough time to wake a sleeping Render Free worker.
CATALOG_PROFILE = RouteProfile(
    "catalog", read_timeout=90, total_timeout=270, retry_read_timeout=True)
RUN_PROFILE = RouteProfile(
    "run", read_timeout=150, total_timeout=330)
SHARE_CREATE_PROFILE = RouteProfile(
    "share-create", read_timeout=60, total_timeout=240)
ADVANCED_PROFILE = RouteProfile(
    "run-advanced", read_timeout=330, total_timeout=510)
LINT_PROFILE = RouteProfile(
    "lint", read_timeout=150, total_timeout=330, heavy=True)
SYNTH_PROFILE = RouteProfile(
    "synthesize", read_timeout=180, total_timeout=360, heavy=True)
STATE_READ_PROFILE = RouteProfile(
    "state-read", read_timeout=120, total_timeout=300)
WAVEFORM_DOWNLOAD_PROFILE = RouteProfile(
    "waveform-download", read_timeout=180, total_timeout=300, stream=True,
    max_attempts=1)
GENERIC_READ_PROFILE = RouteProfile(
    "generic-read", read_timeout=60, total_timeout=240, retry_read_timeout=True)
GENERIC_WRITE_PROFILE = RouteProfile(
    "generic-write", read_timeout=60, total_timeout=240)
DEV_AUDIT_PROFILE = RouteProfile(
    "dev-audit", read_timeout=300, total_timeout=480, max_attempts=1)

CATALOG_PATHS = frozenset({"/api/problems", "/api/categories", "/api/svproblems"})

DEFAULT_GENERATIONS = ["1995", "2001", "2005", "2009", "2012",
                       "systemverilog", "sv"]

# The worker Docker image installs all four tools.  These values let the
# synthetic /api/engines and /api/health endpoints answer without waking a
# sleeping worker; a real compute request still reports toolchain errors from
# the worker if an image diverges.
STATIC_ENGINES = {
    "iverilog": True,
    "verilator": True,
    "verilator_coverage": True,
    "yosys": True,
}


def _parse_int(env: Mapping[str, str], key: str, default: int, *,
               minimum: int = 0) -> int:
    raw = env.get(key)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        value = int(str(raw).strip())
    except ValueError as exc:
        raise ConfigError(f"{key} must be an integer, got {raw!r}") from exc
    if value < minimum:
        raise ConfigError(f"{key} must be >= {minimum}, got {value}")
    return value


def _parse_float(env: Mapping[str, str], key: str, default: float, *,
                 minimum: float = 0.0) -> float:
    raw = env.get(key)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        value = float(str(raw).strip())
    except ValueError as exc:
        raise ConfigError(f"{key} must be a number, got {raw!r}") from exc
    if value < minimum:
        raise ConfigError(f"{key} must be >= {minimum}, got {value}")
    return value


def _parse_bool(env: Mapping[str, str], key: str, default: bool) -> bool:
    raw = env.get(key)
    if raw is None or str(raw).strip() == "":
        return default
    value = str(raw).strip().lower()
    if value in _TRUE_VALUES:
        return True
    if value in _FALSE_VALUES:
        return False
    raise ConfigError(f"{key} must be a boolean, got {raw!r}")


def _normalize_worker_url(raw: str, index: int) -> str:
    value = raw.strip().rstrip("/")
    if not value:
        raise ConfigError(f"WORKER_URLS entry {index + 1} is empty")
    parsed = urlsplit(value)
    if parsed.scheme not in ("http", "https"):
        raise ConfigError(
            f"WORKER_URLS entry {index + 1} must start with http:// or https://")
    if not parsed.netloc or not parsed.hostname:
        raise ConfigError(f"WORKER_URLS entry {index + 1} has no host")
    if parsed.username or parsed.password:
        raise ConfigError(
            f"WORKER_URLS entry {index + 1} must not contain credentials")
    if parsed.query or parsed.fragment:
        raise ConfigError(
            f"WORKER_URLS entry {index + 1} must not contain a query or fragment")
    if parsed.path not in ("", "/"):
        raise ConfigError(
            f"WORKER_URLS entry {index + 1} must be an origin (no path)")
    return urlunsplit((parsed.scheme.lower(), parsed.netloc, "", "", ""))


def _default_tags(urls: list[str]) -> list[str]:
    """Derive stable tags from worker URLs.

    Waveform/share IDs embed the tag of the worker that owns the file.  A tag
    derived from the URL therefore survives reordering or adding workers,
    which an index-based tag does not.  A collision is astronomically unlikely
    but would be silent data corruption, so it is a configuration error and
    the operator must pin ``WORKER_TAGS`` explicitly.
    """
    tags = [hashlib.sha256(url.encode("utf-8")).hexdigest()[:5]
            for url in urls]
    if len(set(tags)) != len(tags):
        raise ConfigError(
            "Two worker URLs hash to the same default tag; set WORKER_TAGS "
            "explicitly to one unique five-character hex tag per worker")
    return tags


def _parse_worker_auth_token(env: Mapping[str, str]) -> str:
    raw = str(env.get("WORKER_AUTH_TOKEN", "")).strip()
    if not raw:
        raise ConfigError(
            "WORKER_AUTH_TOKEN is required so workers can reject requests "
            "that bypass the dispatcher")
    if len(raw) < 16:
        raise ConfigError("WORKER_AUTH_TOKEN must be at least 16 characters")
    if any(char.isspace() for char in raw):
        raise ConfigError("WORKER_AUTH_TOKEN must not contain whitespace")
    return raw


@dataclass(frozen=True)
class Settings:
    workers: tuple[WorkerConfig, ...]
    worker_auth_token: str = ""
    worker_max_inflight: int = 3
    heavy_max_inflight: int = 1
    verilator_max_inflight: int = 1
    max_cold_workers: int = 2
    queue_timeout_seconds: float = 60.0
    cold_start_budget_seconds: float = 90.0
    warm_window_seconds: float = 600.0
    max_attempts: int = 2
    failure_threshold: int = 3
    cooldown_base_seconds: float = 30.0
    cooldown_max_seconds: float = 300.0
    busy_cooldown_seconds: float = 12.0
    connect_timeout_seconds: float = 10.0
    write_timeout_seconds: float = 30.0
    pool_timeout_seconds: float = 10.0
    catalog_ttl_seconds: float = 1800.0
    catalog_stale_seconds: float = 604800.0
    max_request_bytes: int = 2_000_000
    max_buffered_response_bytes: int = 32_000_000
    idempotency_ttl_seconds: float = 900.0
    idempotency_max_entries: int = 512
    enable_dev_audit: bool = False
    health_probe_interval_seconds: float = 0.0
    legacy_probe_timeout_seconds: float = 15.0
    enable_rate_limits: bool = True
    run_rate_limit_per_minute: int = 10
    advanced_rate_limit_per_minute: int = 20
    rate_limit_window_seconds: float = 60.0

    @property
    def worker_tags(self) -> tuple[str, ...]:
        return tuple(worker.tag for worker in self.workers)

    @property
    def worker_by_tag(self) -> dict[str, WorkerConfig]:
        return {worker.tag: worker for worker in self.workers}

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "Settings":
        env = os.environ if env is None else env

        raw_urls = env.get("WORKER_URLS", "")
        if not raw_urls.strip():
            raise ConfigError(
                "WORKER_URLS is required (comma-separated worker origins; "
                "set it in the Render dashboard after creating the workers)")
        parts = [part for part in raw_urls.split(",") if part.strip()]
        if not parts:
            raise ConfigError("WORKER_URLS contains no worker URLs")
        urls = [_normalize_worker_url(part, i) for i, part in enumerate(parts)]
        if len(set(urls)) != len(urls):
            raise ConfigError("WORKER_URLS contains duplicate worker URLs")
        if len(urls) > 64:
            raise ConfigError("WORKER_URLS supports at most 64 workers")

        raw_tags = env.get("WORKER_TAGS", "").strip()
        if raw_tags:
            tags = [tag.strip().lower() for tag in raw_tags.split(",") if tag.strip()]
            if len(tags) != len(urls):
                raise ConfigError(
                    f"WORKER_TAGS must contain exactly {len(urls)} tags")
        else:
            tags = _default_tags(urls)
        for tag in tags:
            if not _TAG_RE.match(tag):
                raise ConfigError(
                    "each worker tag must be exactly 5 lowercase hex characters")
        if len(set(tags)) != len(tags):
            raise ConfigError("WORKER_TAGS contains duplicate tags")

        worker_max = _parse_int(env, "WORKER_MAX_INFLIGHT", 3, minimum=1)
        heavy_max = _parse_int(env, "HEAVY_MAX_INFLIGHT", 1, minimum=1)
        verilator_max = _parse_int(env, "VERILATOR_MAX_INFLIGHT", 1, minimum=1)
        if heavy_max > worker_max:
            raise ConfigError("HEAVY_MAX_INFLIGHT cannot exceed WORKER_MAX_INFLIGHT")
        if verilator_max > worker_max:
            raise ConfigError(
                "VERILATOR_MAX_INFLIGHT cannot exceed WORKER_MAX_INFLIGHT")

        cooldown_base = _parse_float(
            env, "COOLDOWN_BASE_SECONDS", 30.0, minimum=0.1)
        cooldown_max = _parse_float(
            env, "COOLDOWN_MAX_SECONDS", 300.0, minimum=0.1)
        if cooldown_max < cooldown_base:
            raise ConfigError(
                "COOLDOWN_MAX_SECONDS must be >= COOLDOWN_BASE_SECONDS")

        workers = tuple(
            WorkerConfig(url=url, tag=tag) for url, tag in zip(urls, tags))

        return cls(
            workers=workers,
            worker_auth_token=_parse_worker_auth_token(env),
            worker_max_inflight=worker_max,
            heavy_max_inflight=heavy_max,
            verilator_max_inflight=verilator_max,
            max_cold_workers=_parse_int(
                env, "MAX_COLD_WORKERS", 2, minimum=1),
            queue_timeout_seconds=_parse_float(
                env, "QUEUE_TIMEOUT_SECONDS", 60.0, minimum=0.0),
            cold_start_budget_seconds=_parse_float(
                env, "COLD_START_BUDGET_SECONDS", 90.0, minimum=0.0),
            warm_window_seconds=_parse_float(
                env, "WARM_WINDOW_SECONDS", 600.0, minimum=1.0),
            max_attempts=_parse_int(env, "MAX_ATTEMPTS", 2, minimum=1),
            failure_threshold=_parse_int(
                env, "FAILURE_THRESHOLD", 3, minimum=1),
            cooldown_base_seconds=cooldown_base,
            cooldown_max_seconds=cooldown_max,
            busy_cooldown_seconds=_parse_float(
                env, "BUSY_COOLDOWN_SECONDS", 12.0, minimum=0.0),
            connect_timeout_seconds=_parse_float(
                env, "CONNECT_TIMEOUT_SECONDS", 10.0, minimum=0.1),
            write_timeout_seconds=_parse_float(
                env, "WRITE_TIMEOUT_SECONDS", 30.0, minimum=0.1),
            pool_timeout_seconds=_parse_float(
                env, "POOL_TIMEOUT_SECONDS", 10.0, minimum=0.1),
            catalog_ttl_seconds=_parse_float(
                env, "CATALOG_TTL_SECONDS", 1800.0, minimum=0.0),
            catalog_stale_seconds=_parse_float(
                env, "CATALOG_STALE_SECONDS", 604800.0, minimum=0.0),
            max_request_bytes=_parse_int(
                env, "MAX_REQUEST_BYTES", 2_000_000, minimum=1024),
            max_buffered_response_bytes=_parse_int(
                env, "MAX_BUFFERED_RESPONSE_BYTES", 32_000_000, minimum=1024),
            idempotency_ttl_seconds=_parse_float(
                env, "IDEMPOTENCY_TTL_SECONDS", 900.0, minimum=0.0),
            idempotency_max_entries=_parse_int(
                env, "IDEMPOTENCY_MAX_ENTRIES", 512, minimum=1),
            enable_dev_audit=_parse_bool(env, "ENABLE_DEV_AUDIT", False),
            health_probe_interval_seconds=_parse_float(
                env, "HEALTH_PROBE_INTERVAL_SECONDS", 0.0, minimum=0.0),
            legacy_probe_timeout_seconds=_parse_float(
                env, "LEGACY_PROBE_TIMEOUT_SECONDS", 15.0, minimum=0.1),
            enable_rate_limits=_parse_bool(env, "ENABLE_RATE_LIMITS", True),
            run_rate_limit_per_minute=_parse_int(
                env, "RUN_RATE_LIMIT_PER_MINUTE", 10, minimum=0),
            advanced_rate_limit_per_minute=_parse_int(
                env, "ADVANCED_RATE_LIMIT_PER_MINUTE", 20, minimum=0),
            rate_limit_window_seconds=_parse_float(
                env, "RATE_LIMIT_WINDOW_SECONDS", 60.0, minimum=1.0),
        )
