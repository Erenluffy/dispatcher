# CHIPVERSITY Verilog dispatcher

One public API in front of eight identical Verilog worker services. The
frontend keeps calling a single URL; the dispatcher picks a live worker, caps
how many simulations each worker runs at once, and pins stateful files to the
worker that created them.

```
browser ──► dispatcher (this service)
                │  least-loaded warm worker, bounded concurrency
                ├──► worker 1  ─┐
                ├──► worker 2   │  identical backend image:
                ├──► ...        │  FastAPI + iverilog + Verilator + Yosys
                └──► worker 8  ─┘
```

The worker API contract is unchanged. `/api/run`, `/api/submit`,
`/api/run-advanced`, `/api/lint`, `/api/synthesize` and the other existing
routes are proxied with their original status codes and JSON bodies, apart from
the worker tag added to waveform and share IDs (see below).

## Layout

| File | Purpose |
| --- | --- |
| `app.py` | Routes, forwarding/failover, affinity resolution, synthetic health |
| `workers.py` | Worker pool, queue, per-worker limits, circuit breaker |
| `proxy.py` | URL/header hygiene, upstream send, buffered reads |
| `affinity.py` | Worker-tagged waveform/share IDs and payload rewriting |
| `cache.py` | TTL cache for problem catalogs with stale-if-error |
| `idempotency.py` | Per-client key records, in-flight coalescing, replay, ambiguous outcomes |
| `ratelimit.py` | Shared per-IP sliding-window limits |
| `config.py` | Environment parsing and per-route timeout profiles |
| `tests/` | pytest suite with an in-process fleet (`mock_fleet.py`) |

## Run locally

The worker refuses to start without `DISPATCHER_AUTH_TOKEN` unless you
explicitly opt out for local development. Run the two services in separate
terminals:

```bash
python -m venv .venv
.venv/Scripts/pip install -r dispatcher/requirements-dev.txt   # Windows
# .venv/bin/pip install -r dispatcher/requirements-dev.txt     # macOS/Linux

# Terminal 1 — worker on :8000
DISPATCHER_AUTH_TOKEN="local-dev-token-change-me" \
  .venv/Scripts/python -m uvicorn "app:app" \
  --app-dir "backend verilog problems" --port 8000 --reload

# Terminal 2 — dispatcher on :8080
WORKER_URLS="http://localhost:8000" \
  WORKER_AUTH_TOKEN="local-dev-token-change-me" \
  .venv/Scripts/python -m uvicorn dispatcher.app:app --reload
```

For a local worker you may instead start it with
`ALLOW_UNAUTHENTICATED_WORKER=true`; the dispatcher still needs a non-empty
`WORKER_AUTH_TOKEN`, which the worker then ignores. Never set the bypass on a
public deployment.

Run the tests from the repository root:

```bash
.venv/Scripts/python -m pytest dispatcher
```

## Deploy on Render

1. **Generate one shared secret.** Run `openssl rand -hex 32`. This value is
   the dispatcher's `WORKER_AUTH_TOKEN` and every worker's
   `DISPATCHER_AUTH_TOKEN`; they must match exactly.
2. **Create the workers.** Deploy `backend verilog problems/render.yaml` from
   the Render Blueprint page. It defines eight Free Docker services named
   `chipversity-worker-1` … `chipversity-worker-8`, with no persistent disk.
   Render prompts for `DISPATCHER_AUTH_TOKEN`; enter the value from step 1.
   Wait until all eight are live.
3. **Collect the eight URLs.** They look like
   `https://chipversity-worker-1.onrender.com`.
4. **Create the dispatcher.** Deploy `dispatcher/render.yaml` (root directory
   `dispatcher`). Render prompts for the `sync: false` environment variables.
5. **Set `WORKER_URLS`** to the eight URLs, comma-separated, and set
   `WORKER_AUTH_TOKEN` to the same secret from step 1. `WORKER_TAGS` is
   optional: defaults are derived from each URL, so they remain stable when
   URLs are reordered or a worker is added. Old waveform/share links keep
   working across a reorder; changing a worker's URL or its tag invalidates
   the links that point at it.
6. **Deploy the dispatcher**, then open
   `https://<dispatcher>.onrender.com/dispatcher/health`. `"status": "ok"`
   with `"workers_configured": 8` means the fleet is configured. See
   “Reading health” below for what the worker counts do and do not mean.
7. **Point the frontend at it.** Set `DISPATCHER_URL` in
   `assets/js/api-config.js` and redeploy the static site.

Health checks never wake workers. `/api/health`, `/api/engines` and
`/dispatcher/health` are answered by the dispatcher itself. Catalogs are cached
for 30 minutes and served stale for up to 7 days if every worker is cold or
down, so lesson pages keep working.

### Reading health

`/dispatcher/health` is **observational**: it reports what this dispatcher
process has seen, not the result of a background poll. A worker counted as
`unknown` has simply not served traffic since the dispatcher started; it may be
asleep, starting, or broken. `cold` means it answered before but has been idle
past the warm window. Top-level counts distinguish:

- `workers_configured` — entries in `WORKER_URLS`;
- `workers_recently_responsive` — observed a success inside the warm window;
- `workers_cold` — previously responsive, now outside the warm window;
- `workers_unknown` — no successful request observed yet;
- `workers_circuit_open` — breaker open after failures;
- `workers_cooling_down` — temporarily skipped after a busy response.

Add `?probe=1` to actively call every worker's `/api/health`. Probes can wake
sleeping Free instances and consume instance hours, so they are never automatic.

### Required environment

| Variable | Default | Notes |
| --- | --- | --- |
| `WORKER_URLS` | — (required) | Comma-separated origins, no paths/query |
| `WORKER_AUTH_TOKEN` | — (required) | Sent as `X-Dispatcher-Token`; must match every worker's `DISPATCHER_AUTH_TOKEN` |
| `WORKER_TAGS` | URL-derived | Optional one 5-hex tag per URL; stable across URL reordering |
| `ENABLE_DEV_AUDIT` | `false` | Leave off: it runs every problem for minutes |
| `MAX_COLD_WORKERS` | `2` | How many sleeping workers may be woken at once |
| `WORKER_MAX_INFLIGHT` | `3` | General requests per worker |
| `HEAVY_MAX_INFLIGHT` | `1` | Lint/synthesis per worker |
| `VERILATOR_MAX_INFLIGHT` | `1` | Verilator builds per worker |
| `QUEUE_TIMEOUT_SECONDS` | `60` | Max wait for a free worker slot |
| `COLD_START_BUDGET_SECONDS` | `90` | Connect allowance for a sleeping worker |
| `CATALOG_TTL_SECONDS` | `1800` | Fresh catalog lifetime |
| `CATALOG_STALE_SECONDS` | `604800` | How long stale catalogs may be served |
| `IDEMPOTENCY_TTL_SECONDS` | `900` | How long completed/ambiguous key records replay |
| `IDEMPOTENCY_MAX_ENTRIES` | `512` | Bounded in-process idempotency records |
| `RUN_RATE_LIMIT_PER_MINUTE` | `10` | Shared per-IP limit for run/submit/synthesize |
| `ADVANCED_RATE_LIMIT_PER_MINUTE` | `20` | Shared per-IP limit for advanced tools |

All limits are enforced by the dispatcher, so they are fleet-wide rather than
per worker. The workers keep their own protections as a second layer, and they
reject any request without the shared dispatcher token (except
`/api/health`), so a caller cannot bypass the dispatcher by discovering a
worker URL.

### Queue, cold starts and deadlines

Each route profile is the worst case of **queue + cold start + worker read**
plus a 30-second framing margin. The current defaults are:

| Route | Queue | Cold wake | Read | Total |
| --- | --- | --- | --- | --- |
| Run / submit | 60s | 90s | 150s | 330s |
| Advanced run | 60s | 90s | 330s | 510s |
| Lint / synthesis | 60s | 90s | 150s / 180s | 330s / 360s |
| Catalog / state read | 60s | 90s | 90s / 120s | 270s / 300s |

The 90-second cold budget is a conservative assumption for Render Free, not a
measurement of your services. Real wake times vary with the region, image size
and platform load. Measure the first request after a quiet period, then adjust
`COLD_START_BUDGET_SECONDS` and `QUEUE_TIMEOUT_SECONDS` together: if a request
can wait 60 seconds for a slot and then spends 90 seconds waking, the read
budget must still fit inside the route total. The test
`test_route_budgets_leave_room_for_a_cold_start` fails if a future profile edit
breaks that relationship.

## Stateful data and worker affinity

Waveform VCD files and shared projects live on a worker's local disk. When a
worker creates one, the dispatcher prefixes its ID with that worker's tag:

| Data | Worker ID | Public ID |
| --- | --- | --- |
| Waveform | 36-char UUID | 41 chars: 5-hex tag + UUID |
| Share | 10 hex chars | 15 chars: 5-hex tag + 10 hex |

Later reads are routed straight back to the owner. The dispatcher rewrites
`waveform_id`, `waveform_url`, `waveform_download_url`, the viewer HTML, and
share `id`/`url` fields; everything else passes through untouched.

Default tags are the first five hex characters of SHA-256 of the worker's
normalised URL. Reordering `WORKER_URLS` or adding a worker therefore keeps
existing links working. Changing a worker's URL, or pinning a new
`WORKER_TAGS` value, changes its tag and makes old links to that worker
unresolvable (the dispatcher returns a clean 404). Explicit `WORKER_TAGS` may
be used to pin identities across a URL change, but the old worker's files must
still exist for those links to work.

IDs created before the dispatcher existed (untagged) are supported by probing
all workers once and caching the owner for an hour. A 404 means the worker was
redeployed or its free instance restarted and the ephemeral file is gone.

## Behaviour worth knowing

- **Safe retries only.** `GET`, `HEAD` and `OPTIONS` may retry transport
  failures, read timeouts and 502/504 responses on another worker. A
  non-idempotent method (`POST`, `PUT`, `PATCH`, `DELETE`) is retried only when
  the connection was never established (connect error, connect timeout or pool
  timeout) or the worker explicitly answered 503 “busy” before running the
  job. A read timeout, a connection reset after send, or a generic 502/504 on a
  compute POST is returned to the caller and never replayed automatically.
- **Idempotency keys.** The simulators send an `Idempotency-Key` header on
  compute and share requests. The dispatcher coalesces concurrent duplicates,
  replays a completed result for the same key for 15 minutes, rejects reuse of
  a key with a different body (409), and records an ambiguous outcome (502) so
  a retry with that key cannot silently execute the job twice. To deliberately
  re-run after an ambiguous failure, use a new key. Records are in-process and
  bounded; restarting the dispatcher clears them.
- **Circuit breaker.** Three consecutive failures open a worker's circuit for
  30s, doubling up to 5 minutes. A half-open trial closes it again on any real
  response. A worker that answers 503 “busy” gets a short cooldown without
  opening its circuit.
- **Streaming.** Waveform data and downloads stream through without a full
  buffer. Other responses are buffered and capped at 32 MB.
- **Proxy hygiene.** Hop-by-hop headers, upstream CORS headers, client
  `X-Forwarded-*` values and any inbound `X-Dispatcher-Token` are stripped; the
  dispatcher is the only CORS owner and the only source of the worker token.
  The caller IP is read from the first `X-Forwarded-For` entry, which Render
  sets to the real client address; trailing caller-supplied entries are ignored
  so they cannot dodge the rate limiter. Uvicorn proxy-header handling is
  intentionally disabled to keep this resolution explicit and testable.
- **Single process.** Do not scale the dispatcher to multiple instances or
  uvicorn workers: the pool, queue, rate limits, cache and idempotency records
  are in-process.

## Free-tier limits (read this)

Render's Free plan gives a workspace **750 instance hours per month**, shared by
every Free service. Eight workers plus the dispatcher are nine services; if
traffic keeps them awake, the monthly allowance is exhausted long before the
month ends, and Render suspends Free services until the quota resets. The
dispatcher deliberately wakes at most two cold workers at a time and lets idle
services sleep, but it cannot make nine Free services cheap.

Practical guidance:

- For light traffic, start with two or three worker URLs in `WORKER_URLS`; the
  dispatcher works with any number and can grow to eight later.
- Free services sleep after ~15 minutes idle and take roughly 30–90 seconds to
  wake. The profiles budget for a 90-second wake, but you should still measure
  your own first-request time and tune `COLD_START_BUDGET_SECONDS` and
  `QUEUE_TIMEOUT_SECONDS`. The dispatcher is a Free service too: if it has
  slept, Render wakes it before the app sees the request, so that delay is
  additional to the route budgets below.
- There is no persistent disk on Free. Waveforms and shares survive while a
  worker stays up; they are lost on redeploy, restart or spin-down. The
  dispatcher handles the resulting 404 cleanly, but it cannot recover a file.
  Treat shares as temporary. If shared projects or waveforms must survive
  worker restarts, move them to external storage (S3/R2/Bucket or a small
  database with object storage); running on a paid instance with a persistent
  disk is the alternative.
- Move workers to a paid instance type before relying on the site for anything
  important. The 512 MB Free instance is also the reason Verilator builds are
  capped at one per worker.

## Troubleshooting

| Symptom | Meaning |
| --- | --- |
| `503 No simulation worker is currently available` | Every worker is open/cooling down or the queue timed out; retry after `Retry-After` |
| `502 The simulation worker is unavailable` | Connections to workers failed; check the worker services |
| `404 Waveform not found or expired` | Worker restarted, or the ID's tag is no longer configured |
| `429 Rate limit exceeded` | Shared per-IP limit; check `Retry-After` |
| `409 A request with this Idempotency-Key is still running` | The first duplicate is still in flight; retry after `Retry-After`, or wait for its response |
| `409 Idempotency-Key was reused with a different request body` | Same key, different method/path/body; use a new key for a new request |
| `502`/`504` with `x-dispatcher-idempotency: ambiguous` | The worker may have started the job but its result is unknown. The same key replays this error; use a new key only if you intend to re-run |
| `401 Unauthorized worker request` (in worker logs) | Dispatcher and worker tokens differ; `WORKER_AUTH_TOKEN` must equal that worker's `DISPATCHER_AUTH_TOKEN` |
| `/dispatcher/health` shows `"degraded"` | All circuits are open; inspect `workers[].last_error` |
| `503 Dispatcher is not configured` | `WORKER_URLS` or `WORKER_AUTH_TOKEN` is missing or invalid; see the service logs |
