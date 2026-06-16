# Batch Inference Engine

A production-shaped **batch inference platform**: ingest a batch of AI prompts,
process them **concurrently** against a rate-limited inference provider, **fairly
schedule** across many jobs, handle **HTTP 429** with both *proactive* rate
limiting and *reactive* retry/backoff, and **aggregate** results — with full
observability, idempotent submits, cost accounting, dead-letter recovery, and a
clean cloud deployment path.

Built with **FastAPI + asyncio**. Submitting a batch returns immediately; work
runs in the background on a single **global, fair, bounded worker pool**.

---

## Features

Core (assignment requirements):

- **Batch ingestion** — submit a JSON array of prompts (1,000+ items) via request
  body or file upload; get an immediate `202 Accepted` acknowledgment.
- **Concurrent processing** — one global worker pool drains a shared scheduler in
  parallel, instead of running prompts sequentially.
- **Rate-limit handling** — retries `429 Too Many Requests` with exponential
  backoff + jitter (honoring `Retry-After`), so prompts are never dropped.
- **Bounded concurrency** — worker pool + bounded scheduler + an adaptive global
  limiter cap in-flight inference across all jobs (no unbounded task spawning).
- **Result aggregation** — completions compiled into a paginated JSON result set.
- **Job status API** — poll live progress (`400/1000 completed`, retries, cost).

Platform / cloud-engineering features:

- **Fair multi-tenant scheduler** — weighted round-robin (priority high/normal/low)
  so a 10k-prompt batch can't starve small jobs; nothing is starved.
- **Adaptive rate limiting** — token-bucket global RPS cap + **AIMD** controller
  that shrinks concurrency on 429s and grows it back on success (prevents
  stampedes *before* they happen).
- **Idempotent submits** — `Idempotency-Key` header returns the original job.
- **API backpressure** — `503 + Retry-After` once `MAX_ACTIVE_JOBS` are running.
- **Dead-letter queue + replay** — inspect failed prompts; re-run only those.
- **Cost & token accounting** — per-job and cumulative estimated cost.
- **Observability** — Prometheus `/metrics` (counters/gauges/histograms),
  structured JSON logs with `request_id`/`job_id`/`prompt_id`/`latency_ms`/`cost`.
- **Health model** — `/livez` (alive) + `/readyz` (accepting & not saturated).
- **Graceful shutdown** — drains in-flight prompts, then marks unfinished cancelled.
- **SSE events** — `GET /v1/jobs/{id}/events` streams lifecycle events.
- **Versioned API** (`/v1`) + **non-root Dockerfile** with `HEALTHCHECK`.

---

## Architecture

See **[docs/architecture.md](docs/architecture.md)** for the full diagram, and
**[docs/architecture-decisions.md](docs/architecture-decisions.md)** for the ADRs.
In short:

```
submit ─▶ [ FairScheduler: weighted round-robin per job ] ─▶ global worker pool
                                                                   │
                          token bucket (RPS) + AIMD limiter ───────┤
                                                                   ▼
                          infer_with_retry ──(429)──▶ backoff   provider
                                                                   │
                                  results / dead-letter ──▶ JobStore (live progress)
```

- **Scheduler** fairly interleaves jobs by priority; **one** global pool drains it.
- **Proactive** (token bucket + AIMD) + **reactive** (retry/backoff) rate control.
- Per-prompt failures are isolated to a **dead-letter queue**; the batch continues.
- Single event loop ⇒ shared counters need no locks. `JobStore` seam keeps the
  store swappable (Postgres/Redis in prod).

**Docs:** [architecture](docs/architecture.md) ·
[ADRs](docs/architecture-decisions.md) · [operations / alerts](docs/operations.md) ·
[SLOs](docs/slo.md) · [DigitalOcean deploy](docs/deploy-digitalocean.md)

---

## Quickstart

> Requires **Python 3.11+**.

```bash
# 1. Create a virtualenv and install deps
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Run the API
uvicorn app.main:app --reload --port 8000

# 3. Open interactive docs
#    http://127.0.0.1:8000/docs
```

### Submit a batch (inline JSON)

```bash
curl -X POST http://127.0.0.1:8000/batches \
  -H 'Content-Type: application/json' \
  -d '{"prompts":[{"prompt":"hello"},{"prompt":"world"}]}'
```

Response (`202 Accepted`):

```json
{
  "job_id": "fcf1d2fc03c54b15b2e922ad4c812d8e",
  "state": "pending",
  "total": 2,
  "message": "Batch accepted. Processing in the background.",
  "status_url": "http://127.0.0.1:8000/jobs/fcf1d2fc03c54b15b2e922ad4c812d8e",
  "results_url": "http://127.0.0.1:8000/jobs/fcf1d2fc03c54b15b2e922ad4c812d8e/results"
}
```

### Submit a 1,000-prompt batch by file upload

```bash
python scripts/generate_prompts.py 1000 > data/prompts_1000.json
curl -X POST http://127.0.0.1:8000/batches/upload -F "file=@data/prompts_1000.json"
```

### Track progress

```bash
curl http://127.0.0.1:8000/jobs/<job_id> | jq
```

```json
{
  "job_id": "fcf1d2fc...",
  "state": "running",
  "total": 1000,
  "completed": 590,
  "succeeded": 590,
  "failed": 0,
  "retries": 98,
  "progress": "590/1000",
  "percent": 59.0,
  "duration_seconds": 2.16
}
```

### Fetch aggregated results

```bash
curl http://127.0.0.1:8000/jobs/<job_id>/results | jq
```

### One-shot end-to-end demo

```bash
bash scripts/demo.sh
```

### Run with Docker

```bash
docker build -t batch-inference-engine .
docker run --rm -p 8080:8080 -e WORKER_POOL_SIZE=32 batch-inference-engine
# -> http://127.0.0.1:8080/docs   |   /metrics   |   /healthz
```

### Cloud smoke test

Verify every platform invariant on the live deployment with one command:

```bash
BASE=https://batch-inference-engine-vl6nx.ondigitalocean.app
curl -s -X POST "$BASE/v1/system/self-test?n=50" | jq
```

Returns a structured report — HTTP 200 means all checks green, HTTP 500 means a regression:

```json
{
  "status": "passed",
  "prompts": 50,
  "duration_seconds": 1.84,
  "throughput_rps": 27.2,
  "ack_latency_ms": 3.1,
  "retries": 7,
  "rate_limit_handling": "observed_429s_and_recovered",
  "peak_inflight_observed": 32,
  "concurrency_cap": 64,
  "checks": {
    "immediate_ack": true,
    "all_prompts_aggregated": true,
    "no_prompts_dropped": true,
    "retry_recovery_observed": true,
    "concurrency_cap_respected": true,
    "queue_drained": true,
    "idempotency_roundtrip": true,
    "fair_scheduling": true
  }
}
```

### Observability

```bash
curl http://127.0.0.1:8000/metrics      # Prometheus exposition
# Logs are structured JSON, one object per line:
# {"ts":"...","level":"INFO","logger":"batch_engine","msg":"prompt processed",
#  "job_id":"...","prompt_id":"prompt-1","status":"succeeded","attempts":1,"latency_ms":7.4}
```

---

## API reference

All batch/job routes are available under `/v1` (canonical) and at the root path
(back-compat).

| Method | Path | Description |
|---|---|---|
| `POST` | `/v1/batches` | Submit a JSON batch `{ "prompts": [...], "priority": "high\|normal\|low" }`. Honors `Idempotency-Key`. Returns `202` + `job_id`. |
| `POST` | `/v1/batches/upload?priority=` | Submit a batch as a `.json` file (bare array or `{prompts:[...]}`). |
| `GET`  | `/v1/jobs?state=&limit=&offset=` | List jobs (newest first), optionally filtered by state. |
| `GET`  | `/v1/jobs/{job_id}` | Real-time progress (state, progress, pending, retries, cost). |
| `GET`  | `/v1/jobs/{job_id}/results?limit=&offset=` | Paginated aggregated results. |
| `GET`  | `/v1/jobs/{job_id}/dead-letter?limit=&offset=` | Failed prompts (dead-letter queue). |
| `POST` | `/v1/jobs/{job_id}/replay-failed` | New job from only the failed prompts. |
| `POST` | `/v1/jobs/{job_id}/cancel` | Cancel a running job. |
| `GET`  | `/v1/jobs/{job_id}/events` | Server-Sent Events lifecycle stream. |
| `GET`  | `/v1/system/capacity` | Live capacity (active jobs, queue depth, in-flight, concurrency limit). |
| `POST` | `/v1/system/self-test?n=50` | Built-in invariant smoke test — returns structured pass/fail report. |
| `GET`  | `/metrics` | Prometheus metrics (text exposition format). |
| `GET`  | `/healthz` · `/livez` · `/readyz` | Health: basic · liveness · readiness. |
| `GET`  | `/docs` | Swagger UI. |

Headers: send `Idempotency-Key` on submit for safe retries; every response
carries an `X-Request-ID` (echoed if you provide one). Reusing an
`Idempotency-Key` with a *different* payload returns `409 Conflict`. When the
engine is at capacity (`MAX_ACTIVE_JOBS` or `MAX_QUEUE_SIZE`), submits return
`503` with a `Retry-After` header.

**Auth (optional):** set `API_KEY` to require an `X-API-Key` header on all `/v1`
routes (health and `/metrics` stay open). Unset by default.

---

## Configuration

All settings are environment variables (see [`app/config.py`](app/config.py)):

| Variable | Default | Meaning |
|---|---|---|
| `WORKER_POOL_SIZE` | `16` | Workers per job (per-job in-flight cap). |
| `MAX_QUEUE_SIZE` | `10000` | Bounded queue size (pending-work backpressure). |
| `GLOBAL_MAX_CONCURRENCY` | `64` | Upper bound the adaptive limiter may grow to. |
| `ADAPTIVE_MIN_CONCURRENCY` | `4` | Floor for the AIMD limiter. |
| `ADAPTIVE_INCREASE_AFTER` | `5` | Success streak before additive +1. |
| `ADAPTIVE_DECREASE_FACTOR` | `0.8` | Multiplier applied to the limit on each 429. |
| `PROVIDER_MAX_RPS` | `0` | Token-bucket RPS cap (0 disables proactive limiting). |
| `COST_PER_1K_INPUT_TOKENS` | `0.00015` | Cost estimate, USD per 1K input tokens. |
| `COST_PER_1K_OUTPUT_TOKENS` | `0.0006` | Cost estimate, USD per 1K output tokens. |
| `MAX_ACTIVE_JOBS` | `50` | Active jobs before submit returns `503 + Retry-After`. |
| `OVERLOAD_RETRY_AFTER_SECONDS` | `5` | `Retry-After` value sent when overloaded. |
| `GRACEFUL_SHUTDOWN_SECONDS` | `10` | Drain window for in-flight prompts on shutdown. |
| `API_KEY` | _(unset)_ | If set, require `X-API-Key` on `/v1` routes. |
| `MAX_RETRIES` | `5` | Retry attempts per prompt on 429. |
| `BACKOFF_BASE_SECONDS` | `0.2` | Base for exponential backoff. |
| `BACKOFF_MAX_SECONDS` | `10.0` | Backoff ceiling. |
| `BACKOFF_JITTER` | `0.1` | Max added jitter (seconds). |
| `MOCK_RATE_LIMIT_EVERY` | `7` | Mock endpoint returns 429 every Nth call. |
| `LOG_LEVEL` | `INFO` | Log level for structured JSON logs. |

Example: `WORKER_POOL_SIZE=32 MAX_RETRIES=8 uvicorn app.main:app`

---

## Testing

```bash
pip install -r requirements-dev.txt
pytest -v
```

The suite (55 tests) covers:

- **Backoff math** — exponential growth, capping, jitter bounds.
- **429 retry** — succeeds after N 429s; counts each backoff; honors `Retry-After`;
  exhausts the budget and raises instead of looping forever; **does not** retry
  non-retryable errors.
- **Engine resilience** — periodic 429s **do not fail the batch**; failures are
  isolated; the immediate-ack/background behavior holds.
- **Correctness fixes** — prompts without ids never collide in results; an
  unexpected per-prompt exception does **not** hang the job.
- **Concurrency bound** — peak in-flight calls never exceed `WORKER_POOL_SIZE`,
  and the **global semaphore** caps concurrency across multiple jobs.
- **Backpressure** — `submit` raises (→ `503`) past `MAX_ACTIVE_JOBS` or
  `MAX_QUEUE_SIZE`; recovers after jobs drain. Cancellation marks jobs `cancelled`.
- **Metrics** — counters/histograms increment and render as valid Prometheus text;
  histogram `le` buckets are cumulative (no double-counting).
- **Idempotency** — same key + same payload reuses the job; **different payload
  → `409 Conflict`**.
- **CI guard** — workflow triggers on the active branch.
- **Scheduler** — round-robin fairness, priority weighting, anti-starvation.
- **Rate limiting** — AIMD multiplicative decrease / additive increase; limiter
  blocks beyond its limit; token bucket enforces RPS.
- **Platform** — idempotency, dead-letter + replay, cost accounting, provider
  abstraction, adaptive shrink under throttle.
- **API** — `/v1` prefix, priority, `X-Request-ID`, health model, SSE stream,
  pagination, `/metrics`, 404s, validation errors.
- **Scale** — a 1,000-prompt batch completes successfully.

The retry tests inject a fake `sleep` that *records* delays instead of waiting,
so the back-off logic is verified deterministically and instantly.

---

## CI/CD

[`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs on every push/PR:

1. **test** — `pytest` across Python 3.11/3.12/3.13.
2. **smoke** — boots the API with `uvicorn`, submits a batch, and reads job status
   over HTTP.

---

## Project layout

```
app/
  config.py          # env-driven settings (pools, backoff, AIMD, RPS, cost, limits)
  models.py          # Pydantic models + internal Job/WorkItem + Priority
  mock_inference.py  # mock endpoint that periodically returns HTTP 429
  providers.py       # InferenceProvider protocol + Mock/Slow/Flaky providers
  scheduler.py       # global fair scheduler (weighted round-robin)
  ratelimit.py       # token bucket + AIMD adaptive concurrency limiter
  store.py           # JobStore protocol + InMemoryJobStore
  engine.py          # global worker pool + scheduler + limiters + cost + dead-letter
  metrics.py         # dependency-free Prometheus registry (counters/gauges/histos)
  logging_config.py  # structured JSON logging + request-id contextvar
  main.py            # FastAPI: /v1 router, health model, SSE, middleware
tests/               # backoff, engine, hardening, scheduler, ratelimit, platform, API
scripts/
  generate_prompts.py
  demo.sh
docs/
  architecture.md            # diagram + design rationale
  architecture-decisions.md  # ADRs (the "why")
  operations.md              # runbook + Prometheus alert rules
  slo.md                     # SLOs + error budget
  deploy-digitalocean.md     # App Platform + DOKS deployment guide
Dockerfile             # non-root, healthcheck, honors $PORT
.dockerignore
.do/app.yaml           # DigitalOcean App Platform spec
k8s/                   # DOKS manifests: deployment, service, HPA
.github/workflows/ci.yml
```

---

## Tradeoffs (current scope)

Deliberate simplifications, and what production would change:

| Area | Current | Production direction |
|---|---|---|
| **Durability** | In-memory job store; jobs/results lost on restart. | Postgres for jobs/results, or Redis for fast shared state. |
| **Scale-out** | Single process; job store is per-instance. | Shared store (Redis/Postgres) + a real task queue (Celery/RQ/Arq) so any worker/instance can process and any instance can answer status. |
| **Memory** | Prompt list + results held in RAM; uploads read fully into memory. Bounds *in-flight* work, not total memory. | Streaming/file-backed ingestion; spill results to DB/object storage. |
| **Inference backend** | Mock client behind the `InferFn` interface. | Swap in a real provider (same interface); add per-provider rate-limit config. |
| **AuthN/Z** | None. | API keys / OIDC, per-tenant quotas. |

## Possible next steps

- Persist jobs/results to Postgres (durability across restarts) behind a
  `JobStore` interface; keep `InMemoryJobStore` for local/dev.
- Move to a distributed task queue so workers scale horizontally across pods.
- Add an `HorizontalPodAutoscaler` keyed on `inference_latency_seconds`.
- Result streaming (NDJSON) for very large batches.
