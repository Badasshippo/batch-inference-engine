# Batch Inference Engine

A backend REST service that ingests a batch of AI prompts, processes them
**concurrently** against a mock rate-limited inference endpoint, transparently
handles **HTTP 429** with retry/backoff, and **aggregates** the results — while
exposing a **real-time job-status API**.

Built with **FastAPI + asyncio**. Submitting a batch returns immediately; the
work runs in the background on a **bounded worker pool**.

---

## Features

- **Batch ingestion** — submit a JSON array of prompts (1,000+ items) via request
  body or file upload; get an immediate `202 Accepted` acknowledgment.
- **Concurrent processing** — a bounded pool of `N` async workers drains a shared
  queue, instead of running prompts sequentially.
- **Rate-limit handling** — workers retry `429 Too Many Requests` with exponential
  backoff + jitter (honoring `Retry-After`), so prompts are never dropped.
- **Bounded concurrency** — fixed worker count **and** a bounded queue keep memory
  flat and prevent unbounded task spawning.
- **Result aggregation** — successful completions are compiled into a JSON result
  set, queryable per job.
- **Job status API** — poll live progress (e.g. `400/1000 completed`, retries,
  duration).

---

## Architecture

See **[docs/architecture.md](docs/architecture.md)** for the full diagram and
rationale. In short:

```
prompts ─▶ [ bounded asyncio.Queue ] ─▶ N worker coroutines ─▶ results
                                              │
                                              ▼
                                   infer_with_retry()  ──(429)──▶ backoff + retry
                                              │
                                              ▼
                                    in-memory job store (live progress)
```

- **Producer** enqueues prompts into a bounded queue (backpressure).
- **Worker pool** (`WORKER_POOL_SIZE` coroutines) is the hard cap on in-flight calls.
- **`infer_with_retry`** backs off on 429s; exhausted/non-retryable items are marked
  failed without crashing the batch.
- Single event loop ⇒ shared counters need no locks.

---

## Quickstart

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

---

## API reference

| Method | Path | Description |
|---|---|---|
| `POST` | `/batches` | Submit a JSON batch `{ "prompts": [{ "id"?, "prompt" }] }`. Returns `202` + `job_id`. |
| `POST` | `/batches/upload` | Submit a batch as a `.json` file (bare array or `{prompts:[...]}`). |
| `GET`  | `/jobs/{job_id}` | Real-time progress for a job. |
| `GET`  | `/jobs/{job_id}/results` | Aggregated results (partial while running). |
| `GET`  | `/healthz` | Liveness probe. |
| `GET`  | `/docs` | Swagger UI. |

---

## Configuration

All settings are environment variables (see [`app/config.py`](app/config.py)):

| Variable | Default | Meaning |
|---|---|---|
| `WORKER_POOL_SIZE` | `16` | Number of concurrent workers (in-flight call cap). |
| `MAX_QUEUE_SIZE` | `10000` | Bounded queue size (backpressure / memory cap). |
| `MAX_RETRIES` | `5` | Retry attempts per prompt on 429. |
| `BACKOFF_BASE_SECONDS` | `0.2` | Base for exponential backoff. |
| `BACKOFF_MAX_SECONDS` | `10.0` | Backoff ceiling. |
| `BACKOFF_JITTER` | `0.1` | Max added jitter (seconds). |
| `MOCK_RATE_LIMIT_EVERY` | `7` | Mock endpoint returns 429 every Nth call. |

Example: `WORKER_POOL_SIZE=32 MAX_RETRIES=8 uvicorn app.main:app`

---

## Testing

```bash
pip install -r requirements-dev.txt
pytest -v
```

The suite (17 tests) covers:

- **Backoff math** — exponential growth, capping, jitter bounds.
- **429 retry** — succeeds after N 429s; counts each backoff; honors `Retry-After`;
  exhausts the budget and raises instead of looping forever; **does not** retry
  non-retryable errors.
- **Engine resilience** — periodic 429s **do not fail the batch**; failures are
  isolated; the immediate-ack/background behavior holds.
- **Concurrency bound** — peak in-flight calls never exceed `WORKER_POOL_SIZE`.
- **Scale** — a 1,000-prompt batch completes successfully.
- **API** — submit, upload, poll-to-completion, 404s, validation errors.

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
  config.py          # env-driven settings (pool size, backoff, mock cadence)
  models.py          # Pydantic request/response models + internal Job record
  mock_inference.py  # mock endpoint that periodically returns HTTP 429
  engine.py          # bounded worker pool + infer_with_retry (backoff) + job store
  main.py            # FastAPI endpoints
tests/               # backoff, engine, and API tests
scripts/
  generate_prompts.py
  demo.sh
docs/architecture.md # architecture diagram + design rationale
.github/workflows/ci.yml
```

---

## Possible next steps

- Persist jobs/results to a database (SQLite/Postgres) for durability across restarts.
- Replace the mock client with a real provider behind the same `InferFn` interface.
- Add a per-job cancel endpoint and result pagination/streaming.
- Move the job store to Redis to scale workers across processes.
