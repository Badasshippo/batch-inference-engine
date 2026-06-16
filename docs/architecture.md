# Architecture

## System flow

```mermaid
flowchart TD
    subgraph Client
        C1["POST /batches&nbsp;&nbsp;(JSON array)"]
        C2["POST /batches/upload&nbsp;&nbsp;(.json file)"]
        C3["GET /jobs/{id}&nbsp;&nbsp;(progress)"]
        C4["GET /jobs/{id}/results"]
    end

    subgraph API["FastAPI app (app/main.py)"]
        H["Validate payload<br/>(Pydantic models)"]
        ACK["Create Job + return 202 ACK<br/>immediately"]
        STAT["Read live counters<br/>from Job store"]
    end

    subgraph Engine["BatchEngine (app/engine.py)"]
        Q[["Bounded asyncio.Queue<br/>(backpressure)"]]
        P["Producer<br/>enqueues prompts"]
        subgraph Pool["Bounded worker pool (N coroutines)"]
            W1["worker 1"]
            W2["worker 2"]
            WN["worker N"]
        end
        AGG["Aggregate results<br/>+ update counters"]
        STORE[("In-memory Job store<br/>state / progress / results")]
    end

    subgraph Retry["infer_with_retry()"]
        R{"429?"}
        BO["backoff = min(base*2^n, max) + jitter<br/>(honors Retry-After)"]
        OK["return output"]
        FAIL["exhaust retries -> mark item failed<br/>(batch keeps going)"]
    end

    MOCK["MockInferenceClient<br/>periodic HTTP 429"]

    C1 --> H
    C2 --> H
    H --> ACK
    ACK -->|"background task"| P
    P --> Q
    Q --> W1 & W2 & WN
    W1 & W2 & WN --> R
    R -->|yes| BO --> R
    R -->|no| OK
    R -.->|budget exhausted| FAIL
    BO -.-> MOCK
    OK -.-> MOCK
    W1 & W2 & WN --> AGG --> STORE
    C3 --> STAT --> STORE
    C4 --> STORE
```

## Why this design

| Requirement | How it is met |
|---|---|
| **Immediate acknowledgment** | `submit()` registers the `Job` and schedules an `asyncio` background task, then returns `202 Accepted` with `job_id` + status URLs. The request never blocks on processing. |
| **Concurrent processing** | A fixed set of `WORKER_POOL_SIZE` worker coroutines drain a shared queue, so prompts are processed in parallel instead of sequentially. |
| **Bounded concurrency** | Concurrency is capped two ways: a fixed number of workers (in-flight calls) and a `maxsize`-bounded queue (pending work). Memory stays flat even for huge batches. |
| **Rate-limit handling** | `infer_with_retry()` catches `RateLimitError` (429) and sleeps using exponential backoff + jitter, honoring a `Retry-After` hint, up to `MAX_RETRIES`. Prompts are never dropped on a transient 429. |
| **Resilience** | A prompt that exhausts retries (or hits a non-retryable 500) is recorded as a failed item; the worker and the rest of the batch continue. |
| **Result aggregation** | Each worker writes its `InferenceResult` into the job's result map and increments counters; `/jobs/{id}/results` returns the compiled JSON. |
| **Job status API** | Workers update `completed/succeeded/failed/retries` atomically (single-threaded event loop), so `/jobs/{id}` reports real-time progress like `400/1000`. |

## Concurrency model details

The engine uses a **producer / bounded-queue / worker-pool** pattern on a single
asyncio event loop:

1. **Producer** iterates the batch and `await queue.put(item)`. Because the queue
   is bounded, `put` suspends when full — this is backpressure that keeps memory
   bounded regardless of batch size.
2. **Workers** (`N = WORKER_POOL_SIZE`) loop on `await queue.get()`, process one
   prompt at a time, and call `queue.task_done()`. The pool size is the hard cap
   on simultaneous inference calls.
3. **Completion** is detected with `await queue.join()` (all items processed),
   after which the workers are cancelled and the job is marked `completed`.

Because everything runs on one event loop, the shared counters and result map
need no locks — there is no preemption between `await` points where we mutate
them. The only lock is inside the mock client, used to make its 429 cadence
deterministic across concurrent callers.
