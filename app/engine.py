"""The batch inference engine.

Concurrency model
-----------------
Each submitted batch becomes a `Job`. Processing uses a classic bounded
producer/worker-pool pattern built on asyncio primitives:

    prompts --> [bounded asyncio.Queue] --> N worker coroutines --> results

* A single producer enqueues prompts into a *bounded* queue. Because the queue
  has a max size, enqueueing applies backpressure and memory stays bounded even
  for very large batches (e.g. 1,000+ prompts).
* Exactly `worker_pool_size` worker coroutines drain the queue. This caps the
  number of in-flight inference calls, so we never spawn unbounded tasks.
* Each worker runs `infer_with_retry`, which handles HTTP 429 responses with
  exponential backoff + jitter. A prompt that exhausts its retries is recorded
  as a failed item but never crashes the worker or the batch.

This module is deliberately framework-agnostic (no FastAPI imports) so it can be
unit tested in isolation.
"""
from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable, Sequence

from .config import Settings, get_settings
from .mock_inference import InferenceError, MockInferenceClient, RateLimitError
from .models import InferenceResult, Job, JobState, PromptItem

# A coroutine function that maps a prompt string to a completion string.
InferFn = Callable[[str], Awaitable[str]]
SleepFn = Callable[[float], Awaitable[None]]


def compute_backoff(attempt: int, settings: Settings, rng: random.Random | None = None) -> float:
    """Exponential backoff with full jitter for a given (1-based) attempt.

    delay = min(base * 2**(attempt-1), max) + jitter
    """
    rng = rng or random
    raw = settings.backoff_base_seconds * (2 ** (attempt - 1))
    capped = min(raw, settings.backoff_max_seconds)
    jitter = rng.uniform(0, settings.backoff_jitter)
    return capped + jitter


async def infer_with_retry(
    infer: InferFn,
    prompt: str,
    settings: Settings,
    *,
    sleep: SleepFn = asyncio.sleep,
    rng: random.Random | None = None,
    on_retry: Callable[[int, float], None] | None = None,
) -> tuple[str, int]:
    """Call `infer(prompt)`, retrying HTTP 429s with backoff.

    Returns a tuple of (output, attempts). Raises the last error if all retries
    are exhausted or the error is non-retryable.

    `sleep` and `rng` are injectable so tests can run instantly and
    deterministically. `on_retry(attempt, delay)` is invoked before each backoff
    sleep, which the engine uses to count retries for live progress reporting.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            output = await infer(prompt)
            return output, attempt
        except RateLimitError as exc:
            # Retryable. Back off unless we've exhausted our budget.
            if attempt > settings.max_retries:
                raise
            # Honor a server-provided Retry-After hint if larger than our backoff.
            delay = max(compute_backoff(attempt, settings, rng), exc.retry_after)
            if on_retry is not None:
                on_retry(attempt, delay)
            await sleep(delay)
        except InferenceError:
            # Non-retryable upstream failure: surface immediately.
            raise


class BatchEngine:
    """Owns the in-memory job store and runs batches on a bounded worker pool."""

    def __init__(
        self,
        infer: InferFn | None = None,
        settings: Settings | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        # Default to the mock client; injectable for tests / real backends.
        self._infer: InferFn = infer or MockInferenceClient(
            rate_limit_every=self.settings.mock_rate_limit_every,
            min_latency_ms=self.settings.mock_min_latency_ms,
            max_latency_ms=self.settings.mock_max_latency_ms,
        ).infer
        self._jobs: dict[str, Job] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()

    # ----------------------------- public API ----------------------------- #
    async def submit(self, prompts: Sequence[PromptItem]) -> Job:
        """Register a job and kick off background processing immediately."""
        job = Job(total=len(prompts))
        async with self._lock:
            self._jobs[job.id] = job
        # Fire-and-forget: processing continues after we return the ack.
        task = asyncio.create_task(self._run_job(job, list(prompts)))
        self._tasks[job.id] = task
        task.add_done_callback(lambda t: self._tasks.pop(job.id, None))
        return job

    def get_job(self, job_id: str) -> Job | None:
        return self._jobs.get(job_id)

    async def shutdown(self) -> None:
        """Cancel any in-flight jobs (used on app shutdown)."""
        tasks = list(self._tasks.values())
        for t in tasks:
            t.cancel()
        for t in tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass

    # --------------------------- internal logic --------------------------- #
    async def _run_job(self, job: Job, prompts: list[PromptItem]) -> None:
        job.state = JobState.RUNNING
        job.started_at = time.time()

        queue: asyncio.Queue[PromptItem] = asyncio.Queue(maxsize=self.settings.max_queue_size)

        async def producer() -> None:
            for item in prompts:
                await queue.put(item)  # blocks when full -> backpressure

        async def worker() -> None:
            while True:
                try:
                    item = await queue.get()
                except asyncio.CancelledError:
                    raise
                try:
                    await self._process_item(job, item)
                finally:
                    queue.task_done()

        # Start the bounded worker pool.
        n_workers = max(1, self.settings.worker_pool_size)
        workers = [asyncio.create_task(worker()) for _ in range(n_workers)]
        producer_task = asyncio.create_task(producer())

        try:
            await producer_task
            await queue.join()  # wait until every prompt is processed
            job.state = JobState.COMPLETED
        except asyncio.CancelledError:
            job.state = JobState.FAILED
            raise
        finally:
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            job.finished_at = time.time()

    async def _process_item(self, job: Job, item: PromptItem) -> None:
        prompt_id = item.id or f"prompt-{job.completed + 1}"

        def _count_retry(_attempt: int, _delay: float) -> None:
            job.retries += 1

        try:
            output, attempts = await infer_with_retry(
                self._infer,
                item.prompt,
                self.settings,
                on_retry=_count_retry,
            )
            result = InferenceResult(
                id=prompt_id,
                prompt=item.prompt,
                success=True,
                output=output,
                attempts=attempts,
            )
            job.succeeded += 1
        except (RateLimitError, InferenceError) as exc:
            result = InferenceResult(
                id=prompt_id,
                prompt=item.prompt,
                success=False,
                error=str(exc),
            )
            job.failed += 1
        finally:
            job.completed += 1

        job.results[prompt_id] = result
