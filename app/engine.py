"""The batch inference engine.

Concurrency model
-----------------
Each submitted batch becomes a `Job`. Processing uses a classic bounded
producer/worker-pool pattern built on asyncio primitives:

    prompts --> [bounded asyncio.Queue] --> N worker coroutines --> results

* A single producer enqueues work into a *bounded* queue. Because the queue has
  a max size, enqueueing applies backpressure so pending work cannot grow without
  limit. (Note: this bounds *in-flight* work, not total process memory -- the
  prompt list itself is held in RAM. See README "Tradeoffs".)
* Exactly `worker_pool_size` worker coroutines drain the queue per job. On top of
  that, a process-wide semaphore (`global_max_concurrency`) caps the number of
  inference calls in flight across *all* jobs, so concurrent batches cannot
  multiply concurrency and exhaust the upstream / memory.
* Each worker runs `infer_with_retry`, which handles HTTP 429 with exponential
  backoff + jitter. Any per-prompt failure (exhausted retries, non-retryable
  error, or an unexpected exception) is recorded as a failed item and never
  crashes the worker or the batch.

This module is deliberately framework-agnostic (no FastAPI imports) so it can be
unit tested in isolation.
"""
from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable, Sequence

from .config import Settings, get_settings
from .logging_config import get_logger
from .metrics import metrics
from .mock_inference import InferenceError, MockInferenceClient, RateLimitError
from .models import InferenceResult, Job, JobState, PromptItem, WorkItem

# A coroutine function that maps a prompt string to a completion string.
InferFn = Callable[[str], Awaitable[str]]
SleepFn = Callable[[float], Awaitable[None]]

log = get_logger()


class OverloadedError(Exception):
    """Raised when the service refuses a batch due to backpressure limits."""

    def __init__(self, retry_after: int, message: str = "Service overloaded") -> None:
        super().__init__(message)
        self.retry_after = retry_after


def compute_backoff(attempt: int, settings: Settings, rng: random.Random | None = None) -> float:
    """Exponential backoff with jitter for a given (1-based) attempt.

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
        self._accepting = True
        # Process-wide cap on concurrent inference calls across all jobs.
        self._global_sem = asyncio.Semaphore(self.settings.global_max_concurrency)

    # ----------------------------- public API ----------------------------- #
    @property
    def active_jobs(self) -> int:
        return len(self._tasks)

    async def submit(self, prompts: Sequence[PromptItem]) -> Job:
        """Register a job and kick off background processing immediately.

        Raises OverloadedError (-> HTTP 503) if the service is shutting down or
        already running its maximum number of concurrent jobs.
        """
        if not self._accepting:
            raise OverloadedError(
                self.settings.overload_retry_after_seconds, "Service is shutting down"
            )
        if self.active_jobs >= self.settings.max_active_jobs:
            metrics.jobs_rejected.inc()
            raise OverloadedError(self.settings.overload_retry_after_seconds)

        metrics.jobs_submitted.inc()
        job = Job(total=len(prompts))
        # Assign stable, collision-free identities *before* processing starts.
        items = [
            WorkItem(seq=i, id=(p.id or f"prompt-{i + 1}"), prompt=p.prompt)
            for i, p in enumerate(prompts)
        ]
        async with self._lock:
            self._jobs[job.id] = job

        task = asyncio.create_task(self._run_job(job, items))
        self._tasks[job.id] = task
        task.add_done_callback(lambda t: self._tasks.pop(job.id, None))
        log.info("job submitted", extra={"job_id": job.id, "total": job.total})
        return job

    def get_job(self, job_id: str) -> Job | None:
        return self._jobs.get(job_id)

    async def cancel(self, job_id: str) -> bool | None:
        """Cancel a running job. Returns True if cancelled, False if already
        finished, or None if the job is unknown."""
        job = self._jobs.get(job_id)
        if job is None:
            return None
        if job.state in (JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED):
            return False
        job.state = JobState.CANCELLED
        task = self._tasks.get(job_id)
        if task is not None:
            task.cancel()
        log.info("job cancellation requested", extra={"job_id": job_id})
        return True

    async def shutdown(self) -> None:
        """Graceful shutdown: stop accepting new jobs, let in-flight prompts
        finish for up to `graceful_shutdown_seconds`, then cancel the rest."""
        self._accepting = False
        tasks = list(self._tasks.values())
        if not tasks:
            return
        log.info(
            "graceful shutdown started",
            extra={"active_jobs": len(tasks), "grace_s": self.settings.graceful_shutdown_seconds},
        )
        done, pending = await asyncio.wait(
            tasks, timeout=self.settings.graceful_shutdown_seconds
        )
        for t in pending:
            t.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        log.info(
            "graceful shutdown complete",
            extra={"finished": len(done), "force_cancelled": len(pending)},
        )

    # --------------------------- internal logic --------------------------- #
    async def _run_job(self, job: Job, items: list[WorkItem]) -> None:
        job.state = JobState.RUNNING
        job.started_at = time.time()

        queue: asyncio.Queue[WorkItem] = asyncio.Queue(maxsize=self.settings.max_queue_size)

        async def producer() -> None:
            for item in items:
                await queue.put(item)  # blocks when full -> backpressure

        async def worker() -> None:
            while True:
                item = await queue.get()
                try:
                    await self._process_item(job, item)
                finally:
                    queue.task_done()

        n_workers = max(1, self.settings.worker_pool_size)
        workers = [asyncio.create_task(worker()) for _ in range(n_workers)]
        producer_task = asyncio.create_task(producer())

        try:
            await producer_task
            await queue.join()  # wait until every prompt is processed
            job.state = JobState.COMPLETED
            metrics.jobs_completed.inc()
        except asyncio.CancelledError:
            # Distinguish an intentional cancel from an unexpected failure.
            if job.state != JobState.CANCELLED:
                job.state = JobState.FAILED
            else:
                metrics.jobs_cancelled.inc()
            raise
        finally:
            producer_task.cancel()
            for w in workers:
                w.cancel()
            await asyncio.gather(producer_task, *workers, return_exceptions=True)
            job.finished_at = time.time()
            if job.started_at is not None:
                metrics.job_duration.observe(job.finished_at - job.started_at)
            log.info(
                "job finished",
                extra={
                    "job_id": job.id,
                    "state": job.state.value,
                    "succeeded": job.succeeded,
                    "failed": job.failed,
                    "retries": job.retries,
                },
            )

    async def _process_item(self, job: Job, item: WorkItem) -> None:
        start = time.perf_counter()

        def _on_retry(_attempt: int, _delay: float) -> None:
            job.retries += 1
            metrics.inference_retries.inc()
            metrics.inference_rate_limited.inc()

        status = "succeeded"
        try:
            # The global semaphore caps inference concurrency across all jobs.
            async with self._global_sem:
                output, attempts = await infer_with_retry(
                    self._infer,
                    item.prompt,
                    self.settings,
                    on_retry=_on_retry,
                )
            result = InferenceResult(
                id=item.id, prompt=item.prompt, success=True, output=output, attempts=attempts
            )
            job.succeeded += 1
            metrics.prompts_succeeded.inc()
        except asyncio.CancelledError:
            # Job is being cancelled; propagate so the worker can stop.
            raise
        except Exception as exc:  # noqa: BLE001 - isolate any per-prompt failure
            status = "failed"
            result = InferenceResult(
                id=item.id, prompt=item.prompt, success=False, error=str(exc)
            )
            job.failed += 1
            metrics.prompts_failed.inc()
        finally:
            job.completed += 1
            metrics.prompts_completed.inc()

        latency = time.perf_counter() - start
        metrics.inference_latency.observe(latency)
        job.results[item.seq] = result
        log.info(
            "prompt processed",
            extra={
                "job_id": job.id,
                "prompt_id": item.id,
                "status": status,
                "attempts": result.attempts,
                "latency_ms": round(latency * 1000, 2),
            },
        )
