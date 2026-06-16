"""The batch inference platform engine.

Architecture
------------
All jobs feed a single global fair scheduler that one shared worker pool drains:

    submit --> [FairScheduler: weighted round-robin per job] --> global worker pool
                                                                       |
                              token bucket (RPS)  +  AIMD limiter ------+--> provider
                                                                       |
                              retry/backoff on 429  <------------------+
                                                                       |
                                          results / dead-letter --> JobStore

Why this shape:
* **One pool, not one-pool-per-job** -> a 10k-prompt batch can't multiply
  concurrency or starve small jobs; the scheduler interleaves them fairly.
* **Proactive limiting** (token bucket + AIMD) prevents 429 stampedes; **retries
  with backoff** recover from the 429s that still slip through.
* **JobStore seam** keeps the store swappable (in-memory now, Postgres/Redis in
  prod) for horizontal scale.

The module is framework-agnostic (no FastAPI imports) so it is unit-testable.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import math
import random
import time
from collections.abc import Awaitable, Callable, Sequence

from .config import Settings, get_settings
from .logging_config import get_logger
from .metrics import metrics
from .mock_inference import InferenceError, RateLimitError
from .models import InferenceResult, Job, JobState, Priority, PromptItem, WorkItem
from .providers import InferenceProvider, MockProvider
from .ratelimit import AdaptiveConcurrencyLimiter, TokenBucket
from .scheduler import FairScheduler
from .store import InMemoryJobStore, JobStore

InferFn = Callable[[str], Awaitable[str]]
SleepFn = Callable[[float], Awaitable[None]]

log = get_logger()


class OverloadedError(Exception):
    """Raised when the service refuses a batch due to backpressure limits."""

    def __init__(self, retry_after: int, message: str = "Service overloaded") -> None:
        super().__init__(message)
        self.retry_after = retry_after


class IdempotencyConflictError(Exception):
    """Raised when an Idempotency-Key is reused with a different payload."""


def _fingerprint(prompts: "Sequence[PromptItem]", priority: "Priority") -> str:
    """Stable hash of a submission, used to detect idempotency-key misuse."""
    payload = json.dumps(
        {"p": priority.value, "items": [[p.id, p.prompt] for p in prompts]},
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def compute_backoff(attempt: int, settings: Settings, rng: random.Random | None = None) -> float:
    """Exponential backoff with jitter for a given (1-based) attempt."""
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

    Returns (output, attempts). Raises the last error if retries are exhausted or
    the error is non-retryable. `sleep`/`rng` are injectable for deterministic
    tests; `on_retry(attempt, delay)` fires before each backoff sleep.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            output = await infer(prompt)
            return output, attempt
        except RateLimitError as exc:
            if attempt > settings.max_retries:
                raise
            delay = max(compute_backoff(attempt, settings, rng), exc.retry_after)
            if on_retry is not None:
                on_retry(attempt, delay)
            await sleep(delay)
        except InferenceError:
            raise


class BatchEngine:
    """Coordinates the scheduler, worker pool, rate limiters, and job store."""

    def __init__(
        self,
        infer: InferFn | None = None,
        settings: Settings | None = None,
        *,
        provider: InferenceProvider | None = None,
        store: JobStore | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        if infer is not None:
            self._infer: InferFn = infer
        elif provider is not None:
            self._infer = provider.infer
        else:
            self._infer = MockProvider(
                rate_limit_every=self.settings.mock_rate_limit_every,
                min_latency_ms=self.settings.mock_min_latency_ms,
                max_latency_ms=self.settings.mock_max_latency_ms,
            ).infer

        self.store: JobStore = store or InMemoryJobStore()
        self.scheduler = FairScheduler()
        self._limiter = AdaptiveConcurrencyLimiter(
            initial=self.settings.global_max_concurrency,
            minimum=self.settings.adaptive_min_concurrency,
            maximum=self.settings.global_max_concurrency,
            decrease_factor=self.settings.adaptive_decrease_factor,
            increase_after=self.settings.adaptive_increase_after,
        )
        self._rate_limiter = TokenBucket(
            self.settings.provider_max_rps,
            self.settings.provider_burst or self.settings.provider_max_rps,
        )
        self._active: set[str] = set()
        self._workers: list[asyncio.Task] = []
        self._worker_lock = asyncio.Lock()
        self._accepting = True
        metrics.concurrency_limit.set(self._limiter.limit)

    # ----------------------------- public API ----------------------------- #
    @property
    def active_jobs(self) -> int:
        return len(self._active)

    @property
    def accepting(self) -> bool:
        return self._accepting

    async def submit(
        self,
        prompts: Sequence[PromptItem],
        *,
        priority: Priority = Priority.NORMAL,
        idempotency_key: str | None = None,
    ) -> Job:
        job, _ = await self.submit_with_idempotency(
            prompts, priority=priority, idempotency_key=idempotency_key
        )
        return job

    async def submit_with_idempotency(
        self,
        prompts: Sequence[PromptItem],
        *,
        priority: Priority = Priority.NORMAL,
        idempotency_key: str | None = None,
    ) -> tuple[Job, bool]:
        """Register a job and start processing. Returns (job, idempotent_reuse)."""
        if not self._accepting:
            raise OverloadedError(
                self.settings.overload_retry_after_seconds, "Service is shutting down"
            )
        fingerprint = _fingerprint(prompts, priority)
        if idempotency_key:
            existing = self.store.find_by_idempotency_key(idempotency_key)
            if existing is not None:
                # Same key + same payload -> idempotent reuse. Same key + a
                # *different* payload is a client bug -> 409 Conflict.
                if existing.request_fingerprint != fingerprint:
                    raise IdempotencyConflictError(
                        f"Idempotency-Key '{idempotency_key}' was already used with a "
                        "different payload."
                    )
                metrics.idempotent_reuse.inc()
                return existing, True
        if self.active_jobs >= self.settings.max_active_jobs:
            metrics.jobs_rejected.inc()
            raise OverloadedError(self.settings.overload_retry_after_seconds)
        # Bounded pending-work backpressure: reject if accepting this batch would
        # blow past the configured queue capacity.
        if self.scheduler.pending + len(prompts) > self.settings.max_queue_size:
            metrics.jobs_rejected.inc()
            raise OverloadedError(
                self.settings.overload_retry_after_seconds,
                "Scheduler queue is at capacity",
            )

        metrics.jobs_submitted.inc()
        job = Job(
            total=len(prompts),
            priority=priority,
            idempotency_key=idempotency_key,
            request_fingerprint=fingerprint,
            state=JobState.RUNNING,
            started_at=time.time(),
        )
        items = [
            WorkItem(seq=i, id=(p.id or f"prompt-{i + 1}"), prompt=p.prompt)
            for i, p in enumerate(prompts)
        ]
        self.store.create(job)
        self._active.add(job.id)
        self.scheduler.add_job(job.id, items, priority)
        self._refresh_gauges()
        await self._ensure_workers()
        log.info(
            "job submitted",
            extra={"job_id": job.id, "total": job.total, "priority": priority.value},
        )
        return job, False

    def get_job(self, job_id: str) -> Job | None:
        return self.store.get(job_id)

    def job_pending(self, job_id: str) -> int:
        return self.scheduler.job_pending(job_id)

    async def cancel(self, job_id: str) -> bool | None:
        job = self.store.get(job_id)
        if job is None:
            return None
        if job.state in (JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED):
            return False
        removed = self.scheduler.remove_job(job_id)
        self._finalize(job, JobState.CANCELLED)
        metrics.jobs_cancelled.inc()
        self._refresh_gauges()
        log.info("job cancelled", extra={"job_id": job_id, "dropped_pending": removed})
        return True

    async def replay_failed(self, job_id: str, priority: Priority | None = None) -> Job | None:
        """Create a *new* job containing only the failed prompts of `job_id`."""
        job = self.store.get(job_id)
        if job is None:
            return None
        failed = job.dead_letter()
        if not failed:
            return None
        prompts = [PromptItem(id=r.id, prompt=r.prompt) for r in failed]
        new_job = await self.submit(prompts, priority=priority or job.priority)
        log.info(
            "replayed failed prompts",
            extra={"source_job": job_id, "new_job": new_job.id, "count": len(prompts)},
        )
        return new_job

    async def shutdown(self) -> None:
        """Graceful shutdown: stop accepting, drain in-flight up to the grace
        window, cancel the rest, and mark unfinished jobs as cancelled."""
        self._accepting = False
        try:
            await asyncio.wait_for(
                self._await_workers(), timeout=self.settings.graceful_shutdown_seconds
            )
        except (asyncio.TimeoutError, TimeoutError):
            pass
        for w in self._workers:
            w.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)

        interrupted = 0
        for job in self.store.all():
            if job.state in (JobState.PENDING, JobState.RUNNING):
                self._finalize(job, JobState.CANCELLED)
                interrupted += 1
        log.info("shutdown complete", extra={"interrupted_jobs": interrupted})

    # --------------------------- internal logic --------------------------- #
    async def _await_workers(self) -> None:
        while True:
            live = [w for w in self._workers if not w.done()]
            if not live:
                return
            await asyncio.gather(*live, return_exceptions=True)

    async def _ensure_workers(self) -> None:
        """Lazily (re)spawn the global worker pool up to the configured size.

        Workers exit when the scheduler drains, so there are no idle background
        tasks between batches; the next submit respawns them.
        """
        async with self._worker_lock:
            self._workers = [w for w in self._workers if not w.done()]
            target = min(self.settings.worker_pool_size, max(1, self.scheduler.pending))
            for _ in range(max(0, target - len(self._workers))):
                self._workers.append(asyncio.create_task(self._worker()))

    async def _worker(self) -> None:
        while True:
            await self._limiter.acquire()
            try:
                got = self.scheduler.pop()
                if got is None:
                    return
                job_id, item = got
                metrics.queue_depth.set(self.scheduler.pending)
                metrics.inflight.set(self._limiter.in_use)
                try:
                    await self._process(job_id, item)
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001 - never let one item kill the worker
                    log.exception("worker error processing item", extra={"job_id": job_id})
            finally:
                await self._limiter.release()
                metrics.inflight.set(self._limiter.in_use)

    async def _gated_infer(self, prompt: str) -> str:
        # Proactive global RPS cap before every upstream call.
        await self._rate_limiter.acquire()
        return await self._infer(prompt)

    def _estimate_tokens(self, text: str) -> int:
        return max(1, math.ceil(len(text) / self.settings.chars_per_token))

    async def _process(self, job_id: str, item: WorkItem) -> None:
        job = self.store.get(job_id)
        if job is None or job.state == JobState.CANCELLED:
            return  # job was cancelled after this item was dequeued

        start = time.perf_counter()

        def _on_retry(_attempt: int, _delay: float) -> None:
            job.retries += 1
            metrics.inference_retries.inc()
            metrics.inference_rate_limited.inc()
            self._limiter.record_throttle()  # AIMD: shrink on throttle
            metrics.concurrency_limit.set(self._limiter.limit)

        status = "succeeded"
        in_tokens = out_tokens = 0
        cost = 0.0
        try:
            output, attempts = await infer_with_retry(
                self._gated_infer, item.prompt, self.settings, on_retry=_on_retry
            )
            self._limiter.record_success()  # AIMD: grow on sustained success
            in_tokens = self._estimate_tokens(item.prompt)
            out_tokens = self._estimate_tokens(output)
            cost = (
                in_tokens / 1000 * self.settings.cost_per_1k_input_tokens
                + out_tokens / 1000 * self.settings.cost_per_1k_output_tokens
            )
            result = InferenceResult(
                id=item.id,
                prompt=item.prompt,
                success=True,
                output=output,
                attempts=attempts,
                input_tokens=in_tokens,
                output_tokens=out_tokens,
                cost_usd=round(cost, 8),
            )
            job.succeeded += 1
            job.input_tokens += in_tokens
            job.output_tokens += out_tokens
            job.cost_usd += cost
            metrics.prompts_succeeded.inc()
            metrics.tokens.inc(in_tokens + out_tokens)
            metrics.cost_usd.inc(cost)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - isolate any per-prompt failure
            status = "failed"
            if isinstance(exc, RateLimitError):
                metrics.retries_exhausted.inc()
            result = InferenceResult(
                id=item.id, prompt=item.prompt, success=False, error=str(exc)
            )
            job.failed += 1
            metrics.prompts_failed.inc()

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
                "cost_usd": round(cost, 8),
            },
        )

        if job.completed >= job.total and job.state == JobState.RUNNING:
            self._finalize(job, JobState.COMPLETED)
            self._refresh_gauges()

    def _finalize(self, job: Job, state: JobState) -> None:
        job.state = state
        if job.finished_at is None:
            job.finished_at = time.time()
        self._active.discard(job.id)
        if state == JobState.COMPLETED:
            metrics.jobs_completed.inc()
            if job.started_at is not None:
                metrics.job_duration.observe(job.finished_at - job.started_at)
            log.info(
                "job finished",
                extra={
                    "job_id": job.id,
                    "state": state.value,
                    "succeeded": job.succeeded,
                    "failed": job.failed,
                    "retries": job.retries,
                    "cost_usd": round(job.cost_usd, 6),
                },
            )

    def _refresh_gauges(self) -> None:
        metrics.queue_depth.set(self.scheduler.pending)
        metrics.active_jobs.set(self.scheduler.active_jobs)
        metrics.concurrency_limit.set(self._limiter.limit)
