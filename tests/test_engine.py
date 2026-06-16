"""Integration-style tests for BatchEngine: concurrency + resilience."""
from __future__ import annotations

import asyncio

from app.config import Settings
from app.engine import BatchEngine
from app.mock_inference import InferenceError, MockInferenceClient, RateLimitError
from app.models import JobState, PromptItem


def _prompts(n: int) -> list[PromptItem]:
    return [PromptItem(id=f"p{i}", prompt=f"prompt-{i}") for i in range(n)]


async def _wait_for(job, engine, timeout: float = 10.0) -> None:
    """Poll until the job reaches a terminal state."""
    deadline = asyncio.get_event_loop().time() + timeout
    while job.state in (JobState.PENDING, JobState.RUNNING):
        if asyncio.get_event_loop().time() > deadline:
            raise AssertionError(f"job did not finish; state={job.state}")
        await asyncio.sleep(0.01)


async def test_rate_limits_do_not_fail_the_batch():
    """Every 3rd call 429s, but with retries the whole batch still succeeds."""
    settings = Settings(
        worker_pool_size=8,
        max_retries=8,
        backoff_base_seconds=0.001,
        backoff_max_seconds=0.01,
        backoff_jitter=0.0,
        mock_rate_limit_every=3,
        mock_min_latency_ms=0,
        mock_max_latency_ms=1,
    )
    client = MockInferenceClient(rate_limit_every=3, min_latency_ms=0, max_latency_ms=1, seed=1)
    engine = BatchEngine(infer=client.infer, settings=settings)

    job = await engine.submit(_prompts(50))
    await _wait_for(job, engine)

    assert job.state == JobState.COMPLETED
    assert job.succeeded == 50
    assert job.failed == 0
    assert job.completed == 50
    # The 429s happened and were retried, not dropped.
    assert job.retries > 0


async def test_immediate_ack_then_background_processing():
    """submit() returns before processing finishes (non-blocking ack)."""
    settings = Settings(worker_pool_size=2, mock_min_latency_ms=5, mock_max_latency_ms=10)
    client = MockInferenceClient(rate_limit_every=0, min_latency_ms=5, max_latency_ms=10, seed=2)
    engine = BatchEngine(infer=client.infer, settings=settings)

    job = await engine.submit(_prompts(20))
    # Right after submit, the job should not yet be fully done.
    assert job.state in (JobState.PENDING, JobState.RUNNING)
    assert job.completed < 20

    await _wait_for(job, engine)
    assert job.state == JobState.COMPLETED
    assert job.completed == 20


async def test_non_retryable_failures_are_isolated():
    """A prompt that always 500s is marked failed; the batch still completes."""

    async def infer(prompt: str) -> str:
        if prompt == "prompt-1":
            raise InferenceError("boom")
        return f"ok::{prompt}"

    settings = Settings(worker_pool_size=4, max_retries=2, backoff_base_seconds=0.001)
    engine = BatchEngine(infer=infer, settings=settings)

    job = await engine.submit(_prompts(5))
    await _wait_for(job, engine)

    assert job.state == JobState.COMPLETED
    assert job.failed == 1
    assert job.succeeded == 4
    assert job.results["p1"].success is False


async def test_worker_pool_is_bounded():
    """No more than worker_pool_size inferences run concurrently."""
    pool_size = 5
    concurrent = 0
    peak = 0
    lock = asyncio.Lock()

    async def infer(prompt: str) -> str:
        nonlocal concurrent, peak
        async with lock:
            concurrent += 1
            peak = max(peak, concurrent)
        try:
            await asyncio.sleep(0.01)
            return f"ok::{prompt}"
        finally:
            async with lock:
                concurrent -= 1

    settings = Settings(worker_pool_size=pool_size, max_queue_size=1000)
    engine = BatchEngine(infer=infer, settings=settings)

    job = await engine.submit(_prompts(100))
    await _wait_for(job, engine)

    assert job.state == JobState.COMPLETED
    assert job.succeeded == 100
    # The bound must never be exceeded.
    assert peak <= pool_size


async def test_large_batch_completes():
    """A 1,000-prompt batch completes with bounded resources."""
    settings = Settings(
        worker_pool_size=32,
        max_queue_size=500,
        max_retries=10,
        backoff_base_seconds=0.001,
        backoff_max_seconds=0.01,
        backoff_jitter=0.0,
        mock_min_latency_ms=0,
        mock_max_latency_ms=1,
    )
    client = MockInferenceClient(rate_limit_every=11, min_latency_ms=0, max_latency_ms=1, seed=7)
    engine = BatchEngine(infer=client.infer, settings=settings)

    job = await engine.submit(_prompts(1000))
    await _wait_for(job, engine, timeout=30.0)

    assert job.state == JobState.COMPLETED
    assert job.completed == 1000
    assert job.succeeded == 1000
