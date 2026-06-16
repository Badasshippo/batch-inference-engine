"""Tests for the review fixes and operational hardening:

- stable per-item IDs (no result collisions)
- unexpected per-prompt exceptions don't hang the job
- global concurrency semaphore across jobs
- API backpressure (503 + Retry-After) and graceful shutdown
- Prometheus metrics increment
- CI triggers on the active branch
"""
from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from app.config import Settings
from app.engine import BatchEngine, OverloadedError
from app.metrics import metrics, reset_metrics
from app.models import JobState, PromptItem


async def _wait_for(job, timeout: float = 10.0) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while job.state in (JobState.PENDING, JobState.RUNNING):
        if asyncio.get_event_loop().time() > deadline:
            raise AssertionError(f"job did not finish; state={job.state}")
        await asyncio.sleep(0.01)


async def test_prompts_without_ids_do_not_collide():
    """100 prompts with NO id must produce 100 distinct results (P1 fix)."""
    async def infer(prompt: str) -> str:
        await asyncio.sleep(0.001)
        return f"ok::{prompt}"

    settings = Settings(worker_pool_size=20, mock_rate_limit_every=0)
    engine = BatchEngine(infer=infer, settings=settings)

    prompts = [PromptItem(prompt=f"text-{i}") for i in range(100)]
    job = await engine.submit(prompts)
    await _wait_for(job)

    assert job.state == JobState.COMPLETED
    assert job.succeeded == 100
    # No overwrites: exactly 100 stored results with 100 unique ids.
    assert len(job.results) == 100
    assert len({r.id for r in job.results.values()}) == 100


async def test_unexpected_exception_does_not_hang_job():
    """A non-RateLimit/non-Inference exception is isolated; the job completes."""
    async def infer(prompt: str) -> str:
        if prompt == "text-2":
            raise ValueError("totally unexpected")
        return f"ok::{prompt}"

    settings = Settings(worker_pool_size=4, mock_rate_limit_every=0)
    engine = BatchEngine(infer=infer, settings=settings)

    prompts = [PromptItem(id=f"p{i}", prompt=f"text-{i}") for i in range(5)]
    job = await engine.submit(prompts)
    await _wait_for(job, timeout=5.0)

    assert job.state == JobState.COMPLETED
    assert job.failed == 1
    assert job.succeeded == 4


async def test_global_semaphore_caps_concurrency_across_jobs():
    """Two jobs, each with a big pool, must still respect global_max_concurrency."""
    limit = 5
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

    settings = Settings(
        worker_pool_size=20,
        global_max_concurrency=limit,
        max_active_jobs=10,
        mock_rate_limit_every=0,
    )
    engine = BatchEngine(infer=infer, settings=settings)

    job1 = await engine.submit([PromptItem(prompt=f"a{i}") for i in range(50)])
    job2 = await engine.submit([PromptItem(prompt=f"b{i}") for i in range(50)])
    await _wait_for(job1)
    await _wait_for(job2)

    assert peak <= limit


async def test_backpressure_rejects_when_too_many_active_jobs():
    """Once max_active_jobs is reached, submit() raises OverloadedError (-> 503)."""
    async def infer(prompt: str) -> str:
        await asyncio.sleep(0.1)
        return f"ok::{prompt}"

    settings = Settings(worker_pool_size=1, max_active_jobs=1, mock_rate_limit_every=0)
    engine = BatchEngine(infer=infer, settings=settings)

    job1 = await engine.submit([PromptItem(prompt="x") for _ in range(10)])
    with pytest.raises(OverloadedError) as ei:
        await engine.submit([PromptItem(prompt="y")])
    assert ei.value.retry_after == settings.overload_retry_after_seconds

    await _wait_for(job1)
    # After the first job drains, new submissions are accepted again.
    job2 = await engine.submit([PromptItem(prompt="z")])
    await _wait_for(job2)
    assert job2.state == JobState.COMPLETED


async def test_cancel_marks_job_cancelled():
    async def infer(prompt: str) -> str:
        await asyncio.sleep(0.05)
        return f"ok::{prompt}"

    settings = Settings(worker_pool_size=2, mock_rate_limit_every=0)
    engine = BatchEngine(infer=infer, settings=settings)

    job = await engine.submit([PromptItem(prompt=f"x{i}") for i in range(200)])
    await asyncio.sleep(0.02)
    outcome = await engine.cancel(job.id)
    assert outcome is True
    # Give the cancellation a moment to propagate.
    await asyncio.sleep(0.05)
    assert job.state == JobState.CANCELLED


async def test_metrics_increment_on_processing():
    reset_metrics()

    async def infer(prompt: str) -> str:
        return f"ok::{prompt}"

    settings = Settings(worker_pool_size=4, mock_rate_limit_every=0)
    engine = BatchEngine(infer=infer, settings=settings)

    job = await engine.submit([PromptItem(prompt=f"x{i}") for i in range(10)])
    await _wait_for(job)

    assert metrics.jobs_submitted.value == 1
    assert metrics.jobs_completed.value == 1
    assert metrics.prompts_completed.value == 10
    assert metrics.prompts_succeeded.value == 10
    assert metrics.inference_latency.count == 10
    # Render must produce valid Prometheus text.
    text = metrics.render()
    assert "batch_prompts_completed_total 10" in text
    assert "# TYPE inference_latency_seconds histogram" in text


def test_histogram_buckets_are_cumulative_not_double_counted():
    from app.metrics import Histogram

    h = Histogram("t", "help", buckets=(0.1, 0.5, 1.0))
    h.observe(0.05)
    h.observe(0.05)
    text = "\n".join(h.render())
    # Two observations <= 0.1: every `le` bucket must read 2 (cumulative), not 4.
    assert 't_bucket{le="0.1"} 2' in text
    assert 't_bucket{le="0.5"} 2' in text
    assert 't_bucket{le="1.0"} 2' in text
    assert 't_bucket{le="+Inf"} 2' in text
    assert "t_count 2" in text

    # An observation above all finite buckets only shows in +Inf.
    h.observe(2.0)
    text = "\n".join(h.render())
    assert 't_bucket{le="1.0"} 2' in text
    assert 't_bucket{le="+Inf"} 3' in text


def test_ci_runs_on_active_branch():
    """The CI workflow must trigger on the repo's actual default branch."""
    ci = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "ci.yml"
    text = ci.read_text()
    # Guard against the master/main mismatch called out in review.
    assert "master" in text
    assert "main" in text
