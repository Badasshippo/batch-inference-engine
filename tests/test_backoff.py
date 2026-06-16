"""Unit tests for the 429 back-off / retry logic.

These verify the core engineering requirement: when the mock endpoint returns
HTTP 429, workers back off and retry rather than dropping the prompt or failing
the whole batch.
"""
from __future__ import annotations

import random

import pytest

from app.engine import compute_backoff, infer_with_retry
from app.mock_inference import InferenceError, RateLimitError
from tests.conftest import FlakyInfer


def test_compute_backoff_is_exponential_and_capped(settings):
    # base=0.1, jitter=0 -> 0.1, 0.2, 0.4, 0.8, 1.6, then capped at 2.0
    delays = [compute_backoff(a, settings, rng=random.Random(0)) for a in range(1, 8)]
    assert delays[0] == pytest.approx(0.1)
    assert delays[1] == pytest.approx(0.2)
    assert delays[2] == pytest.approx(0.4)
    assert delays[3] == pytest.approx(0.8)
    assert delays[4] == pytest.approx(1.6)
    # Capped at backoff_max_seconds.
    assert delays[5] == pytest.approx(2.0)
    assert delays[6] == pytest.approx(2.0)


def test_backoff_jitter_stays_within_bounds():
    from app.config import Settings

    s = Settings(backoff_base_seconds=0.1, backoff_max_seconds=2.0, backoff_jitter=0.5)
    rng = random.Random(42)
    for attempt in range(1, 6):
        base = min(0.1 * (2 ** (attempt - 1)), 2.0)
        d = compute_backoff(attempt, s, rng=rng)
        assert base <= d <= base + 0.5


async def test_retry_succeeds_after_429s(settings, recording_sleep, rng):
    """Two 429s then success: returns output, slept twice, 3 attempts total."""
    infer = FlakyInfer(fail_times=2, exc_factory=RateLimitError)

    output, attempts = await infer_with_retry(
        infer, "hello", settings, sleep=recording_sleep, rng=rng
    )

    assert output == "ok::hello"
    assert attempts == 3
    assert infer.calls == 3
    # Backoff triggered exactly twice (once per 429).
    assert recording_sleep.count == 2
    assert recording_sleep.delays == pytest.approx([0.1, 0.2])


async def test_retry_counts_each_backoff(settings, recording_sleep, rng):
    retries = []
    infer = FlakyInfer(fail_times=3, exc_factory=RateLimitError)

    await infer_with_retry(
        infer,
        "x",
        settings,
        sleep=recording_sleep,
        rng=rng,
        on_retry=lambda attempt, delay: retries.append((attempt, delay)),
    )

    assert len(retries) == 3
    assert [r[0] for r in retries] == [1, 2, 3]


async def test_retry_exhausts_budget_then_raises(settings, recording_sleep, rng):
    """Persistent 429s: raises after max_retries backoffs, doesn't loop forever."""
    infer = FlakyInfer(fail_times=999, exc_factory=RateLimitError)

    with pytest.raises(RateLimitError):
        await infer_with_retry(infer, "x", settings, sleep=recording_sleep, rng=rng)

    # max_retries=5 -> 5 backoff sleeps, 6 total attempts.
    assert recording_sleep.count == settings.max_retries
    assert infer.calls == settings.max_retries + 1


async def test_non_retryable_error_is_not_retried(settings, recording_sleep, rng):
    """A 500-style error surfaces immediately with no backoff."""
    infer = FlakyInfer(fail_times=1, exc_factory=InferenceError)

    with pytest.raises(InferenceError):
        await infer_with_retry(infer, "x", settings, sleep=recording_sleep, rng=rng)

    assert recording_sleep.count == 0
    assert infer.calls == 1


async def test_retry_after_hint_is_respected(settings, recording_sleep, rng):
    """If the server's Retry-After exceeds our computed backoff, we wait longer."""

    def big_retry_after():
        return RateLimitError(retry_after=5.0)

    infer = FlakyInfer(fail_times=1, exc_factory=big_retry_after)
    await infer_with_retry(infer, "x", settings, sleep=recording_sleep, rng=rng)

    # computed backoff for attempt 1 is 0.1, but Retry-After=5.0 wins.
    assert recording_sleep.delays == pytest.approx([5.0])
