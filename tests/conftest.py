"""Shared test fixtures and fakes."""
from __future__ import annotations

import random

import pytest

from app.config import Settings


class RecordingSleep:
    """An async sleep replacement that records delays instead of waiting.

    Lets us assert that backoff actually triggers (and how long it would wait)
    without slowing the test suite down.
    """

    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)

    @property
    def count(self) -> int:
        return len(self.delays)


class FlakyInfer:
    """Fake inference fn that raises 429 a fixed number of times, then succeeds."""

    def __init__(self, fail_times: int, exc_factory) -> None:
        self.fail_times = fail_times
        self.exc_factory = exc_factory
        self.calls = 0

    async def __call__(self, prompt: str) -> str:
        self.calls += 1
        if self.calls <= self.fail_times:
            raise self.exc_factory()
        return f"ok::{prompt}"


@pytest.fixture
def settings() -> Settings:
    # Deterministic, small backoff so test math is predictable.
    return Settings(
        worker_pool_size=4,
        max_queue_size=100,
        max_retries=5,
        backoff_base_seconds=0.1,
        backoff_max_seconds=2.0,
        backoff_jitter=0.0,  # no jitter -> deterministic delays
        mock_rate_limit_every=3,
    )


@pytest.fixture
def rng() -> random.Random:
    return random.Random(1234)


@pytest.fixture
def recording_sleep() -> RecordingSleep:
    return RecordingSleep()
