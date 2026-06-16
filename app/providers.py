"""Inference provider abstraction.

The engine talks to inference backends through the `InferenceProvider` protocol,
so a real provider (OpenAI, Anthropic, a self-hosted model on DOKS, ...) can be
dropped in without touching the scheduler, rate limiter, or retry logic. The
included implementations are all *mock* providers used to exercise the platform.
"""
from __future__ import annotations

import asyncio
from typing import Protocol, runtime_checkable

from .mock_inference import MockInferenceClient, RateLimitError


@runtime_checkable
class InferenceProvider(Protocol):
    name: str

    async def infer(self, prompt: str) -> str:  # pragma: no cover - protocol
        ...


class MockProvider:
    """Default provider: periodic 429s + small simulated latency."""

    name = "mock"

    def __init__(
        self,
        rate_limit_every: int = 7,
        min_latency_ms: int = 5,
        max_latency_ms: int = 25,
        seed: int | None = None,
    ) -> None:
        self._client = MockInferenceClient(
            rate_limit_every=rate_limit_every,
            min_latency_ms=min_latency_ms,
            max_latency_ms=max_latency_ms,
            seed=seed,
        )

    async def infer(self, prompt: str) -> str:
        return await self._client.infer(prompt)


class SlowProvider:
    """Provider with high, configurable latency (useful for load/timeout demos)."""

    name = "slow"

    def __init__(self, latency_ms: int = 250) -> None:
        self._latency = latency_ms / 1000.0

    async def infer(self, prompt: str) -> str:
        await asyncio.sleep(self._latency)
        return f"slow-completion::{prompt}"


class FlakyProvider:
    """Provider that raises 429 for the first `fail_times` calls, then succeeds.

    Deterministic, so it is handy in tests of the retry/backoff path.
    """

    name = "flaky"

    def __init__(self, fail_times: int = 2) -> None:
        self._fail_times = fail_times
        self._calls = 0

    async def infer(self, prompt: str) -> str:
        self._calls += 1
        if self._calls <= self._fail_times:
            raise RateLimitError(retry_after=0.01)
        return f"completion::{prompt}"
