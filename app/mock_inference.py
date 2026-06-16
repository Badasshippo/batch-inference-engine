"""A mock external inference endpoint that periodically rate-limits callers.

This simulates a real provider (e.g. OpenAI/Anthropic) that returns HTTP 429
when it is overloaded. The engine's worker pool is responsible for backing off
and retrying these responses without dropping prompts.

The client is intentionally simple and injectable so tests can substitute a
deterministic version that always (or never) rate-limits.
"""
from __future__ import annotations

import asyncio
import itertools
import random


class RateLimitError(Exception):
    """Raised to mimic an HTTP 429 Too Many Requests response."""

    def __init__(self, retry_after: float = 0.0) -> None:
        super().__init__("429 Too Many Requests")
        self.retry_after = retry_after
        self.status_code = 429


class InferenceError(Exception):
    """Raised to mimic a non-retryable upstream failure (e.g. HTTP 500)."""

    def __init__(self, message: str = "500 Internal Server Error") -> None:
        super().__init__(message)
        self.status_code = 500


class MockInferenceClient:
    """Mock client whose `infer` coroutine occasionally raises RateLimitError.

    `rate_limit_every`: every Nth call returns a 429. Set to 0 to disable.
    The counter is shared across all concurrent callers and guarded by a lock,
    which is what creates the periodic, contention-driven 429s that exercise
    the retry path.
    """

    def __init__(
        self,
        rate_limit_every: int = 7,
        min_latency_ms: int = 5,
        max_latency_ms: int = 25,
        seed: int | None = None,
    ) -> None:
        self.rate_limit_every = rate_limit_every
        self.min_latency_ms = min_latency_ms
        self.max_latency_ms = max_latency_ms
        self._counter = itertools.count(1)
        self._lock = asyncio.Lock()
        self._rng = random.Random(seed)

    async def infer(self, prompt: str) -> str:
        """Run 'inference' on a prompt, sometimes raising RateLimitError."""
        async with self._lock:
            n = next(self._counter)
        if self.rate_limit_every and n % self.rate_limit_every == 0:
            # Suggest a small server-driven backoff window, like Retry-After.
            raise RateLimitError(retry_after=self._rng.uniform(0.05, 0.2))

        # Simulate network + compute latency.
        latency = self._rng.uniform(self.min_latency_ms, self.max_latency_ms) / 1000.0
        await asyncio.sleep(latency)
        return f"completion::{prompt}"
