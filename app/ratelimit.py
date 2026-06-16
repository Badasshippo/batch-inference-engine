"""Proactive rate limiting primitives.

Retries handle failure *after* it happens; these primitives prevent stampedes
*before* they happen:

* `TokenBucket` enforces a global requests-per-second ceiling on the upstream
  provider (a shared, constrained system).
* `AdaptiveConcurrencyLimiter` implements AIMD (additive-increase /
  multiplicative-decrease, the same control law as TCP congestion control):
  every observed 429 multiplicatively shrinks the allowed concurrency, and a
  streak of successes slowly grows it back. This keeps us near the provider's
  real capacity without hammering it.
"""
from __future__ import annotations

import asyncio


class TokenBucket:
    """Async token bucket. A `rate` of 0 disables limiting (unlimited)."""

    def __init__(self, rate_per_sec: float, capacity: float | None = None) -> None:
        self.rate = rate_per_sec
        self.capacity = capacity if capacity is not None else max(rate_per_sec, 1.0)
        self._tokens = self.capacity
        self._lock = asyncio.Lock()
        self._last = None  # set lazily on the running loop

    def _now(self) -> float:
        return asyncio.get_running_loop().time()

    def _refill(self) -> None:
        now = self._now()
        if self._last is None:
            self._last = now
            return
        elapsed = now - self._last
        self._last = now
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)

    async def acquire(self, n: float = 1.0) -> None:
        if self.rate <= 0:
            return
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= n:
                    self._tokens -= n
                    return
                deficit = n - self._tokens
                wait = deficit / self.rate
            await asyncio.sleep(wait)

    @property
    def available(self) -> float:
        return self._tokens


class AdaptiveConcurrencyLimiter:
    """AIMD concurrency gate.

    Acts like an `asyncio.Semaphore` whose limit moves with observed load:
      * `record_throttle()` -> limit = max(min, floor(limit * decrease_factor))
      * `record_success()`  -> after `increase_after` consecutive successes,
                               limit = min(max, limit + 1)
    """

    def __init__(
        self,
        initial: int,
        minimum: int = 1,
        maximum: int | None = None,
        decrease_factor: float = 0.5,
        increase_after: int = 10,
    ) -> None:
        # Clamp so the invariant min <= limit <= max always holds, even if the
        # operator misconfigures ADAPTIVE_MIN_CONCURRENCY > GLOBAL_MAX_CONCURRENCY.
        self._max = maximum if maximum is not None else initial
        self._min = min(minimum, self._max)
        self._limit = max(self._min, min(initial, self._max))
        self._decrease = decrease_factor
        self._increase_after = increase_after
        self._in_use = 0
        self._success_streak = 0
        self._cond = asyncio.Condition()

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def in_use(self) -> int:
        return self._in_use

    async def acquire(self) -> None:
        async with self._cond:
            while self._in_use >= self._limit:
                await self._cond.wait()
            self._in_use += 1

    async def release(self) -> None:
        async with self._cond:
            self._in_use -= 1
            self._cond.notify(1)

    async def __aenter__(self) -> "AdaptiveConcurrencyLimiter":
        await self.acquire()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.release()

    def record_throttle(self) -> None:
        """A 429 was observed: multiplicatively decrease the limit.

        Synchronous (a plain int update on the single event loop). Waiters do not
        need to be woken on a *decrease* -- fewer permits become available, never
        more.
        """
        self._success_streak = 0
        self._limit = max(self._min, int(self._limit * self._decrease))

    def record_success(self) -> None:
        """A success was observed: additively increase after a streak.

        Synchronous. We don't notify here: every success is immediately followed
        by the caller's `release()`, which notifies a waiter that then re-checks
        against the (possibly higher) limit.
        """
        self._success_streak += 1
        if self._success_streak >= self._increase_after and self._limit < self._max:
            self._limit += 1
            self._success_streak = 0
