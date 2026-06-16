"""Unit tests for the adaptive concurrency limiter and token bucket."""
from __future__ import annotations

import asyncio

from app.ratelimit import AdaptiveConcurrencyLimiter, TokenBucket


def test_aimd_multiplicative_decrease_on_throttle():
    lim = AdaptiveConcurrencyLimiter(initial=10, minimum=2, decrease_factor=0.5)
    assert lim.limit == 10
    lim.record_throttle()
    assert lim.limit == 5
    lim.record_throttle()
    assert lim.limit == 2  # max(min, int(5*0.5)) = 2
    lim.record_throttle()
    assert lim.limit == 2  # floored at minimum


def test_aimd_additive_increase_after_success_streak():
    lim = AdaptiveConcurrencyLimiter(initial=2, minimum=1, maximum=5, increase_after=3)
    for _ in range(2):
        lim.record_success()
    assert lim.limit == 2  # not yet
    lim.record_success()
    assert lim.limit == 3  # streak reached -> +1
    # Cannot exceed maximum.
    for _ in range(100):
        lim.record_success()
    assert lim.limit == 5


async def test_limiter_blocks_beyond_limit():
    lim = AdaptiveConcurrencyLimiter(initial=2, minimum=1, maximum=2)
    await lim.acquire()
    await lim.acquire()
    assert lim.in_use == 2

    third = asyncio.create_task(lim.acquire())
    await asyncio.sleep(0.02)
    assert not third.done()  # blocked at the limit

    await lim.release()
    await asyncio.sleep(0.02)
    assert third.done()  # a permit freed up
    await lim.release()
    await lim.release()


async def test_token_bucket_disabled_is_noop():
    tb = TokenBucket(rate_per_sec=0)
    for _ in range(1000):
        await tb.acquire()  # never blocks


async def test_token_bucket_enforces_rate():
    # capacity 5, rate 50/s. Burst of 5 is instant; the 6th waits for a refill.
    tb = TokenBucket(rate_per_sec=50, capacity=5)
    loop = asyncio.get_event_loop()
    start = loop.time()
    for _ in range(5):
        await tb.acquire()
    mid = loop.time()
    await tb.acquire()  # must wait ~1/50s for a token
    end = loop.time()

    assert (mid - start) < 0.02      # first 5 were instant
    assert (end - mid) >= 0.01       # 6th had to wait for refill
