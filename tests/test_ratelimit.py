"""令牌桶限流：容量边界、惰性补充、高并发下的正确性。"""
from __future__ import annotations

import asyncio
import time

import pytest

from gateway.config import RateLimitConfig
from gateway.ratelimit import InMemoryRateLimiter, TokenBucket


def test_bucket_capacity_burst() -> None:
    b = TokenBucket(capacity=5, refill_per_second=100)
    now = time.monotonic()
    assert [b.take(1, now) for _ in range(5)] == [True] * 5
    assert b.take(1, now) is False          # 桶空，第 6 个立即拒绝


def test_bucket_refill() -> None:
    b = TokenBucket(capacity=10, refill_per_second=4)
    t0 = time.monotonic()
    for _ in range(10):
        assert b.take(1, t0)
    assert b.take(1, t0) is False
    # 0.5 秒后应补充 2 个令牌
    assert b.take(1, t0 + 0.5) is True
    assert b.take(1, t0 + 0.5) is True
    assert b.take(1, t0 + 0.5) is False     # 当时点只补了 2 个


def test_bucket_never_exceeds_capacity() -> None:
    b = TokenBucket(capacity=3, refill_per_second=100)
    t0 = time.monotonic()
    b.take(3, t0)
    assert b.take(1, t0 + 100) is True
    assert b.take(1, t0 + 100) is True
    assert b.take(1, t0 + 100) is True
    assert b.take(1, t0 + 100) is False      # 长时间空闲也不会超过容量


@pytest.mark.asyncio
async def test_concurrent_acquires_never_oversell() -> None:
    """500 个协程同时抢 100 容量的桶：放行数必须恰好等于容量。

    这是并发正确性的关键断言：补充/判断/扣减在无 await 临界区内完成，
    高并发下不会超发。
    """
    limiter = InMemoryRateLimiter()
    limiter.register("r", RateLimitConfig(capacity=100, refill_per_second=0))
    results = await asyncio.gather(*[limiter.acquire("r") for _ in range(500)])
    assert sum(results) == 100
    snap = limiter.snapshot()[0]
    assert (snap.allowed, snap.rejected) == (100, 400)


@pytest.mark.asyncio
async def test_routes_isolated() -> None:
    limiter = InMemoryRateLimiter()
    limiter.register("a", RateLimitConfig(capacity=1, refill_per_second=0))
    limiter.register("b", RateLimitConfig(capacity=1, refill_per_second=0))
    assert await limiter.acquire("a") is True
    assert await limiter.acquire("a") is False
    # a 桶空不影响 b 桶
    assert await limiter.acquire("b") is True
