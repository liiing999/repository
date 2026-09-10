"""熔断器状态机：跳闸、OPEN 拒绝、定时转 HALF_OPEN、试探成败。"""
from __future__ import annotations

import pytest

from gateway.circuitbreaker import (
    BreakerOpenError,
    BreakerState,
    CircuitBreaker,
)
from gateway.config import CircuitBreakerConfig


def make_breaker(**kw) -> CircuitBreaker:
    defaults = dict(
        window_seconds=10, min_requests=4, failure_rate=0.5,
        open_seconds=0.3, half_open_max_calls=1,
    )
    defaults.update(kw)
    return CircuitBreaker("u", CircuitBreakerConfig(**defaults))


async def _fail(b: CircuitBreaker) -> None:
    await b.allow()
    await b.record_failure()


async def _succeed(b: CircuitBreaker) -> None:
    await b.allow()
    await b.record_success()


@pytest.mark.asyncio
async def test_trips_after_threshold() -> None:
    b = make_breaker()
    for _ in range(4):
        await _fail(b)
    assert b.state is BreakerState.OPEN
    with pytest.raises(BreakerOpenError):
        await b.allow()


@pytest.mark.asyncio
async def test_below_min_requests_does_not_trip() -> None:
    b = make_breaker()
    for _ in range(3):                # 全部失败但样本不足 min_requests
        await _fail(b)
    assert b.state is BreakerState.CLOSED
    await b.allow()                  # 仍然放行


@pytest.mark.asyncio
async def test_successes_prevent_trip() -> None:
    b = make_breaker()
    for _ in range(4):
        await _succeed(b)
    for _ in range(3):
        await _fail(b)
    # 7 个样本中 3 失败 ≈ 0.43 < 0.5
    assert b.state is BreakerState.CLOSED


@pytest.mark.asyncio
async def test_open_then_half_open_success_recovers() -> None:
    b = make_breaker()
    for _ in range(4):
        await _fail(b)
    assert b.state is BreakerState.OPEN

    # 冷却结束后，第一个 allow 进入 HALF_OPEN 并占用探测名额
    b._opened_at -= b.cfg.open_seconds + 0.01  # type: ignore[attr-defined]
    await b.allow()
    assert b.state is BreakerState.HALF_OPEN
    with pytest.raises(BreakerOpenError):
        await b.allow()              # half_open_max_calls=1，第二个被拒

    await b.record_success()
    assert b.state is BreakerState.CLOSED
    snap = b.snapshot()
    assert snap.total == 0           # 恢复后清空旧失败样本


@pytest.mark.asyncio
async def test_half_open_failure_reopens() -> None:
    b = make_breaker(open_seconds=0.3)
    for _ in range(4):
        await _fail(b)
    b._opened_at -= b.cfg.open_seconds + 0.01  # type: ignore[attr-defined]
    await b.allow()
    await b.record_failure()
    assert b.state is BreakerState.OPEN
    # 重新计时：立刻仍然拒绝
    with pytest.raises(BreakerOpenError):
        await b.allow()


@pytest.mark.asyncio
async def test_half_open_multiple_probes() -> None:
    b = make_breaker(half_open_max_calls=3)
    for _ in range(4):
        await _fail(b)
    b._opened_at -= b.cfg.open_seconds + 0.01  # type: ignore[attr-defined]
    await b.allow()
    await b.allow()
    await b.allow()
    with pytest.raises(BreakerOpenError):
        await b.allow()              # 第 4 个超过探测名额


@pytest.mark.asyncio
async def test_sliding_window_forgets_old_failures() -> None:
    b = make_breaker(window_seconds=10, min_requests=4)
    # 手工塞入 4 个"很久以前"的失败
    old = -100
    b._events.extend([(old, False)] * 4)  # type: allow[attr-defined]
    # 旧样本滑出窗口，不应触发熔断
    await b.allow()
    await b.record_failure()
    assert b.state is BreakerState.CLOSED
