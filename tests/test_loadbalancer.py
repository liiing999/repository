"""负载均衡：轮询公平性、最少连接、熔断跳过、重试排除已试上游。"""
from __future__ import annotations

import pytest

from gateway.circuitbreaker import BreakerRegistry
from gateway.config import (
    CircuitBreakerConfig,
    RetryConfig,
    RouteConfig,
    UpstreamConfig,
)
from gateway.loadbalancer import LoadBalancer, NoHealthyUpstreamError


def upstreams() -> list[UpstreamConfig]:
    return [UpstreamConfig(name=f"u{i}", base_url=f"http://u{i}") for i in range(3)]


def route(lb: str) -> RouteConfig:
    return RouteConfig(
        name="r", path="/r", upstreams=("u0", "u1", "u2"), lb=lb,
        retries=RetryConfig(),
        circuit_breaker=CircuitBreakerConfig(enabled=False),
    )


@pytest.mark.asyncio
async def test_round_robin_distributes_evenly() -> None:
    lb = LoadBalancer(route("round_robin"), upstreams(), BreakerRegistry({}))
    names = []
    for _ in range(6):
        u = await lb.pick()
        names.append(u.name)
        await lb.release(u.name)
    assert sorted(names) == ["u0", "u0", "u1", "u1", "u2", "u2"]


@pytest.mark.asyncio
async def test_least_connections_prefers_idle() -> None:
    lb = LoadBalancer(route("least_connections"), upstreams(), BreakerRegistry({}))
    first = await lb.pick()                 # u0 被占用
    second = await lb.pick()                # 应选另一个空闲上游
    assert second.name != first.name
    counts = lb.active_counts()
    assert counts[first.name] == 1 and counts[second.name] == 1

    third = await lb.pick()                 # 最后一个空闲上游
    assert counts.get(third.name, 0) == 0 or third.name not in (first.name, second.name)
    await lb.release(first.name)
    fourth = await lb.pick()                # u0 已释放，连接数最少
    assert fourth.name == first.name
    for u in (second, third, fourth):
        await lb.release(u.name)


@pytest.mark.asyncio
async def test_exclude_tried_upstreams_during_retry() -> None:
    lb = LoadBalancer(route("round_robin"), upstreams(), BreakerRegistry({}))
    u1 = await lb.pick(exclude=set())
    await lb.release(u1.name)
    u2 = await lb.pick(exclude={u1.name})
    assert u2.name != u1.name
    await lb.release(u2.name)
    u3 = await lb.pick(exclude={u1.name, u2.name})
    assert u3.name not in {u1.name, u2.name}
    await lb.release(u3.name)


@pytest.mark.asyncio
async def test_open_breakers_are_skipped() -> None:
    cfg = CircuitBreakerConfig(
        enabled=True, min_requests=1, failure_rate=0.0,
        window_seconds=10, open_seconds=10,
    )
    registry = BreakerRegistry({"u0": cfg, "u1": cfg, "u2": cfg})
    lb = LoadBalancer(route("round_robin"), upstreams(), registry)

    # 让 u0 与 u1 跳闸（min_requests=1，一次失败即开）
    for name in ("u0", "u1"):
        b = registry.get(name)
        assert b is not None
        await b.allow()
        await b.record_failure()

    for _ in range(4):
        u = await lb.pick()
        assert u.name == "u2"               # OPEN 上游永远不被选中
        await lb.release(u.name)


@pytest.mark.asyncio
async def test_all_open_raises() -> None:
    cfg = CircuitBreakerConfig(
        enabled=True, min_requests=1, failure_rate=0.0,
        window_seconds=10, open_seconds=10,
    )
    registry = BreakerRegistry({"u0": cfg})
    single = [UpstreamConfig(name="u0", base_url="http://u0")]
    lb = LoadBalancer(
        RouteConfig(
            name="r", path="/r", upstreams=("u0",),
            retries=RetryConfig(),
            circuit_breaker=CircuitBreakerConfig(enabled=True),
        ),
        single, registry,
    )
    b = registry.get("u0")
    await b.allow()
    await b.record_failure()
    with pytest.raises(NoHealthyUpstreamError):
        await lb.pick()
