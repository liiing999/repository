"""端到端集成测试：透传、轮询、重试、超时映射、故障熔断转移、限流、流式、request_id。"""
from __future__ import annotations

import asyncio

import aiohttp
from aiohttp.test_utils import TestClient

from gateway.config import (
    CircuitBreakerConfig,
    RateLimitConfig,
    RetryConfig,
)


def mock_url(mock_servers, upstream: str, path: str = "") -> str:
    idx = {"upstream-a": 0, "upstream-b": 1, "upstream-c": 2}[upstream]
    server = mock_servers[idx]
    return f"http://127.0.0.1:{server.port}{path}"


async def set_mode(mock_servers, upstream: str, mode: str) -> None:
    async with aiohttp.ClientSession() as s:
        async with s.post(mock_url(mock_servers, upstream, f"/__mode/{mode}")) as r:
            assert r.status == 200


# ---- 基础转发 ---------------------------------------------------------------

async def test_proxy_round_trip_and_header_passthrough(gateway_factory) -> None:
    client: TestClient = await gateway_factory()
    resp = await client.get(
        "/demo/echo?x=1",
        headers={"X-Custom-Header": "abc", "X-Request-Id": "req-fixed-1"},
    )
    assert resp.status == 200
    data = await resp.json()
    assert data["query"] == {"x": "1"}
    assert data["headers"]["X-Custom-Header"] == "abc"
    # request_id 透传到上游并原样回到客户端
    assert data["request_id"] == "req-fixed-1"
    assert resp.headers["x-request-id"] == "req-fixed-1"


async def test_request_body_is_forwarded(gateway_factory) -> None:
    client = await gateway_factory()
    payload = b'{"hello": "world"}'
    resp = await client.post("/demo/echo", data=payload, headers={"Content-Type": "application/json"})
    assert resp.status == 200
    data = await resp.json()
    assert data["body"] == '{"hello": "world"}'
    assert data["body_len"] == len(payload)


async def test_generated_request_id(gateway_factory) -> None:
    client = await gateway_factory()
    resp = await client.get("/demo/mirror")
    rid = resp.headers["x-request-id"]
    assert rid.startswith("gw-")
    data = await resp.json()
    assert data["request_id"] == rid


async def test_round_robin_distribution(gateway_factory) -> None:
    client = await gateway_factory()
    served = []
    for _ in range(6):
        data = await (await client.get("/demo/mirror")).json()
        served.append(data["served_by"])
    # mock 名称 -> 期望各 2 次
    assert sorted(served) == sorted(["mock-a", "mock-b", "mock-c"] * 2)


async def test_no_route_404(gateway_factory) -> None:
    client = await gateway_factory()
    resp = await client.get("/nope/x")
    assert resp.status == 404
    body = await resp.json()
    assert body["error"] == "no_route"


# ---- 重试 -------------------------------------------------------------------

async def test_retry_then_success_same_or_other_upstream(gateway_factory) -> None:
    # 单上游：第一次 503（fail=1），重试时同一上游已恢复 -> 200
    client = await gateway_factory(
        upstream_names=("upstream-a",),
        retries=RetryConfig(max_attempts=3, backoff_base=0.01, jitter=0.0),
    )
    resp = await client.get("/demo/fail-then-ok?fail=1&key=k1")
    assert resp.status == 200
    data = await resp.json()
    assert data["served_by"] == "mock-a"
    assert data["recovered"] is True


async def test_retry_shifts_to_healthy_upstream(gateway_factory, mock_servers) -> None:
    # 上游 a 持续故障：先打到 a，重试转移到健康的 b/c
    await set_mode(mock_servers, "upstream-a", "fail")
    client = await gateway_factory(
        retries=RetryConfig(max_attempts=3, backoff_base=0.01, jitter=0.0),
    )
    resp = await client.get("/demo/mirror")
    assert resp.status == 200
    data = await resp.json()
    assert data["served_by"] in {"mock-b", "mock-c"}
    await set_mode(mock_servers, "upstream-a", "mirror")


async def test_all_attempts_fail_returns_503(gateway_factory, mock_servers) -> None:
    for up in ("upstream-a", "upstream-b", "upstream-c"):
        await set_mode(mock_servers, up, "fail")
    client = await gateway_factory(
        retries=RetryConfig(max_attempts=3, backoff_base=0.01, jitter=0.0),
    )
    resp = await client.get("/demo/mirror")
    assert resp.status == 503
    body = await resp.json()
    assert body["error"] == "upstream_failure"
    assert body["retry_count"] == 2
    # 恢复，避免影响其它测试（fixture 每测试重建，保险起见）
    for up in ("upstream-a", "upstream-b", "upstream-c"):
        await set_mode(mock_servers, up, "mirror")


async def test_retryable_status_config(gateway_factory, mock_servers) -> None:
    # 404 不在重试列表中：直接透传，不重试
    client = await gateway_factory(
        retries=RetryConfig(max_attempts=3, backoff_base=0.01, jitter=0.0,
                            retry_on_status=(502, 503, 504)),
    )
    resp = await client.get("/demo/status/404")
    assert resp.status == 404
    body = await resp.json()
    assert body["status"] == 404


# ---- 超时 -------------------------------------------------------------------

async def test_upstream_timeout_maps_to_504(gateway_factory, mock_servers) -> None:
    await set_mode(mock_servers, "upstream-a", "timeout")
    await set_mode(mock_servers, "upstream-b", "timeout")
    await set_mode(mock_servers, "upstream-c", "timeout")
    client = await gateway_factory(
        timeout=0.5,
        retries=RetryConfig(max_attempts=2, backoff_base=0.01, jitter=0.0),
    )
    resp = await client.get("/demo/mirror")
    assert resp.status == 504
    body = await resp.json()
    assert body["error"] == "upstream_failure"
    for up in ("upstream-a", "upstream-b", "upstream-c"):
        await set_mode(mock_servers, up, "mirror")


async def test_slow_within_timeout_is_ok(gateway_factory) -> None:
    client = await gateway_factory(timeout=2.0)
    resp = await client.get("/demo/slow?delay=0.2")
    assert resp.status == 200
    data = await resp.json()
    assert data["delayed"] == 0.2


# ---- 熔断与故障转移 ----------------------------------------------------------

async def test_circuit_breaker_opens_and_shifts_traffic(gateway_factory, mock_servers) -> None:
    """一个上游持续故障：失败次数达到阈值后熔断，流量全部转到健康上游。"""
    await set_mode(mock_servers, "upstream-a", "fail")
    client = await gateway_factory(
        retries=RetryConfig(max_attempts=2, backoff_base=0.01, jitter=0.0),
        circuit_breaker=CircuitBreakerConfig(
            window_seconds=5, min_requests=4, failure_rate=0.5,
            open_seconds=5, half_open_max_calls=1,
        ),
    )
    # 前若干请求会因"先打到 a 再重试到 b/c"而成功，同时把 a 的失败率推过阈值
    for _ in range(8):
        resp = await client.get("/demo/mirror")
        assert resp.status in (200, 503)

    state = await (await client.get("/__admin/state")).json()
    breaker_a = next(b for b in state["breakers"] if b["upstream"] == "upstream-a")
    assert breaker_a["state"] == "open"

    # 熔断后：请求不再发往 a，全部由 b/c 处理
    served = set()
    for _ in range(8):
        resp = await client.get("/demo/mirror")
        assert resp.status == 200
        served.add((await resp.json())["served_by"])
    assert served <= {"mock-b", "mock-c"}
    assert "mock-a" not in served

    await set_mode(mock_servers, "upstream-a", "mirror")


async def test_all_upstreams_open_returns_503(gateway_factory, mock_servers) -> None:
    for up in ("upstream-a", "upstream-b", "upstream-c"):
        await set_mode(mock_servers, up, "fail")
    client = await gateway_factory(
        retries=RetryConfig(max_attempts=1, jitter=0.0),
        circuit_breaker=CircuitBreakerConfig(
            window_seconds=5, min_requests=1, failure_rate=0.5,
            open_seconds=5, half_open_max_calls=1,
        ),
    )
    statuses = []
    for _ in range(6):
        statuses.append((await client.get("/demo/mirror")).status)
    # 前 3 次为 503（真实打到 3 个上游并熔断），之后为熔断拒绝
    assert 503 in statuses
    later = await client.get("/demo/mirror")
    assert later.status == 503
    body = await later.json()
    assert body["error"] == "upstream_unavailable"
    for up in ("upstream-a", "upstream-b", "upstream-c"):
        await set_mode(mock_servers, up, "mirror")


async def test_half_open_probe_recovers(gateway_factory, mock_servers) -> None:
    await set_mode(mock_servers, "upstream-a", "fail")
    client = await gateway_factory(
        upstream_names=("upstream-a",),
        retries=RetryConfig(max_attempts=1, jitter=0.0),
        circuit_breaker=CircuitBreakerConfig(
            window_seconds=5, min_requests=2, failure_rate=0.5,
            open_seconds=0.5, half_open_max_calls=1,
        ),
    )
    for _ in range(3):
        assert (await client.get("/demo/mirror")).status == 503
    state = await (await client.get("/__admin/state")).json()
    assert state["breakers"][0]["state"] == "open"

    # 熔断期间立即请求 -> 503 upstream_unavailable，不打上游
    resp = await client.get("/demo/mirror")
    assert resp.status == 503
    assert (await resp.json())["error"] == "upstream_unavailable"

    # 上游恢复 + 冷却结束 -> HALF_OPEN 试探成功 -> CLOSED
    await set_mode(mock_servers, "upstream-a", "mirror")
    await asyncio.sleep(0.7)
    resp = await client.get("/demo/mirror")
    assert resp.status == 200
    state = await (await client.get("/__admin/state")).json()
    assert state["breakers"][0]["state"] == "closed"


# ---- 限流 -------------------------------------------------------------------

async def test_rate_limit_429(gateway_factory) -> None:
    client = await gateway_factory(
        rate_limit=RateLimitConfig(capacity=5, refill_per_second=1),
    )
    resps = [await client.get("/demo/mirror") for _ in range(8)]
    statuses = [r.status for r in resps]
    assert statuses.count(200) == 5
    assert statuses.count(429) == 3
    # 被拒响应带有 Retry-After
    assert next(r for r in resps if r.status == 429).headers["Retry-After"]

    state = await (await client.get("/__admin/state")).json()
    rl = state["rate_limits"][0]
    assert rl["rejected"] == 3


async def test_rate_limit_refill_allows_more(gateway_factory) -> None:
    client = await gateway_factory(
        rate_limit=RateLimitConfig(capacity=2, refill_per_second=10),
    )
    first = [(await client.get("/demo/mirror")).status for _ in range(2)]
    assert first == [200, 200]
    assert (await client.get("/demo/mirror")).status == 429
    await asyncio.sleep(0.25)   # 补充约 2.5 个令牌
    assert (await client.get("/demo/mirror")).status == 200


# ---- 流式 -------------------------------------------------------------------

async def test_streaming_response_passes_chunks(gateway_factory) -> None:
    client = await gateway_factory()
    resp = await client.get("/demo/stream?chunks=4&delay=0.02")
    assert resp.status == 200
    lines = []
    async for line in resp.content:
        lines.append(line)
    assert len(lines) == 4
    assert b'"seq": 3' in lines[-1]
    assert resp.headers["x-served-by"].startswith("mock-")
