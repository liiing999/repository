"""测试夹具：在同一事件循环里启动 3 个 mock 上游 + 网关（均使用随机端口）。"""
from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable

import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer
from aiohttp.web import Application

from gateway.app import create_app
from gateway.config import (
    CircuitBreakerConfig,
    GatewayConfig,
    RetryConfig,
    RouteConfig,
    UpstreamConfig,
)
from mock_upstream.server import create_app as create_mock_app


@pytest_asyncio.fixture
async def mock_servers() -> AsyncIterator[list[TestServer]]:
    names = ["mock-a", "mock-b", "mock-c"]
    servers: list[TestServer] = []
    for name in names:
        server = TestServer(create_mock_app(name))
        await server.start_server()
        servers.append(server)
    yield servers
    for server in servers:
        await server.close()


def _url(server: TestServer) -> str:
    return f"http://127.0.0.1:{server.port}"


@pytest_asyncio.fixture
async def gateway_factory(
    mock_servers: list[TestServer],
) -> AsyncIterator[Callable[..., Awaitable[TestClient]]]:
    clients: list[TestClient] = []

    async def _factory(**route_overrides) -> TestClient:
        upstream_names = route_overrides.pop(
            "upstream_names", ("upstream-a", "upstream-b", "upstream-c")
        )
        all_names = ("upstream-a", "upstream-b", "upstream-c")
        upstreams = {
            name: UpstreamConfig(name=name, base_url=_url(server))
            for name, server in zip(all_names, mock_servers)
            if name in upstream_names
        }
        retry = route_overrides.pop(
            "retries",
            RetryConfig(max_attempts=3, backoff_base=0.02, backoff_max=0.2, jitter=0.0),
        )
        cb = route_overrides.pop(
            "circuit_breaker",
            CircuitBreakerConfig(
                window_seconds=5, min_requests=4, failure_rate=0.5,
                open_seconds=0.6, half_open_max_calls=1,
            ),
        )
        route = RouteConfig(
            name="demo", path="/demo",
            upstreams=upstream_names,
            strip_prefix=True, lb=route_overrides.pop("lb", "round_robin"),
            timeout=route_overrides.pop("timeout", 2.0),
            retries=retry, circuit_breaker=cb,
            rate_limit=route_overrides.pop("rate_limit", None),
        )
        extra_routes = route_overrides.pop("extra_routes", [])
        config = GatewayConfig(
            host="127.0.0.1", port=0, request_timeout=15.0, drain_timeout=5.0,
            log_level="DEBUG", log_json=True, trust_forwarded=True,
            upstreams=upstreams, routes=[route, *extra_routes],
            rate_limit_defaults=None, redis=None,
        )
        app: Application = create_app(config)
        client = TestClient(TestServer(app))
        await client.start_server()
        clients.append(client)
        return client

    yield _factory

    for client in clients:
        await client.close()
