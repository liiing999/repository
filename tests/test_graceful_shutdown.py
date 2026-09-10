"""优雅退出：排空期间拒绝新请求、在途请求完成后再退出。"""
from __future__ import annotations

import asyncio
import time

import aiohttp
import pytest
from aiohttp import web

from gateway.app import create_app
from gateway.config import (
    CircuitBreakerConfig,
    GatewayConfig,
    RetryConfig,
    RouteConfig,
    UpstreamConfig,
)
from aiohttp.test_utils import TestClient


def _config(base_url: str) -> GatewayConfig:
    return GatewayConfig(
        host="127.0.0.1", port=0, request_timeout=15.0, drain_timeout=5.0,
        log_level="INFO", log_json=True, trust_forwarded=True,
        upstreams={"u": UpstreamConfig(name="u", base_url=base_url)},
        routes=[
            RouteConfig(
                name="demo", path="/demo", upstreams=("u",), strip_prefix=True,
                timeout=5.0,
                retries=RetryConfig(max_attempts=1),
                circuit_breaker=CircuitBreakerConfig(enabled=False),
            )
        ],
        rate_limit_defaults=None, redis=None,
    )


async def test_draining_rejects_new_requests(gateway_factory) -> None:
    client: TestClient = await gateway_factory()
    app = client.server.app
    app["runtime"].drain_event.set()
    resp = await client.get("/demo/mirror")
    assert resp.status == 503
    body = await resp.json()
    assert body["error"] == "gateway_draining"
    # 健康检查暴露 draining 状态
    health = await (await client.get("/healthz")).json()
    assert health["status"] == "draining"


async def test_inflight_requests_are_drained_before_exit(mock_servers) -> None:
    """完整生命周期：关闭开始后，在途慢请求必须完成，runner.cleanup 才返回。"""
    port = mock_servers[0].port
    base_url = f"http://127.0.0.1:{port}"
    app = create_app(_config(base_url))

    runner = web.AppRunner(app, access_log=None, shutdown_timeout=5.0)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    bound_port = runner.addresses[0][1]

    async with aiohttp.ClientSession() as session:
        started = time.monotonic()
        slow_task = asyncio.create_task(
            session.get(f"http://127.0.0.1:{bound_port}/demo/slow?delay=1.0")
        )
        await asyncio.sleep(0.3)             # 确保请求已经在途
        assert app["runtime"].inflight == 1

        cleanup_task = asyncio.create_task(site.stop())
        await asyncio.wait_for(cleanup_task, timeout=3)   # 停止监听
        # runner.cleanup 触发 on_shutdown 排空
        await asyncio.wait_for(runner.cleanup(), timeout=5)

        elapsed = time.monotonic() - started
        assert elapsed >= 0.9               # 确实等到了慢请求完成
        resp = await slow_task
        assert resp.status == 200
        data = await resp.json()
        assert data["delayed"] == 1.0


async def test_drain_timeout_forces_exit(mock_servers) -> None:
    """超过 drain_timeout 仍有在途请求时，清理也必须按时返回（不永久挂起）。"""
    port = mock_servers[0].port
    cfg = _config(f"http://127.0.0.1:{port}")
    object.__setattr__(cfg, "drain_timeout", 0.3)
    app = create_app(cfg)
    runner = web.AppRunner(app, access_log=None, shutdown_timeout=0.3)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    bound_port = runner.addresses[0][1]

    async with aiohttp.ClientSession() as session:
        slow_task = asyncio.create_task(
            session.get(f"http://127.0.0.1:{bound_port}/demo/slow?delay=3.0")
        )
        await asyncio.sleep(0.3)
        await site.stop()
        t0 = time.monotonic()
        await runner.cleanup()
        assert time.monotonic() - t0 < 2.0  # drain_timeout 后强制退出
        slow_task.cancel()
        with pytest.raises((asyncio.CancelledError, aiohttp.ClientError)):
            await slow_task
