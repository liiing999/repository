"""aiohttp 应用装配与优雅退出。

优雅退出流程（SIGTERM/SIGINT）：
1. 置 ``drain_event``：中间件开始对新请求返回 503；
2. 停止监听 socket（不再接受新连接）；
3. 等待在途请求排空，最多 ``drain_timeout``；
4. 超时后强制清理（取消仍未完成的在途请求）、关闭上游连接池。
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import signal

import aiohttp
from aiohttp import web

from .config import GatewayConfig, load_config
from .logging_setup import configure_logging
from .proxy import ProxyRuntime, build_rate_limiter

HEALTH_PATH = "/healthz"
ADMIN_STATE_PATH = "/__admin/state"


async def _healthz(request: web.Request) -> web.Response:
    runtime: ProxyRuntime = request.app["runtime"]
    return web.json_response(
        {"status": "draining" if runtime.drain_event.is_set() else "ok",
         "inflight": runtime.inflight}
    )


async def _admin_state(request: web.Request) -> web.Response:
    runtime: ProxyRuntime = request.app["runtime"]
    return web.json_response(
        {
            "inflight": runtime.inflight,
            "draining": runtime.drain_event.is_set(),
            "breakers": [
                {
                    "upstream": s.upstream, "state": s.state,
                    "failure_rate": s.failure_rate, "window_total": s.total,
                }
                for s in runtime.breakers.snapshots()
            ],
            "rate_limits": [
                {
                    "route": s.route, "backend": s.backend,
                    "capacity": s.capacity, "refill_per_second": s.refill_per_second,
                    "allowed": s.allowed, "rejected": s.rejected, "tokens": s.tokens,
                }
                for s in await _snapshot_rate_limits(runtime)
            ],
            "active_connections": {
                name: lb.active_counts()
                for name, lb in runtime.load_balancers.items()
            },
        }
    )


async def _snapshot_rate_limits(runtime: ProxyRuntime):
    # snapshot() 当前为同步方法，预留 await 以兼容未来的远程实现
    return runtime.rate_limiter.snapshot()


async def _on_shutdown(app: web.Application) -> None:
    runtime: ProxyRuntime = app["runtime"]
    app.logger.info("shutdown_signal_received", extra={"inflight": runtime.inflight})
    runtime.drain_event.set()
    try:
        await asyncio.wait_for(runtime.wait_drained(), timeout=runtime.config.drain_timeout)
        app.logger.info("drain_complete")
    except TimeoutError:
        app.logger.warning(
            "drain_timeout_forcing_exit",
            extra={"inflight": runtime.inflight,
                   "drain_timeout": runtime.config.drain_timeout},
        )


async def _on_cleanup(app: web.Application) -> None:
    runtime: ProxyRuntime = app["runtime"]
    await runtime.rate_limiter.aclose()
    await app["client_session"].close()


def create_app(config: GatewayConfig) -> web.Application:
    app = web.Application(
        middlewares=[],
        client_max_size=64 * 1024 * 1024,
    )
    # 中间件顺序：request_id 上下文 -> 在途计数/排空拦截 -> 兜底错误
    drain_event = asyncio.Event()

    async def _init_runtime(application: web.Application) -> None:
        connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
        session = aiohttp.ClientSession(connector=connector)
        application["client_session"] = session
        application["runtime"] = ProxyRuntime(
            config, session, build_rate_limiter(config), drain_event
        )

    app.on_startup.append(_init_runtime)

    @web.middleware
    async def _bound_context(request: web.Request, handler):
        runtime = request.app["runtime"]
        return await runtime.context_middleware(request, handler)

    @web.middleware
    async def _bound_inflight(request: web.Request, handler):
        runtime = request.app["runtime"]
        return await runtime.inflight_middleware(request, handler)

    @web.middleware
    async def _bound_errors(request: web.Request, handler):
        runtime = request.app["runtime"]
        return await runtime.error_middleware(request, handler)

    app.middlewares.append(_bound_context)
    app.middlewares.append(_bound_inflight)
    app.middlewares.append(_bound_errors)

    app.router.add_get(HEALTH_PATH, _healthz)
    app.router.add_get(ADMIN_STATE_PATH, _admin_state)
    # 其余全部路径走代理分发
    app.router.add_route("*", "/{path:.*}", _dispatch)

    app.on_shutdown.append(_on_shutdown)
    app.on_cleanup.append(_on_cleanup)
    app["config"] = config
    return app


async def _dispatch(request: web.Request) -> web.StreamResponse:
    runtime: ProxyRuntime = request.app["runtime"]
    return await runtime.dispatch(request)


async def serve(config: GatewayConfig) -> None:
    configure_logging(config.log_level, config.log_json)
    app = create_app(config)

    runner = web.AppRunner(
        app, access_log=None, shutdown_timeout=config.drain_timeout
    )
    await runner.setup()
    site = web.TCPSite(runner, config.host, config.port)
    await site.start()
    app.logger.info(
        "gateway_started",
        extra={"host": config.host, "port": config.port,
               "routes": [r.path for r in config.routes]},
    )

    loop = asyncio.get_running_loop()
    stopping = asyncio.Event()

    def _request_stop() -> None:
        if not stopping.is_set():
            stopping.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):  # Windows 上部分信号不可用
            loop.add_signal_handler(sig, _request_stop)

    try:
        await stopping.wait()
    finally:
        # site.stop() 停止接收新连接；on_shutdown 负责排空；cleanup 处理残余
        await site.stop()
        await runner.cleanup()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="轻量 API 网关")
    parser.add_argument("-c", "--config", default="config.yaml", help="YAML 配置文件路径")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    try:
        asyncio.run(serve(config))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
