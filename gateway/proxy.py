"""请求代理核心：路由匹配、限流、上游选择、重试转发、流式响应、错误映射。"""
from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any

import aiohttp
from aiohttp import web

from .circuitbreaker import BreakerRegistry
from .config import GatewayConfig, RouteConfig, UpstreamConfig
from .loadbalancer import LoadBalancer, NoHealthyUpstreamError
from .logging_setup import request_context
from .ratelimit import InMemoryRateLimiter, RateLimiter, RedisRateLimiter
from .retry import backoff_delay

# RFC 7230 逐跳头 + Host/Length（由 aiohttp 按实际请求重新生成）
HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "host", "content-length",
}
FORWARD_PREFIX = "x-forwarded-"
REQUEST_ID_HEADER = "x-request-id"

# 各类失败映射给客户端的状态码
STATUS_TIMEOUT = 504
STATUS_UPSTREAM_ERROR = 502
STATUS_NO_UPSTREAM = 503


class UpstreamAttemptError(Exception):
    def __init__(self, kind: str, status: int, detail: str) -> None:
        super().__init__(detail)
        self.kind = kind          # timeout | connect | breaker
        self.status = status


class ProxyRuntime:
    """网关运行时：共享的 session、熔断器、负载均衡器、限流器、在途计数。"""

    def __init__(
        self,
        config: GatewayConfig,
        session: aiohttp.ClientSession,
        rate_limiter: RateLimiter,
        drain_event: asyncio.Event | None = None,
    ) -> None:
        self.config = config
        self.session = session
        self.rate_limiter = rate_limiter
        self.drain_event = drain_event or asyncio.Event()
        self.inflight = 0
        self._inflight_zero = asyncio.Event()
        self._inflight_zero.set()

        # 熔断器：优先上游级配置，否则用默认 enabled=False 的配置 + 路由默认
        self.breakers = self._build_breakers()
        self.load_balancers = {
            route.name: LoadBalancer(
                route, [config.upstreams[n] for n in route.upstreams], self.breakers
            )
            for route in config.routes
        }
        self._routes_sorted = sorted(config.routes, key=lambda r: len(r.path), reverse=True)

    def _build_breakers(self) -> BreakerRegistry:
        # 任意路由启用了熔断的上游都需要一个 breaker；上游级配置优先
        breaker_cfgs: dict[str, Any] = {}
        for route in self.config.routes:
            if route.circuit_breaker.enabled:
                for name in route.upstreams:
                    breaker_cfgs.setdefault(name, route.circuit_breaker)
        for name, ups in self.config.upstreams.items():
            if ups.circuit_breaker is not None and ups.circuit_breaker.enabled:
                breaker_cfgs[name] = ups.circuit_breaker
        return BreakerRegistry(breaker_cfgs)

    def match_route(self, path: str) -> RouteConfig | None:
        for route in self._routes_sorted:
            if path == route.path or path.startswith(route.path.rstrip("/") + "/"):
                return route
        return None

    def enter_inflight(self) -> None:
        self.inflight += 1
        self._inflight_zero.clear()

    def leave_inflight(self) -> None:
        self.inflight -= 1
        if self.inflight == 0:
            self._inflight_zero.set()

    async def wait_drained(self) -> None:
        if self.inflight == 0:
            return
        await self._inflight_zero.wait()

    # ---- 中间件与入口 -----------------------------------------------------

    @web.middleware
    async def context_middleware(
        self, request: web.Request, handler: Any
    ) -> web.StreamResponse:
        incoming_id = request.headers.get(REQUEST_ID_HEADER)
        request_id = incoming_id or f"gw-{uuid.uuid4().hex}"
        request["request_id"] = request_id
        token = request_context.set({"request_id": request_id})
        try:
            response = await handler(request)
            # StreamResponse 可能已在处理器内 prepare（流式转发），那时不能再改头；
            # 那种情况由 _stream_response 自己写入 request_id
            if getattr(response, "prepared", None) is None:
                response.headers[REQUEST_ID_HEADER] = request_id
            return response
        finally:
            request_context.reset(token)

    @web.middleware
    async def inflight_middleware(
        self, request: web.Request, handler: Any
    ) -> web.StreamResponse:
        # 可观测端点在排空阶段仍然可用（用于确认 draining 状态）
        if request.path in ("/healthz", "/__admin/state"):
            return await handler(request)
        # 排空阶段直接拒绝新请求
        if self.drain_event.is_set():
            return self._error_response(
                STATUS_NO_UPSTREAM, "gateway_draining",
                "网关正在关闭，停止接收新请求", request,
            )
        self.enter_inflight()
        try:
            return await handler(request)
        finally:
            self.leave_inflight()

    @web.middleware
    async def error_middleware(
        self, request: web.Request, handler: Any
    ) -> web.StreamResponse:
        try:
            return await handler(request)
        except asyncio.CancelledError:
            raise
        except web.HTTPException:
            raise
        except Exception:  # noqa: BLE001 - 兜底，避免未处理异常穿透成 500 空响应
            request.app.logger.exception("unhandled_error", extra={"route": request.get("route")})
            return self._error_response(500, "internal_error", "网关内部错误", request)

    # ---- 转发主流程 -------------------------------------------------------

    async def dispatch(self, request: web.Request) -> web.StreamResponse:
        route = self.match_route(request.path)
        if route is None:
            return self._error_response(404, "no_route", f"无匹配路由: {request.path}", request)
        request["route"] = route.name

        # 1) 每路由独立令牌桶限流
        if route.rate_limit is not None:
            allowed = await self.rate_limiter.acquire(route.name)
            if not allowed:
                request.app.logger.info("rate_limited", extra={"route": route.name})
                resp = self._error_response(
                    429, "rate_limited", "请求超过路由限流，请稍后重试", request
                )
                resp.headers["Retry-After"] = str(
                    max(1, round(1 / max(route.rate_limit.refill_per_second, 0.001)))
                )
                return resp

        body = await request.read()  # 缓存请求体以支持安全重试（见 README 取舍说明）
        started = time.monotonic()

        budget = self.config.request_timeout
        try:
            # 预算只覆盖"拿到上游响应头"阶段（含全部重试/退避）；之后进入流式转发
            async with asyncio.timeout(budget):
                plan = await self._attempt_loop(request, route, body, started)
        except TimeoutError:
            request.app.logger.warning(
                "request_budget_timeout",
                extra={"route": route.name, "budget": budget},
            )
            return self._error_response(
                STATUS_TIMEOUT, "gateway_timeout",
                f"请求超过整体预算 {budget}s", request,
            )

        if isinstance(plan, web.StreamResponse):
            return plan
        upstream, response, retry_count = plan
        # 响应头已经就绪，之后的流式中断无法再改写状态码/重试
        return await self._stream_response(
            request, route, upstream, response, started, retry_count
        )

    async def _attempt_loop(
        self,
        request: web.Request,
        route: RouteConfig,
        body: bytes,
        started: float,
    ) -> web.StreamResponse | tuple[UpstreamConfig, aiohttp.ClientResponse, int]:
        """返回错误响应，或 (上游, 上游响应, 已重试次数) 供流式阶段使用。"""
        tried: set[str] = set()
        retry_count = 0
        last_error: UpstreamAttemptError | None = None
        for attempt in range(1, route.retries.max_attempts + 1):
            if self.drain_event.is_set():
                return self._error_response(
                    STATUS_NO_UPSTREAM, "gateway_draining", "网关正在关闭", request
                )
            try:
                upstream = await self.load_balancers[route.name].pick(exclude=tried)
            except NoHealthyUpstreamError:
                request.app.logger.warning(
                    "all_upstreams_unavailable",
                    extra={"route": route.name, "attempt": attempt},
                )
                return self._error_response(
                    STATUS_NO_UPSTREAM, "upstream_unavailable",
                    "所有上游暂不可用（熔断中）", request,
                )

            try:
                response = await self._forward_once(
                    request, route, upstream, body, attempt, started
                )
            except UpstreamAttemptError as exc:
                last_error = exc
                await self._record_attempt(upstream.name, success=False)
                await self.load_balancers[route.name].release(upstream.name)
                tried.add(upstream.name)

                if attempt >= route.retries.max_attempts:
                    break
                retry_index = attempt  # 第 1 次失败后做第 1 次重试
                delay = backoff_delay(route.retries, retry_index)
                request.app.logger.warning(
                    "upstream_retry",
                    extra={
                        "route": route.name, "upstream": upstream.name,
                        "attempt": attempt, "error": exc.kind,
                        "retry_in_ms": round(delay * 1000, 1),
                    },
                )
                retry_count += 1
                if not await self._sleep_or_drain(delay):
                    return self._error_response(
                        STATUS_NO_UPSTREAM, "gateway_draining", "网关正在关闭", request
                    )
                continue

            # 响应头可用：LC 计数在这里释放（熔断成败在流结束时记录）
            await self.load_balancers[route.name].release(upstream.name)
            return upstream, response, retry_count

        # 所有尝试均失败
        total_ms = round((time.monotonic() - started) * 1000, 1)
        status = last_error.status if last_error else STATUS_UPSTREAM_ERROR
        request.app.logger.error(
            "request_failed",
            extra={
                "route": route.name, "status": status,
                "retry_count": retry_count, "latency_ms": total_ms,
                "last_error": last_error.kind if last_error else "unknown",
            },
        )
        return self._error_response(
            status, "upstream_failure",
            f"上游请求失败（重试 {retry_count} 次）", request,
            retry_count=retry_count,
        )

    async def _forward_once(
        self,
        request: web.Request,
        route: RouteConfig,
        upstream: UpstreamConfig,
        body: bytes,
        attempt: int,
        started: float,
    ) -> aiohttp.ClientResponse:
        path = request.path
        if route.strip_prefix:
            path = path[len(route.path.rstrip("/")):] or "/"
        url = upstream.base_url + path
        if request.query_string:
            url = f"{url}?{request.query_string}"

        headers: dict[str, str] = {}
        for key, value in request.raw_headers:
            name = key.decode("latin-1").lower()
            if name in HOP_BY_HOP or name == REQUEST_ID_HEADER or name.startswith(FORWARD_PREFIX):
                continue
            headers[key.decode("latin-1")] = value.decode("latin-1")
        headers[REQUEST_ID_HEADER] = request["request_id"]
        if self.config.trust_forwarded:
            fwd_for = request.headers.get("x-forwarded-for", "")
            peer = request.remote or ""
            headers["X-Forwarded-For"] = (
                f"{fwd_for}, {peer}".strip(", ") if fwd_for else peer
            )
            headers["X-Forwarded-Proto"] = request.scheme
            host = request.headers.get("host", "")
            if host:
                headers["X-Forwarded-Host"] = host
        # 上游级固定头（配置中以 ${ENV} 引用凭据，运行时从环境注入）
        headers.update(upstream.headers)

        timeout = aiohttp.ClientTimeout(total=upstream.timeout or route.timeout)
        attempt_started = time.monotonic()
        log = request.app.logger
        try:
            # 直接 await 请求上下文即得到响应；释放责任在调用方（resp.release()）
            resp = await self.session.request(
                request.method, url, headers=headers, data=body or None,
                allow_redirects=False, timeout=timeout,
            )
        except asyncio.TimeoutError:
            log.warning(
                "upstream_timeout",
                extra={"route": route.name, "upstream": upstream.name, "attempt": attempt},
            )
            raise UpstreamAttemptError("timeout", STATUS_TIMEOUT, "上游超时")
        except aiohttp.ClientError as exc:
            log.warning(
                "upstream_connect_error",
                extra={
                    "route": route.name, "upstream": upstream.name,
                    "attempt": attempt, "error": repr(exc),
                },
            )
            raise UpstreamAttemptError("connect", STATUS_UPSTREAM_ERROR, str(exc))

        attempt_ms = round((time.monotonic() - attempt_started) * 1000, 1)
        log.info(
            "upstream_response",
            extra={
                "route": route.name, "upstream": upstream.name, "attempt": attempt,
                "upstream_status": resp.status, "latency_ms": attempt_ms,
            },
        )
        if resp.status in route.retries.retry_on_status:
            # 读完丢弃错误体，安全释放该次响应再重试
            await resp.read()
            resp.release()
            raise UpstreamAttemptError(
                "http_5xx", resp.status, f"上游返回 {resp.status}"
            )
        return resp

    async def _stream_response(
        self,
        request: web.Request,
        route: RouteConfig,
        upstream: UpstreamConfig,
        upstream_resp: aiohttp.ClientResponse,
        started: float,
        retry_count: int,
    ) -> web.StreamResponse:
        out = web.StreamResponse(status=upstream_resp.status)
        for key, value in upstream_resp.raw_headers:
            name = key.decode("latin-1").lower()
            if name in HOP_BY_HOP or name == REQUEST_ID_HEADER:
                continue
            out.headers.add(key.decode("latin-1"), value.decode("latin-1"))
        # 不复制 content-length：按 chunked 流式写出，避免上游长度口径不一致
        out.headers.popall("Content-Length", None)
        # 上游回传的同名头已被剔除，这里写入本网关的 request_id（prepare 前完成）
        out.headers[REQUEST_ID_HEADER] = request["request_id"]

        stream_failed = False
        client_gone = False
        try:
            await out.prepare(request)
            async for chunk in upstream_resp.content.iter_chunked(64 * 1024):
                await out.write(chunk)
            await out.write_eof()
        except (aiohttp.ClientError, asyncio.TimeoutError):
            # 上游响应体传输中断 / 截断（响应头已发出，无法再重试或改写状态码）
            stream_failed = True
        except (ConnectionResetError, BrokenPipeError):
            # 客户端提前断开：上游本身无过，不计入熔断
            client_gone = True
        finally:
            upstream_resp.release()

        if client_gone:
            request.app.logger.info(
                "client_disconnected",
                extra={"route": route.name, "upstream": upstream.name},
            )
        elif stream_failed:
            await self._record_attempt(upstream.name, success=False)
            request.app.logger.warning(
                "upstream_stream_broken",
                extra={"route": route.name, "upstream": upstream.name},
            )
        else:
            await self._record_attempt(upstream.name, success=True)

        request.app.logger.info(
            "request_complete",
            extra={
                "route": route.name, "upstream": upstream.name,
                "status": upstream_resp.status, "retry_count": retry_count,
                "latency_ms": round((time.monotonic() - started) * 1000, 1),
            },
        )
        return out

    async def _record_attempt(self, upstream_name: str, *, success: bool) -> None:
        breaker = self.breakers.get(upstream_name)
        if breaker is None:
            return
        if success:
            await breaker.record_success()
        else:
            await breaker.record_failure()

    async def _sleep_or_drain(self, delay: float) -> bool:
        """退避等待；排空开始则立即中断。返回 False 表示应放弃请求。"""
        try:
            await asyncio.wait_for(self.drain_event.wait(), timeout=delay)
            return False
        except TimeoutError:
            # 正常等完退避时间（drain 事件未置位）
            return not self.drain_event.is_set()

    # ---- 工具 -------------------------------------------------------------

    def _error_response(
        self,
        status: int,
        error: str,
        message: str,
        request: web.Request | None = None,
        **extra: Any,
    ) -> web.Response:
        payload: dict[str, Any] = {"error": error, "message": message}
        if request is not None:
            payload["request_id"] = request.get("request_id")
            route = request.get("route")
            if route:
                payload["route"] = route
        payload.update(extra)
        return web.json_response(payload, status=status)


def build_rate_limiter(config: GatewayConfig) -> RateLimiter:
    redis_routes = {
        r.name: r.rate_limit
        for r in config.routes
        if r.rate_limit is not None and r.rate_limit.backend == "redis"
    }
    if redis_routes:
        if config.redis is None:
            raise ValueError("存在 backend=redis 的限流路由，但未配置顶层 redis 段")
        return RedisRateLimiter(config.redis, redis_routes)

    limiter = InMemoryRateLimiter()
    for route in config.routes:
        if route.rate_limit is not None:
            limiter.register(route.name, route.rate_limit)
    return limiter
