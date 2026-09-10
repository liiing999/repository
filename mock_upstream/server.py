"""可编排的 mock 上游：用于测试重试、熔断、超时、流式转发。

行为可通过环境变量给默认值，也可在运行时用控制端点改变（集成测试用）：
- GET  /mirror            正常 200，返回自身名称与收到的 request_id
- ALL  /echo              原样返回请求头/查询/体（验证透传）
- GET  /status/{code}     返回指定状态码
- GET  /slow?delay=秒      延迟后 200（模拟慢响应）
- GET  /timeout           睡眠到天荒地老（由客户端超时断开）
- GET  /fail-then-ok?fail=N  前 N 次 503，之后 200（验证重试/故障转移）
- GET  /stream?chunks=N&delay=秒  分块流式输出
- POST /__mode/{name}     切换默认模式（mirror|fail|timeout|slow）
所有响应都带 x-served-by 与收到的 x-request-id，便于验证 request_id 贯穿。
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os

from aiohttp import web

REQUEST_ID_HEADER = "x-request-id"


class MockState:
    def __init__(self, name: str, auth_token: str | None = None) -> None:
        self.name = name
        self.auth_token = auth_token
        self.mode = os.environ.get("MOCK_MODE", "mirror")
        self.fail_counts: dict[str, int] = {}   # 路径键 -> 剩余失败次数
        self.request_log: list[dict] = []

    def check_auth(self, request: web.Request) -> str | None:
        if not self.auth_token:
            return None
        supplied = request.headers.get("authorization", "")
        if supplied != f"Bearer {self.auth_token}":
            return "凭据无效或缺失（期望 Authorization: Bearer $UPSTREAM_TOKEN）"
        return None

    def base_headers(self, request: web.Request) -> dict[str, str]:
        return {
            "x-served-by": self.name,
            REQUEST_ID_HEADER: request.headers.get(REQUEST_ID_HEADER, "-"),
        }


async def _mirror(request: web.Request) -> web.Response:
    state: MockState = request.app["state"]
    if err := state.check_auth(request):
        return web.json_response({"error": "unauthorized", "message": err}, status=401)
    body = await request.read()
    return web.json_response(
        {
            "served_by": state.name,
            "request_id": request.headers.get(REQUEST_ID_HEADER),
            "method": request.method,
            "path": request.path,
            "query": dict(request.query),
            "headers": {k: v for k, v in request.headers.items()},
            "body": body.decode("utf-8", errors="replace"),
            "body_len": len(body),
        },
        headers=state.base_headers(request),
    )


async def _status(request: web.Request) -> web.Response:
    state: MockState = request.app["state"]
    code = int(request.match_info["code"])
    return web.json_response(
        {"error": "mock_error", "served_by": state.name, "status": code},
        status=code,
        headers=state.base_headers(request),
    )


async def _slow(request: web.Request) -> web.Response:
    state: MockState = request.app["state"]
    delay = float(request.query.get("delay", "2"))
    await asyncio.sleep(delay)
    return web.json_response(
        {"served_by": state.name, "delayed": delay,
         "request_id": request.headers.get(REQUEST_ID_HEADER)},
        headers=state.base_headers(request),
    )


async def _timeout(request: web.Request) -> web.Response:  # pragma: no cover - 由客户端断开
    await asyncio.sleep(3600)
    return web.json_response({"ok": True})


async def _fail_then_ok(request: web.Request) -> web.Response:
    state: MockState = request.app["state"]
    key = request.query.get("key", request.path)
    remaining = state.fail_counts.get(key)
    if remaining is None:
        remaining = int(request.query.get("fail", "1"))
    if remaining > 0:
        state.fail_counts[key] = remaining - 1
        return web.json_response(
            {"error": "mock_503", "served_by": state.name, "failures_left": remaining - 1},
            status=503,
            headers=state.base_headers(request),
        )
    state.fail_counts.pop(key, None)
    return web.json_response(
        {"served_by": state.name, "recovered": True,
         "request_id": request.headers.get(REQUEST_ID_HEADER)},
        headers=state.base_headers(request),
    )


async def _stream(request: web.Request) -> web.StreamResponse:
    state: MockState = request.app["state"]
    chunks = int(request.query.get("chunks", "5"))
    delay = float(request.query.get("delay", "0.1"))
    resp = web.StreamResponse(headers=state.base_headers(request))
    await resp.prepare(request)
    for i in range(chunks):
        await asyncio.sleep(delay)
        await resp.write((json.dumps({"seq": i, "served_by": state.name}) + "\n").encode())
    await resp.write_eof()
    return resp


async def _set_mode(request: web.Request) -> web.Response:
    state: MockState = request.app["state"]
    mode = request.match_info["mode"]
    if mode not in ("mirror", "fail", "timeout", "slow"):
        return web.json_response({"error": "bad_mode"}, status=400)
    state.mode = mode
    return web.json_response({"mode": mode})


async def _healthz(request: web.Request) -> web.Response:
    state: MockState = request.app["state"]
    return web.json_response({"served_by": state.name, "mode": state.mode})


@web.middleware
async def _mode_middleware(request: web.Request, handler):
    state: MockState = request.app["state"]
    # 显式控制/健康检查端点不受模式影响
    if request.path.startswith("/__") or request.path == "/healthz":
        return await handler(request)
    if request.path not in ("/mirror",):
        return await handler(request)
    if state.mode == "fail":
        return web.json_response(
            {"error": "mode_fail", "served_by": state.name},
            status=503, headers=state.base_headers(request),
        )
    if state.mode == "timeout":
        return await _timeout(request)
    if state.mode == "slow":
        return await _slow(request)
    return await handler(request)


def create_app(name: str, auth_token: str | None = None) -> web.Application:
    app = web.Application(middlewares=[_mode_middleware])
    app["state"] = MockState(name, auth_token)
    app.router.add_get("/healthz", _healthz)
    app.router.add_get("/mirror", _mirror)
    app.router.add_route("*", "/echo{tail:.*}", _mirror)
    app.router.add_get(r"/status/{code:\d+}", _status)
    app.router.add_get("/slow", _slow)
    app.router.add_get("/timeout", _timeout)
    app.router.add_get("/fail-then-ok", _fail_then_ok)
    app.router.add_get("/stream", _stream)
    app.router.add_post("/__mode/{mode}", _set_mode)
    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="mock 上游")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "9000")))
    parser.add_argument("--name", default=os.environ.get("UPSTREAM_NAME", "mock"))
    args = parser.parse_args()
    # 凭据只从环境变量读取，绝不来自命令行参数 / 配置文件
    token = os.environ.get("UPSTREAM_TOKEN")
    web.run_app(create_app(args.name, token), host=args.host, port=args.port, access_log=None)


if __name__ == "__main__":
    main()
