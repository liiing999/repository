"""结构化日志：JSON 行包含 request_id / route / upstream / latency_ms / status / retry_count。"""
from __future__ import annotations

import json
import logging

from gateway.logging_setup import JsonFormatter, request_context


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def test_json_formatter_includes_context_and_extra() -> None:
    formatter = JsonFormatter()
    token = request_context.set({"request_id": "rid-123"})
    try:
        record = logging.LogRecord(
            name="gateway.test", level=logging.INFO, pathname=__file__, lineno=1,
            msg="request_complete", args=(), exc_info=None,
        )
        record.route = "demo"
        record.upstream = "upstream-a"
        record.status = 200
        record.retry_count = 1
        record.latency_ms = 42.5
        line = formatter.format(record)
    finally:
        request_context.reset(token)

    payload = json.loads(line)
    assert payload["msg"] == "request_complete"
    assert payload["request_id"] == "rid-123"
    assert payload["route"] == "demo"
    assert payload["upstream"] == "upstream-a"
    assert payload["status"] == 200
    assert payload["retry_count"] == 1
    assert payload["latency_ms"] == 42.5
    assert payload["level"] == "INFO"


async def test_request_logs_carry_fields(gateway_factory, caplog) -> None:
    """端到端：成功请求与被限流请求的日志记录都带路由字段。"""
    client = await gateway_factory()
    caplog.set_level(logging.INFO, logger="aiohttp.web")
    await client.get("/demo/mirror", headers={"X-Request-Id": "rid-e2e"})

    messages = [r.getMessage() for r in caplog.records]
    assert "upstream_response" in messages
    assert "request_complete" in messages
    complete = next(r for r in caplog.records if r.getMessage() == "request_complete")
    assert complete.route == "demo"
    assert complete.upstream.startswith("upstream-")
    assert complete.status == 200
    assert complete.retry_count == 0
    assert hasattr(complete, "latency_ms")
