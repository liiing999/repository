"""结构化日志：每行一条 JSON，贯穿 request_id / route / upstream 等字段。"""
from __future__ import annotations

import json
import logging
import sys
from contextvars import ContextVar
from typing import Any

# 请求级上下文（中间件在入口设置），所有该请求产生的日志自动带上
request_context: ContextVar[dict[str, Any]] = ContextVar(
    "request_context", default={}
)


class JsonFormatter(logging.Formatter):
    """把 LogRecord 渲染为单行 JSON。

    extra 传入的字段与 request_context 中的字段都会落到日志行里，
    便于按 request_id 检索一次请求的完整链路。
    """

    _RESERVED = {
        "args", "asctime", "created", "exc_info", "exc_text", "filename",
        "funcName", "levelname", "levelno", "lineno", "module", "msecs",
        "message", "msg", "name", "pathname", "process", "processName",
        "relativeCreated", "stack_info", "thread", "threadName",
        "taskName",
    }

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        payload.update(request_context.get())
        for key, value in record.__dict__.items():
            if key not in self._RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


class TextFormatter(logging.Formatter):
    """本地开发用的可读格式。"""

    def format(self, record: logging.LogRecord) -> str:
        ctx = request_context.get()
        rid = ctx.get("request_id", "-")
        base = (
            f"{self.formatTime(record, '%Y-%m-%d %H:%M:%S')} "
            f"{record.levelname:<5} [{rid}] {record.name}: {record.getMessage()}"
        )
        extra = {
            k: v
            for k, v in record.__dict__.items()
            if k not in JsonFormatter._RESERVED
            and not k.startswith("_")
            and k not in ctx
        }
        if extra:
            base += " " + " ".join(f"{k}={v}" for k, v in extra.items())
        if record.exc_info:
            base += "\n" + self.formatException(record.exc_info)
        return base


def configure_logging(level: str = "INFO", json_logs: bool = True) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter() if json_logs else TextFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    # aiohttp.access 自己会打访问日志；统一收口到 root 的 JSON handler
    logging.getLogger("aiohttp").setLevel(root.level)
