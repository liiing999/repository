"""配置模型与 YAML 加载。

设计要点：
- 全部路由 / 上游 / 重试 / 熔断 / 限流参数都来自 YAML，代码不内置业务路由；
- 支持 ``${ENV}`` / ``${ENV:-default}`` 环境变量展开，凭据只走环境变量，不进 YAML 明文；
- 各层默认值（gateway 级 defaults → 路由级覆盖）在加载时合并为完整 dataclass。
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


class ConfigError(ValueError):
    """配置文件非法（结构错误 / 缺失必填 / 引用了未设置的环境变量）。"""


def _expand_env(text: str) -> str:
    def repl(match: re.Match[str]) -> str:
        name, default = match.group(1), match.group(2)
        if name in os.environ:
            return os.environ[name]
        if default is not None:
            return default
        raise ConfigError(f"环境变量 {name} 未设置，且配置未提供默认值")

    return _ENV_PATTERN.sub(repl, text)


def _expand_in_obj(obj: Any) -> Any:
    if isinstance(obj, str):
        return _expand_env(obj)
    if isinstance(obj, list):
        return [_expand_in_obj(v) for v in obj]
    if isinstance(obj, dict):
        return {k: _expand_in_obj(v) for k, v in obj.items()}
    return obj


@dataclass(frozen=True)
class RetryConfig:
    max_attempts: int = 2          # 含首次尝试；1 = 不重试
    backoff_base: float = 0.1      # 秒：第 n 次重试退避 base * 2**(n-1)
    backoff_max: float = 2.0
    jitter: float = 0.05           # 均匀抖动上限（秒）
    retry_on_status: tuple[int, ...] = (502, 503, 504)


@dataclass(frozen=True)
class CircuitBreakerConfig:
    enabled: bool = True
    window_seconds: float = 10.0
    min_requests: int = 10
    failure_rate: float = 0.5
    open_seconds: float = 5.0
    half_open_max_calls: int = 1


@dataclass(frozen=True)
class RateLimitConfig:
    capacity: float = 100.0
    refill_per_second: float = 50.0
    backend: str = "memory"        # memory | redis


@dataclass(frozen=True)
class UpstreamConfig:
    name: str
    base_url: str
    headers: dict[str, str] = field(default_factory=dict)
    timeout: float | None = None   # 缺省由使用它的路由 timeout 决定
    circuit_breaker: CircuitBreakerConfig | None = None


@dataclass(frozen=True)
class RouteConfig:
    name: str
    path: str
    upstreams: tuple[str, ...]
    strip_prefix: bool = False
    lb: str = "round_robin"
    timeout: float = 10.0
    retries: RetryConfig = field(default_factory=RetryConfig)
    rate_limit: RateLimitConfig | None = None
    circuit_breaker: CircuitBreakerConfig = field(
        default_factory=CircuitBreakerConfig
    )


@dataclass(frozen=True)
class RedisConfig:
    url: str
    key_prefix: str = "gw:rl"


@dataclass(frozen=True)
class GatewayConfig:
    host: str
    port: int
    request_timeout: float | None
    drain_timeout: float
    log_level: str
    log_json: bool
    trust_forwarded: bool
    upstreams: dict[str, UpstreamConfig]
    routes: list[RouteConfig]
    rate_limit_defaults: RateLimitConfig | None
    redis: RedisConfig | None


def _build_retry(raw: dict[str, Any] | None) -> RetryConfig:
    raw = raw or {}
    cfg = RetryConfig(
        max_attempts=int(raw.get("max_attempts", 2)),
        backoff_base=float(raw.get("backoff_base", 0.1)),
        backoff_max=float(raw.get("backoff_max", 2.0)),
        jitter=float(raw.get("jitter", 0.05)),
        retry_on_status=tuple(
            int(s) for s in raw.get("retry_on_status", [502, 503, 504])
        ),
    )
    if cfg.max_attempts < 1:
        raise ConfigError("retries.max_attempts 必须 >= 1")
    if cfg.backoff_base < 0 or cfg.jitter < 0:
        raise ConfigError("退避参数不能为负")
    return cfg


def _build_cb(raw: dict[str, Any] | None, *, default_enabled: bool = True) -> CircuitBreakerConfig:
    raw = dict(raw or {})
    raw.setdefault("enabled", default_enabled)
    cfg = CircuitBreakerConfig(
        enabled=bool(raw["enabled"]),
        window_seconds=float(raw.get("window_seconds", 10.0)),
        min_requests=int(raw.get("min_requests", 10)),
        failure_rate=float(raw.get("failure_rate", 0.5)),
        open_seconds=float(raw.get("open_seconds", 5.0)),
        half_open_max_calls=int(raw.get("half_open_max_calls", 1)),
    )
    if not 0 < cfg.failure_rate <= 1:
        raise ConfigError("circuit_breaker.failure_rate 必须在 (0, 1] 区间")
    if cfg.half_open_max_calls < 1:
        raise ConfigError("half_open_max_calls 必须 >= 1")
    if cfg.window_seconds <= 0 or cfg.open_seconds < 0:
        raise ConfigError("熔断器窗口/打开时间非法")
    return cfg


def _build_rl(
    raw: dict[str, Any] | None, defaults: RateLimitConfig | None
) -> RateLimitConfig | None:
    if raw is None:
        return None
    base = defaults or RateLimitConfig()
    cfg = RateLimitConfig(
        capacity=float(raw.get("capacity", base.capacity)),
        refill_per_second=float(raw.get("refill_per_second", base.refill_per_second)),
        backend=str(raw.get("backend", base.backend)),
    )
    if cfg.capacity <= 0 or cfg.refill_per_second < 0:
        raise ConfigError("限流容量必须 > 0、补充速率不能为负")
    if cfg.backend not in ("memory", "redis"):
        raise ConfigError(f"未知限流后端: {cfg.backend}")
    return cfg


def load_config(path: str | Path) -> GatewayConfig:
    path = Path(path)
    if not path.is_file():
        raise ConfigError(f"配置文件不存在: {path}")
    raw_text = path.read_text(encoding="utf-8")
    data = yaml.safe_load(raw_text) or {}
    data = _expand_in_obj(data)
    if not isinstance(data, dict):
        raise ConfigError("配置顶层必须是映射")

    gw = data.get("gateway") or {}
    rl_defaults = _build_rl(data.get("rate_limit_defaults"), None)
    cb_defaults_raw = data.get("circuit_breaker_defaults") or {}

    upstreams: dict[str, UpstreamConfig] = {}
    for item in data.get("upstreams") or []:
        if not item.get("name") or not item.get("base_url"):
            raise ConfigError("每个 upstream 必须有 name 和 base_url")
        name = str(item["name"])
        if name in upstreams:
            raise ConfigError(f"upstream 名称重复: {name}")
        upstreams[name] = UpstreamConfig(
            name=name,
            base_url=str(item["base_url"]).rstrip("/"),
            headers={str(k): str(v) for k, v in (item.get("headers") or {}).items()},
            timeout=float(item["timeout"]) if item.get("timeout") is not None else None,
            circuit_breaker=(
                _build_cb(item["circuit_breaker"], default_enabled=False)
                if item.get("circuit_breaker")
                else None
            ),
        )

    routes: list[RouteConfig] = []
    seen_paths: set[str] = set()
    for item in data.get("routes") or []:
        if not item.get("name") or not item.get("path") or not item.get("upstreams"):
            raise ConfigError("每个 route 必须有 name、path、upstreams")
        name = str(item["name"])
        prefix = str(item["path"])
        if not prefix.startswith("/"):
            raise ConfigError(f"路由 {name} 的 path 必须以 / 开头")
        if prefix in seen_paths:
            raise ConfigError(f"路由路径重复: {prefix}")
        seen_paths.add(prefix)

        ups = tuple(str(u) for u in item["upstreams"])
        unknown = [u for u in ups if u not in upstreams]
        if unknown:
            raise ConfigError(f"路由 {name} 引用了未定义的 upstream: {unknown}")

        lb = str(item.get("lb", "round_robin"))
        if lb not in ("round_robin", "least_connections"):
            raise ConfigError(f"路由 {name} 的 lb 必须是 round_robin/least_connections")

        # 路由级熔断配置 = 默认值上覆盖
        merged_cb = {**cb_defaults_raw, **(item.get("circuit_breaker") or {})}
        routes.append(
            RouteConfig(
                name=name,
                path=prefix,
                upstreams=ups,
                strip_prefix=bool(item.get("strip_prefix", False)),
                lb=lb,
                timeout=float(item.get("timeout", 10.0)),
                retries=_build_retry(item.get("retries")),
                rate_limit=_build_rl(item.get("rate_limit"), rl_defaults),
                circuit_breaker=_build_cb(merged_cb),
            )
        )

    redis_cfg = None
    if data.get("redis"):
        redis_cfg = RedisConfig(
            url=str(data["redis"]["url"]),
            key_prefix=str(data["redis"].get("key_prefix", "gw:rl")),
        )

    return GatewayConfig(
        host=str(gw.get("host", "0.0.0.0")),
        port=int(gw.get("port", 8080)),
        request_timeout=(
            float(gw["request_timeout"]) if gw.get("request_timeout") is not None else None
        ),
        drain_timeout=float(gw.get("drain_timeout", 20.0)),
        log_level=str(gw.get("log_level", "INFO")),
        log_json=bool(gw.get("log_json", True)),
        trust_forwarded=bool(gw.get("trust_forwarded", True)),
        upstreams=upstreams,
        routes=routes,
        rate_limit_defaults=rl_defaults,
        redis=redis_cfg,
    )
