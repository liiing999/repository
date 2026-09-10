"""每路由独立的令牌桶限流。

并发正确性（进程内）
--------------------
asyncio 是单线程事件循环，协程只会在 ``await`` 点切换。令牌桶的
"取当前时间 → 补充令牌 → 判断并扣减" 整段是同步代码，中间没有 ``await``，
因此对每个桶而言是一个不可分割的临界区，多个并发协程不会交错执行；
``asyncio.Lock`` 进一步在语义上显式声明这一互斥关系，并保护懒初始化序列。
桶状态（剩余令牌、上次补充时间）保存在桶对象内，按路由隔离，互不影响。

水平扩展
--------
多进程 / 多副本部署时，进程内状态各自独立，每副本只承担 1/N 的限流精度。
若需要集群级精确限流，配置 ``backend: redis`` + 顶层 ``redis`` 段，由
:class:`RedisRateLimiter` 用一段原子 Lua 脚本在单线程的 Redis 内完成
"补充 + 判断 + 扣减"，跨进程同样安全。
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Protocol

from .config import RateLimitConfig, RedisConfig


@dataclass
class RateLimitSnapshot:
    route: str
    backend: str
    capacity: float
    refill_per_second: float
    allowed: int
    rejected: int
    tokens: float | None = None     # 仅内存桶可观测当前令牌数


class RateLimiter(Protocol):
    async def acquire(self, route: str, tokens: float = 1.0) -> bool: ...
    def snapshot(self) -> list[RateLimitSnapshot]: ...
    async def aclose(self) -> None: ...


class TokenBucket:
    """惰性补充的令牌桶（按单调时钟计时）。"""

    __slots__ = ("capacity", "refill_per_second", "tokens", "updated_at")

    def __init__(self, capacity: float, refill_per_second: float) -> None:
        self.capacity = capacity
        self.refill_per_second = refill_per_second
        self.tokens = capacity
        self.updated_at = time.monotonic()

    def take(self, amount: float, now: float) -> bool:
        # 1) 按经过时间补充
        elapsed = now - self.updated_at
        if elapsed > 0:
            self.tokens = min(
                self.capacity, self.tokens + elapsed * self.refill_per_second
            )
            self.updated_at = now
        # 2) 判断并扣减
        if self.tokens >= amount:
            self.tokens -= amount
            return True
        return False


class InMemoryRateLimiter:
    """路由名 -> 令牌桶；每个桶一把锁。"""

    def __init__(self) -> None:
        self._buckets: dict[str, tuple[RateLimitConfig, TokenBucket]] = {}
        self._stats: dict[str, list[int]] = {}   # route -> [allowed, rejected]
        self._locks: dict[str, asyncio.Lock] = {}

    def register(self, route: str, cfg: RateLimitConfig) -> None:
        if route not in self._buckets:
            self._buckets[route] = (
                cfg,
                TokenBucket(cfg.capacity, cfg.refill_per_second),
            )
            self._stats[route] = [0, 0]
            self._locks[route] = asyncio.Lock()

    async def acquire(self, route: str, tokens: float = 1.0) -> bool:
        async with self._locks[route]:
            cfg, bucket = self._buckets[route]
            allowed = bucket.take(tokens, time.monotonic())
            self._stats[route][0 if allowed else 1] += 1
            return allowed

    def snapshot(self) -> list[RateLimitSnapshot]:
        out = []
        for route, (cfg, bucket) in self._buckets.items():
            allowed, rejected = self._stats[route]
            out.append(
                RateLimitSnapshot(
                    route=route,
                    backend="memory",
                    capacity=cfg.capacity,
                    refill_per_second=cfg.refill_per_second,
                    allowed=allowed,
                    rejected=rejected,
                    tokens=round(bucket.tokens, 3),
                )
            )
        return out

    async def aclose(self) -> None:
        return None


# KEYS[1] = 限流键；ARGV = 容量、每秒补充、请求令牌数、当前毫秒时间
# 返回 {1=放行/0=拒绝, 剩余令牌}；整段脚本在 Redis 单线程内原子执行
_REDIS_LUA = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local need = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then
  tokens = capacity
  ts = now
end
local elapsed = (now - ts) / 1000.0
if elapsed > 0 then
  tokens = math.min(capacity, tokens + elapsed * rate)
end
local allowed = 0
if tokens >= need then
  tokens = tokens - need
  allowed = 1
end
redis.call('HSET', key, 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', key, math.ceil((capacity / math.max(rate, 0.0001)) * 1000) + 5000)
return {allowed, tostring(tokens)}
"""


class RedisRateLimiter:
    """基于 Redis Lua 的跨进程令牌桶（可选依赖 redis-py）。"""

    def __init__(self, redis_cfg: RedisConfig, configs: dict[str, RateLimitConfig]) -> None:
        try:
            import redis.asyncio as redis_lib  # type: ignore
        except ImportError as exc:  # pragma: no cover - 依赖缺失路径
            raise RuntimeError(
                "使用 Redis 限流需要安装可选依赖: pip install '.[redis]'"
            ) from exc
        self._redis = redis_lib.from_url(redis_cfg.url)
        self._prefix = redis_cfg.key_prefix
        self._configs = configs
        self._stats: dict[str, list[int]] = {r: [0, 0] for r in configs}

    async def acquire(self, route: str, tokens: float = 1.0) -> bool:
        cfg = self._configs[route]
        now_ms = time.time() * 1000.0
        key = f"{self._prefix}:{route}"
        result = await self._redis.eval(
            _REDIS_LUA, 1, key, cfg.capacity, cfg.refill_per_second, tokens, now_ms
        )
        allowed = bool(int(result[0]))
        self._stats[route][0 if allowed else 1] += 1
        return allowed

    def snapshot(self) -> list[RateLimitSnapshot]:
        return [
            RateLimitSnapshot(
                route=route,
                backend="redis",
                capacity=cfg.capacity,
                refill_per_second=cfg.refill_per_second,
                allowed=self._stats[route][0],
                rejected=self._stats[route][1],
            )
            for route, cfg in self._configs.items()
        ]

    async def aclose(self) -> None:
        await self._redis.aclose()
