"""熔断器：按上游统计滑动窗口失败率，CLOSED -> OPEN -> HALF_OPEN -> CLOSED。

状态语义：
- CLOSED：正常放行；滚动窗口内请求数达到 ``min_requests`` 且失败率
  ``>= failure_rate`` 时跳闸到 OPEN；
- OPEN：直接拒绝（不发请求），持续 ``open_seconds``；
- HALF_OPEN：OPEN 到期后进入，只允许最多 ``half_open_max_calls`` 个试探请求，
  其余仍拒绝；试探成功 -> CLOSED（清空窗口），试探失败 -> 重新 OPEN。

并发正确性：状态判定与翻转在同一把 ``asyncio.Lock`` 下完成；探测名额用
``inflight`` 计数保证 HALF_OPEN 下不会超额放行。
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from enum import Enum

from .config import CircuitBreakerConfig


class BreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class BreakerOpenError(Exception):
    """熔断器处于 OPEN / HALF_OPEN 名额已满，请求未发往上游。"""


@dataclass
class BreakerSnapshot:
    upstream: str
    state: str
    failure_rate: float
    total: int
    opened_at: float | None


class CircuitBreaker:
    def __init__(self, upstream: str, cfg: CircuitBreakerConfig) -> None:
        self.upstream = upstream
        self.cfg = cfg
        self._state = BreakerState.CLOSED
        self._events: list[tuple[float, bool]] = []   # (单调时间, 是否成功)
        self._opened_at: float | None = None
        self._inflight_probes = 0
        self._lock = asyncio.Lock()

    @property
    def state(self) -> BreakerState:
        return self._state

    def _prune_and_rate(self, now: float) -> tuple[int, float]:
        cutoff = now - self.cfg.window_seconds
        self._events = [(t, ok) for t, ok in self._events if t >= cutoff]
        total = len(self._events)
        if total == 0:
            return 0, 0.0
        failures = sum(1 for _, ok in self._events if not ok)
        return total, failures / total

    async def allow(self) -> None:
        """请求上游前调用；拒绝时抛 :class:`BreakerOpenError`。"""
        async with self._lock:
            now = time.monotonic()
            if self._state == BreakerState.CLOSED:
                return
            if self._state == BreakerState.OPEN:
                assert self._opened_at is not None
                if now - self._opened_at >= self.cfg.open_seconds:
                    self._state = BreakerState.HALF_OPEN
                    self._inflight_probes = 0
                else:
                    raise BreakerOpenError(self.upstream)
            # HALF_OPEN：限制在途试探请求数
            if self._inflight_probes >= self.cfg.half_open_max_calls:
                raise BreakerOpenError(self.upstream)
            self._inflight_probes += 1

    async def record_success(self) -> None:
        async with self._lock:
            now = time.monotonic()
            if self._state == BreakerState.HALF_OPEN:
                # 试探成功：恢复并清空统计，避免旧失败样本立刻再次跳闸
                self._state = BreakerState.CLOSED
                self._opened_at = None
                self._events.clear()
            else:
                self._events.append((now, True))
                self._prune_and_rate(now)
            self._inflight_probes = max(0, self._inflight_probes - 1)

    async def record_failure(self) -> None:
        async with self._lock:
            now = time.monotonic()
            if self._state == BreakerState.HALF_OPEN:
                # 试探失败：重新 OPEN，重新计时
                self._state = BreakerState.OPEN
                self._opened_at = now
            else:
                self._events.append((now, False))
                total, rate = self._prune_and_rate(now)
                if (
                    self._state == BreakerState.CLOSED
                    and total >= self.cfg.min_requests
                    and rate >= self.cfg.failure_rate
                ):
                    self._state = BreakerState.OPEN
                    self._opened_at = now
            self._inflight_probes = max(0, self._inflight_probes - 1)

    def snapshot(self) -> BreakerSnapshot:
        now = time.monotonic()
        cutoff = now - self.cfg.window_seconds
        events = [(t, ok) for t, ok in self._events if t >= cutoff]
        total = len(events)
        failures = sum(1 for _, ok in events if not ok)
        return BreakerSnapshot(
            upstream=self.upstream,
            state=self._state.value,
            failure_rate=round((failures / total) if total else 0.0, 3),
            total=total,
            opened_at=self._opened_at,
        )


class BreakerRegistry:
    """每个上游一个熔断器；上游配置或默认配置在构建时注入。"""

    def __init__(
        self,
        configs: dict[str, CircuitBreakerConfig],
    ) -> None:
        self._breakers = {
            name: CircuitBreaker(name, cfg)
            for name, cfg in configs.items()
            if cfg.enabled
        }

    def get(self, upstream: str) -> CircuitBreaker | None:
        return self._breakers.get(upstream)

    def snapshots(self) -> list[BreakerSnapshot]:
        return [
            b.snapshot()
            for b in sorted(self._breakers.values(), key=lambda x: x.upstream)
        ]
