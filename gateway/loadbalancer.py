"""上游负载均衡：轮询（round_robin）与最少连接（least_connections）。

选择时同时与熔断器联动：熔断 OPEN / HALF_OPEN 探测名额已满的上游会被跳过；
``exclude`` 中是本次请求已经尝试失败的上游，重试优先换到其他上游，全部试过
后才允许回到已尝试的上游。
"""
from __future__ import annotations

import asyncio
import itertools
from dataclasses import dataclass

from .circuitbreaker import BreakerOpenError, BreakerRegistry
from .config import RouteConfig, UpstreamConfig


class NoHealthyUpstreamError(Exception):
    """候选上游全部处于熔断打开状态。"""


@dataclass
class _UpstreamState:
    config: UpstreamConfig
    active: int = 0


class LoadBalancer:
    def __init__(
        self,
        route: RouteConfig,
        upstreams: list[UpstreamConfig],
        breakers: BreakerRegistry,
    ) -> None:
        self.route = route
        self._candidates = [_UpstreamState(cfg) for cfg in upstreams]
        self._breakers = breakers
        self._lock = asyncio.Lock()
        self._rr = itertools.cycle(range(len(self._candidates)))
        self._strategy = route.lb

    def _ordered_candidates(self) -> list[_UpstreamState]:
        if self._strategy == "least_connections":
            # 活动连接数升序；相同则按配置顺序（轮转的公平性由释放后的再排序保证）
            return sorted(self._candidates, key=lambda c: (c.active, c.config.name))
        # 轮询：从循环游标当前位置开始
        start = next(self._rr)
        return self._candidates[start:] + self._candidates[:start]

    async def pick(self, exclude: set[str] | None = None) -> UpstreamConfig:
        """选出一个可发请求的上游；成功即占用（LC 计数 +1）。

        调用方必须在每次尝试结束后 :meth:`release`。
        """
        exclude = exclude or set()
        async with self._lock:
            ordered = self._ordered_candidates()
            # 第一轮排除已尝试的；全部不可用时第二轮放开排除（单上游重试场景）
            for allow_tried in (False, True):
                for cand in ordered:
                    if not allow_tried and cand.config.name in exclude:
                        continue
                    breaker = self._breakers.get(cand.config.name)
                    if breaker is not None:
                        try:
                            await breaker.allow()
                        except BreakerOpenError:
                            continue
                    cand.active += 1
                    return cand.config
            raise NoHealthyUpstreamError(
                f"路由 {self.route.name} 没有可用上游（熔断/名额）"
            )

    async def release(self, name: str) -> None:
        async with self._lock:
            for cand in self._candidates:
                if cand.config.name == name:
                    cand.active = max(0, cand.active - 1)
                    return

    def active_counts(self) -> dict[str, int]:
        return {c.config.name: c.active for c in self._candidates}
