"""重试退避：指数退避 + 均匀抖动。``delay(n) = min(base * 2**(n-1), max) + rand(0, jitter)``。"""
from __future__ import annotations

import random

from .config import RetryConfig


def backoff_delay(cfg: RetryConfig, retry_index: int, rng: random.Random | None = None) -> float:
    """retry_index 从 1 开始：第一次重试前等待的时间（秒）。"""
    rng = rng or random
    base_delay = min(cfg.backoff_base * (2 ** (retry_index - 1)), cfg.backoff_max)
    jitter = rng.uniform(0, cfg.jitter) if cfg.jitter > 0 else 0.0
    return base_delay + jitter
