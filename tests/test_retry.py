"""指数退避 + 抖动计算。"""
from __future__ import annotations

import random

from gateway.config import RetryConfig
from gateway.retry import backoff_delay


def test_exponential_growth_capped() -> None:
    cfg = RetryConfig(backoff_base=0.1, backoff_max=1.0, jitter=0.0)
    assert backoff_delay(cfg, 1, random.Random(0)) == 0.1
    assert backoff_delay(cfg, 2, random.Random(0)) == 0.2
    assert backoff_delay(cfg, 3, random.Random(0)) == 0.4
    assert backoff_delay(cfg, 4, random.Random(0)) == 0.8
    assert backoff_delay(cfg, 5, random.Random(0)) == 1.0   # 封顶
    assert backoff_delay(cfg, 10, random.Random(0)) == 1.0


def test_jitter_bounds() -> None:
    cfg = RetryConfig(backoff_base=1.0, backoff_max=10.0, jitter=0.5)
    for seed in range(50):
        d = backoff_delay(cfg, 1, random.Random(seed))
        assert 1.0 <= d <= 1.5


def test_zero_jitter_is_deterministic() -> None:
    cfg = RetryConfig(backoff_base=0.2, jitter=0.0)
    assert backoff_delay(cfg, 1, random.Random(42)) == 0.2
