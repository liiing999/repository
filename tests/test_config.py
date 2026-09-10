"""配置加载：默认值合并、环境变量展开、校验。"""
from __future__ import annotations

from pathlib import Path

import pytest

from gateway.config import ConfigError, load_config

MINIMAL = """
gateway: {port: 8080}
upstreams:
  - {name: a, base_url: http://a.example}
routes:
  - {name: r1, path: /v1, upstreams: [a]}
"""


def test_minimal_config_defaults(tmp_path: Path) -> None:
    cfg_file = tmp_path / "c.yaml"
    cfg_file.write_text(MINIMAL, encoding="utf-8")
    cfg = load_config(cfg_file)
    assert cfg.port == 8080
    assert cfg.routes[0].retries.max_attempts == 2
    assert cfg.routes[0].lb == "round_robin"
    assert cfg.routes[0].circuit_breaker.enabled is True


def test_env_expansion(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("UP_TOKEN", "secret-123")
    cfg_file = tmp_path / "c.yaml"
    cfg_file.write_text(
        """
gateway: {port: 9000}
upstreams:
  - name: a
    base_url: http://a.example
    headers: {Authorization: "Bearer ${UP_TOKEN}"}
routes:
  - {name: r1, path: /v1, upstreams: [a]}
""",
        encoding="utf-8",
    )
    cfg = load_config(cfg_file)
    assert cfg.upstreams["a"].headers["Authorization"] == "Bearer secret-123"


def test_missing_env_raises(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("MISSING_XYZ", raising=False)
    cfg_file = tmp_path / "c.yaml"
    cfg_file.write_text(
        MINIMAL.replace("http://a.example", '"${MISSING_XYZ}"'), encoding="utf-8"
    )
    with pytest.raises(ConfigError, match="MISSING_XYZ"):
        load_config(cfg_file)


def test_env_default_used(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("MAYBE_SET", raising=False)
    cfg_file = tmp_path / "c.yaml"
    cfg_file.write_text(
        MINIMAL.replace("8080", '"${GW_PORT:-7777}"'), encoding="utf-8"
    )
    assert load_config(cfg_file).port == 7777


def test_unknown_upstream_reference(tmp_path: Path) -> None:
    cfg_file = tmp_path / "c.yaml"
    cfg_file.write_text(
        MINIMAL.replace("[a]", "[ghost]"), encoding="utf-8"
    )
    with pytest.raises(ConfigError, match="未定义的 upstream"):
        load_config(cfg_file)


def test_route_overrides_merge_defaults(tmp_path: Path) -> None:
    cfg_file = tmp_path / "c.yaml"
    cfg_file.write_text(
        """
circuit_breaker_defaults: {min_requests: 99, open_seconds: 7}
upstreams:
  - {name: a, base_url: http://a}
routes:
  - name: r
    path: /v
    upstreams: [a]
    circuit_breaker: {failure_rate: 0.25}
""",
        encoding="utf-8",
    )
    cb = load_config(cfg_file).routes[0].circuit_breaker
    assert cb.min_requests == 99          # 来自默认
    assert cb.open_seconds == 7           # 来自默认
    assert cb.failure_rate == 0.25        # 路由覆盖
    assert cb.window_seconds == 10        # dataclass 默认仍生效


def test_invalid_failure_rate(tmp_path: Path) -> None:
    cfg_file = tmp_path / "c.yaml"
    cfg_file.write_text(
        """
upstreams:
  - {name: a, base_url: http://a}
routes:
  - name: r
    path: /v
    upstreams: [a]
    circuit_breaker: {failure_rate: 1.5}
""",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError):
        load_config(cfg_file)
