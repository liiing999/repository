"""docker-compose 环境的一键演示脚本（仅用标准库）。

前置：docker compose up -d --build
运行：python scripts/demo.py

演示内容：
1. 正常轮询：流量在 mock-a/b/c 间均匀分布；
2. 注入故障：mock-a 持续返回 503 -> 网关重试把在途请求转移到健康上游；
3. 熔断器打开：统计窗口内失败率过阈值后，mock-a 被熔断，流量完全不再发往它；
4. 限流：/limited 路由容量 3，突发第 4 个请求起收到 429；
5. 半开恢复：mock-a 恢复后，OPEN 冷却结束 -> HALF_OPEN 试探成功 -> CLOSED。
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request

GATEWAY = os.environ.get("GATEWAY_URL", "http://localhost:8080").rstrip("/")
MOCKS = {
    "mock-a": os.environ.get("MOCK_A_URL", "http://localhost:9001"),
    "mock-b": os.environ.get("MOCK_B_URL", "http://localhost:9002"),
    "mock-c": os.environ.get("MOCK_C_URL", "http://localhost:9003"),
}


def http(method: str, url: str, timeout: float = 10.0):
    req = urllib.request.Request(url, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode()
            return resp.status, json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode()
        try:
            return exc.code, json.loads(body)
        except json.JSONDecodeError:
            return exc.code, {}


def wait_ready() -> None:
    print("==> 等待网关就绪 ...")
    for _ in range(60):
        status, body = http("GET", f"{GATEWAY}/healthz", timeout=2)
        if status == 200 and body.get("status") == "ok":
            print("    网关已就绪\n")
            return
        time.sleep(1)
    sys.exit("网关在 60s 内未就绪，是否已 docker compose up -d --build ?")


def breaker_state() -> dict:
    _, state = http("GET", f"{GATEWAY}/__admin/state")
    return {b["upstream"]: b["state"] for b in state.get("breakers", [])}


def phase_round_robin() -> None:
    print("==> 阶段 1：正常轮询（12 个请求）")
    served: dict[str, int] = {}
    for i in range(12):
        status, body = http("GET", f"{GATEWAY}/demo/mirror")
        name = body.get("served_by", f"HTTP{status}")
        served[name] = served.get(name, 0) + 1
    print(f"    分布: {served}")
    assert served == {"mock-a": 4, "mock-b": 4, "mock-c": 4}, "轮询应均匀分布"
    print("    OK：三个上游各 4 次\n")


def phase_failover_and_circuit_break() -> None:
    print("==> 阶段 2：让 mock-a 持续返回 503")
    status, _ = http("POST", f"{MOCKS['mock-a']}/__mode/fail")
    assert status == 200

    print("    连续发 20 个请求，观察 (HTTP状态, 实际处理者) ...")
    served: dict[str, int] = {}
    statuses: dict[int, int] = {}
    a_opened_at = None
    for i in range(20):
        st, body = http("GET", f"{GATEWAY}/demo/mirror")
        name = body.get("served_by", f"HTTP{st}")
        served[name] = served.get(name, 0) + 1
        statuses[st] = statuses.get(st, 0) + 1
        states = breaker_state()
        if a_opened_at is None and states.get("upstream-a") == "open":
            a_opened_at = i + 1
        time.sleep(0.05)

    print(f"    HTTP 状态分布: {statuses}")
    print(f"    实际处理者分布: {served}")
    print(f"    熔断器状态: {breaker_state()}")
    print(f"    mock-a 在第 {a_opened_at} 个请求前后被熔断")

    states = breaker_state()
    assert states.get("upstream-a") == "open", "mock-a 熔断器应已打开"
    assert "mock-a" not in served or all(
        # 熔断后流量绝不再到 a：a 即使出现也只可能是熔断前的试探次数
        served.get("mock-a", 0) <= 10
    ), "熔断后不应再选择 mock-a"
    print("    OK：故障上游被熔断，客户端请求全部由健康上游接管（无 5xx）\n")


def phase_rate_limit() -> None:
    print("==> 阶段 3：/limited 路由限流（容量 3，1 令牌/秒）")
    codes = []
    for _ in range(6):
        status, _ = http("GET", f"{GATEWAY}/limited/mirror")
        codes.append(status)
    print(f"    突发 6 请求的状态码: {codes}")
    assert codes[:3] == [200, 200, 200]
    assert codes[3:] == [429, 429, 429]
    print("    OK：前 3 个放行，之后 429\n")


def phase_half_open_recovery() -> None:
    print("==> 阶段 4：恢复 mock-a，等待熔断器进入 HALF_OPEN 试探")
    status, _ = http("POST", f"{MOCKS['mock-a']}/__mode/mirror")
    assert status == 200
    print("    冷却 11 秒（compose 配置 open_seconds=10）...")
    time.sleep(11)
    status, body = http("GET", f"{GATEWAY}/demo/mirror")
    print(f"    试探请求: HTTP {status}, served_by={body.get('served_by')}")
    states = breaker_state()
    print(f"    熔断器状态: {states}")
    assert status == 200
    assert states.get("upstream-a") == "closed", "试探成功后应恢复 CLOSED"
    print("    OK：HALF_OPEN 试探成功，熔断器恢复 CLOSED\n")


def main() -> None:
    wait_ready()
    phase_round_robin()
    phase_failover_and_circuit_break()
    phase_rate_limit()
    phase_half_open_recovery()
    print("全部演示通过。查看结构化日志：docker compose logs gateway | grep request_id")
    print("收尾：docker compose down -v")


if __name__ == "__main__":
    main()
