# lightweight-gateway

一个**配置驱动、可水平扩展的轻量 API 网关**，单进程 Python 3.11+ / asyncio / aiohttp 实现。
**不依赖任何现成网关 / 服务网格框架**——路由、负载均衡、限流、熔断、重试退避、优雅退出全部自行实现。

## 功能一览

| 需求 | 实现 | 位置 |
|------|------|------|
| 配置驱动路由 | YAML，最长前缀匹配，一条路由挂多个上游 | `gateway/config.py` |
| 负载均衡 | 轮询 / 最少连接，选择时联动熔断与重试排除 | `gateway/loadbalancer.py` |
| 超时 / 重试 | 单上游超时 + 整体预算；指数退避 + 抖动，可配最大次数与可重试状态码 | `gateway/retry.py`, `proxy.py` |
| 熔断器 | 按上游、滑动窗口失败率，CLOSED→OPEN→HALF_OPEN→CLOSED | `gateway/circuitbreaker.py` |
| 限流 | 每路由独立令牌桶；进程内（默认）/ Redis Lua（多副本共享） | `gateway/ratelimit.py` |
| 转发正确性 | 头/体透传、流式分块转发、逐跳头过滤、5xx/超时状态映射、request_id 贯穿 | `gateway/proxy.py` |
| 优雅退出 | 停接新请求 → 排空在途（限时）→ 强制清理 → 关连接池 | `gateway/app.py` |
| 可观测性 | 每行一条 JSON：request_id / route / upstream / status / retry_count / latency_ms | `gateway/logging_setup.py` |
| 凭据 | 只从环境变量注入（`${ENV}` / `${ENV:-default}` 展开），YAML 无明文 | `gateway/config.py` |

## 快速开始（容器一键起）

```bash
docker compose up -d --build        # 网关 :8080 + 3 个 mock 上游（:9001-9003）
python scripts/demo.py              # 标准库即可，演示轮询/熔断/转移/限流/半开恢复
docker compose logs -f gateway      # 看结构化 JSON 日志
docker compose down -v              # 收尾
```

演示脚本实测输出要点：

```
阶段1 正常轮询:      {'mock-a': 4, 'mock-b': 4, 'mock-c': 4}
阶段2 mock-a 注入503: 20 个请求全部 200，由 mock-b/mock-c 接管；
                     熔断器状态 {'upstream-a': 'open', ...}
阶段3 /limited 限流: [200, 200, 200, 429, 429, 429]
阶段4 故障恢复:      OPEN 冷却 → HALF_OPEN 试探 200 → CLOSED
```

## 本地开发 / 测试

```bash
python -m venv .venv && . .venv/Scripts/activate   # Windows
# python -m venv .venv && source .venv/bin/activate # Linux/macOS
pip install -r requirements-dev.txt -e .

pytest                       # 49 个单测 + 集成测试
python -m gateway.app -c config.yaml          # 直接跑网关
python -m mock_upstream.server --name mock-a  # 直接跑 mock 上游
```

测试覆盖（`tests/`）：令牌桶容量/补充/**500 协程并发不超发**、熔断器各状态迁移、
轮询/最少连接/熔断跳过/重试排除、退避公式、配置加载与校验，以及端到端：
头与体透传、request_id 贯穿、重试成功、全失败 503、超时→504、**单上游故障被熔断且流量转移**、
半开恢复、429、流式分块、优雅排空（在途请求完成后才退出 / 排空超时强制退出）、JSON 日志字段。

## 配置格式说明

完整示例见 `config.yaml`（本地）/ `config.compose.yaml`（compose）。顶层结构：

```yaml
gateway:
  host: 0.0.0.0
  port: 8080
  request_timeout: 30        # 每请求整体预算（含全部重试/退避）；拿到响应头后的流式传输不受限
  drain_timeout: 20          # 优雅退出排空上限（秒）
  log_level: INFO
  log_json: true
  trust_forwarded: true      # 生成 X-Forwarded-For/Proto/Host

rate_limit_defaults: {capacity: 100, refill_per_second: 50, backend: memory}
circuit_breaker_defaults:    # 路由级 circuit_breaker 在其上覆盖
  {window_seconds: 10, min_requests: 10, failure_rate: 0.5,
   open_seconds: 5, half_open_max_calls: 1}

upstreams:
  - name: upstream-a
    base_url: http://mock-a:9000
    timeout: 3                       # 可选，覆盖路由超时
    headers: {Authorization: "Bearer ${UPSTREAM_A_TOKEN}"}  # 凭据走环境变量
    circuit_breaker: {open_seconds: 3}                      # 可选，上游级覆盖

routes:
  - name: demo
    path: /demo                      # 最长前缀匹配；/demo/x 命中 /demo
    strip_prefix: true               # 转发前去掉 /demo
    upstreams: [upstream-a, upstream-b, upstream-c]
    lb: round_robin                  # round_robin | least_connections
    timeout: 3                       # 单次上游请求超时
    retries:
      max_attempts: 3                # 含首次，1 = 不重试
      backoff_base: 0.1              # delay_n = min(base·2^(n-1), backoff_max) + rand(0, jitter)
      backoff_max: 2.0
      jitter: 0.05
      retry_on_status: [502, 503, 504]   # 4xx 等非列名状态直接透传，不重试
    rate_limit: {capacity: 20, refill_per_second: 10}   # 不配则不限流
    circuit_breaker: {enabled: true}

# 可选：多副本共享限流（水平扩展）
# redis: {url: "${GATEWAY_REDIS_URL:-redis://localhost:6379/0}", key_prefix: "gw:rl"}
```

环境变量展开：`${NAME}`（未设置则启动报错）与 `${NAME:-default}`；只用于字符串值。

## 关键设计与正确性说明

### 限流在并发下为什么是正确的
- asyncio 单线程事件循环中，协程只在 `await` 点切换。令牌桶的"读时钟 → 按经过时间补充
  → 判断 → 扣减"整段**不含 await**，对单个桶是不可分割的临界区；实现中再以
  `asyncio.Lock` 显式串行化（见 `ratelimit.py:take/acquire`）。
- 因此**不会超发**：`tests/test_ratelimit.py` 用 500 个并发协程抢 100 容量的桶，
  断言放行数恰好 100。每个路由一个独立桶，互不影响。
- 水平扩展：多副本时进程内桶是 per-pod 的（精度按副本数分摊）。需要集群级精确限流时，
  配置 `backend: redis`：补充+判断+扣减封装为一段 **Lua 脚本**，在 Redis 单线程内原子完成。

### 熔断器
按**上游**（而非路由）统计，滑动窗口只保留 `window_seconds` 内的成败样本；窗口请求数
达到 `min_requests` 且失败率 ≥ 阈值才跳闸，避免小样本误杀。OPEN 期间负载均衡直接跳过该上游
（不产生网络请求）；冷却后进入 HALF_OPEN，最多放行 `half_open_max_calls` 个试探请求——
成功则清空窗口恢复 CLOSED，失败则重新 OPEN 并重新计时。

### 负载均衡与重试的联动
- 轮询用无锁风格的 cycle 游标 + 选择锁；最少连接按活动请求数（计数在发出前 +1、
  收到响应头即 −1）升序选择。
- 重试选择上游时**排除本次已失败的上游**，优先做故障转移；全部都试过才允许重试同一上游。
- 熔断 OPEN 的上游在选择阶段即被跳过；全部不可用时快速返回 `503 upstream_unavailable`。

### 转发与状态码映射

| 上游情形 | 客户端看到 |
|----------|-----------|
| 连接错误（拒绝/重置/DNS） | `502`，`{"error":"upstream_failure",...}` |
| 上游超时 | `504` |
| 所有上游熔断中 | `503` + `error=upstream_unavailable`（未发请求） |
| 网关整体预算耗尽（重试中） | `504` + `error=gateway_timeout` |
| 上游 5xx 且重试仍失败 | **最后一次上游状态码**（502/503/504）透传 |
| 其他状态（含 4xx、非重试 5xx） | 原样透传状态码、头、体 |
| 排空期间新请求 | `503` + `error=gateway_draining` |

- 请求头全部透传，过滤逐跳头（`Connection/Transfer-Encoding/...`）与旧 `X-Forwarded-*`
  后重新生成；响应体以 64KB 分块**流式转发**（不缓冲整包），响应头同样过滤逐跳头。
- `X-Request-Id`：沿用入站值，缺失时生成 `gw-<uuid>`；写入转发请求头、响应头与全部日志。
  错误体中也带 `request_id`，方便报障对账。
- 为支持失败后安全重试，请求体会被完整读入内存（受 `client_max_size=64MB` 限制）；
  一旦响应头已发给客户端（流式中途上游断开），状态码无法改写，此时不重试，只记日志并把该次
  计入熔断失败；客户端提前断开则不计上游失败。

### 优雅退出（SIGTERM/SIGINT）
1. 置 drain 事件：中间件对新请求立即返回 `503 gateway_draining`（`/healthz` 变为 `draining`）；
2. `site.stop()` 停止监听 socket；
3. `on_shutdown` 等待在途计数归零，最多 `drain_timeout`，期间重试退避也会被排空中断；
4. 超时仍未排空则由 aiohttp 的 `shutdown_timeout` 强制收尾，随后关闭上游连接池与限流器，
   保证进程一定能退出，不会永久挂起。

### 水平扩展
网关本身无状态（除限流/熔断统计外）：任意副本数水平扩容，前置 L4/L7 负载均衡即可。
熔断是各副本独立的本地视图（这是常规客户端侧熔断的语义，故障上游会被每个副本各自独立发现）；
需要集群统一限流时启用 Redis 后端。

## 观测端点

- `GET /healthz` — `{"status":"ok|draining","inflight":N}`
- `GET /__admin/state` — 各熔断器状态/窗口失败率、各路由限流放行拒绝计数与剩余令牌、
  各上游活动连接数

日志示例（每行一条 JSON）：

```json
{"ts":"...","level":"INFO","msg":"request_complete","request_id":"trace-demo-001",
 "route":"demo","upstream":"upstream-b","status":200,"retry_count":0,"latency_ms":1.1}
```

## mock 上游

同一镜像双角色（`mock_upstream/server.py`）。端点：
`/mirror`、`/echo`、`/status/{code}`、`/slow?delay=`、`/timeout`、
`/fail-then-ok?fail=N`、`/stream?chunks=&delay=`；
`POST /__mode/{mirror|fail|timeout|slow}` 运行时注入故障。
设置环境变量 `UPSTREAM_TOKEN` 后要求 `Authorization: Bearer <token>`，用于演示凭据透传。

## 目录结构

```
gateway/            网关包（config / ratelimit / circuitbreaker / loadbalancer /
                    retry / proxy / app / logging_setup）
mock_upstream/      可编排故障的 mock 上游
tests/              pytest：单元 + 同进程端到端集成（49 个）
scripts/demo.py     compose 环境演示脚本
config.yaml         本地配置示例
config.compose.yaml compose 演示配置
Dockerfile          单镜像，命令行区分网关 / mock 角色
docker-compose.yml  网关 + 3 个 mock 上游
```
