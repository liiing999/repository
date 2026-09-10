# 快速上手

设计原理见 [DESIGN.md](./DESIGN.md)。

## 构建与测试

```bash
go test ./...              # 单测 + 3 节点集成测试（go test -race ./... 同样通过）
go build ./...
```

单一二进制 `distconf` 既是节点（默认 `serve` 子命令）也内置客户端。

## docker-compose 三节点

```bash
docker compose up -d --build
# 等待 node1 healthy 后，在同一网络内执行全部验收（1-6）：
docker compose --profile verify run --rm verify
# leader 宕机边界（停 node1 → follower 读可用、写失败 → 自动恢复）：
bash scripts/verify-leader-down.sh
docker compose down -v
```

端口映射：node1 `9001`、node2 `9002`、node3 `9003`（容器内均为 9000）。

## 本地裸进程起三节点

```bash
PEERS="1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003"
distconf serve --node-id 1 --listen 127.0.0.1:9001 --leader-id 1 --peers "$PEERS" &
distconf serve --node-id 2 --listen 127.0.0.1:9002 --leader-id 1 --peers "$PEERS" &
distconf serve --node-id 3 --listen 127.0.0.1:9003 --leader-id 1 --peers "$PEERS" &
```

也可以全部用环境变量：`DISTCONF_NODE_ID`、`DISTCONF_LISTEN`、
`DISTCONF_LEADER_ID`、`DISTCONF_PEERS`、`DISTCONF_SWEEP_INTERVAL`、
`DISTCONF_REPL_TOKEN`（复制流令牌，**只从环境变量读取**）。

## 内置 CLI

```bash
E="--endpoints 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003"
distconf info $E
distconf put  $E --ns cfg --key db.host --value 10.0.0.5:5432 --expect-version -1
distconf put  $E --ns cfg --key db.host --value 10.0.0.6:5432 --expect-version 1  # CAS
distconf get  $E --ns cfg --key db.host          # 可从任意节点读
distconf watch --endpoints 127.0.0.1:9002 --ns cfg
distconf put  $E --ns cfg --key ephemeral --value v --ttl-ms 3000  # 3s 后 EXPIRED
distconf delete $E --ns cfg --key db.host
distconf register  $E --ns prod --service orders --id i-1 --address 10.0.0.11:8080 --ttl-ms 30000
distconf heartbeat $E --ns prod --service orders --id i-1
distconf discover  $E --ns prod --service orders
distconf discover  $E --ns prod --service orders --watch
```

## Go SDK 要点

```go
import "github.com/example/distconf/client"

c, _ := client.New([]string{"127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"})

// CAS 更新：expected 0=必须不存在，-1=无条件，>0=版本必须相等
cur, _ := c.Get(ctx, "cfg", "db.host")
_, err := c.Put(ctx, &pb.PutRequest{Namespace: "cfg", Key: "db.host",
    Value: []byte("10.0.0.6:5432"), ExpectedVersion: cur.Kv.Version})
// 冲突错误可用 errors.Is(err, client.ErrCASConflict) 判定。

// Watch：从上次游标续传，断线/换节点自动补发，不丢事件。
w := c.Watch(ctx, "cfg", "", 0)   // key 空 = 整个 namespace
for ev := range w.Events() { ... lastSeq = ev.Revision ... }

// 带自动心跳的服务租约：
lease := client.NewLease(c, &pb.RegisterRequest{Namespace: "prod",
    Service: "orders", InstanceId: hostname, Address: addr, LeaseTtlMs: 10000})
go lease.KeepAlive(ctx)   // 过期/无多数派后自动重新注册
```

## 运维语义速查

| gRPC code | 触发场景 | 客户端动作 |
|---|---|---|
| `Aborted` | CAS 版本冲突 | 重新 Get 取新 version 后重试 |
| `NotFound` | 心跳时实例已被摘除 | 重新 Register |
| `FailedPrecondition` | 写到 follower（消息带 `leader_hint=`） | SDK 已自动重定向 |
| `Unavailable` / deadline | 多数派不可达 | 退避重试 |
| `OutOfRange` | Watch 游标早于历史保留点 | 全量 Get 后从当前游标重新 Watch |
