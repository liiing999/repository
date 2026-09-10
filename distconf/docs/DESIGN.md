# distconf 设计说明

最小但正确的分布式配置中心 + 服务注册发现：单一 Go 二进制、内嵌内存存储、
gRPC 通信、静态 leader + 多数派复制。不依赖 etcd/consul/zookeeper。

## 1. 数据模型

```
namespace ── key ── { value, version, revision(=日志index), ttl_ms, expire_at_ms }
namespace ── service ── instance_id ── { address, metadata, lease_ttl_ms, expire_at_ms, revision }
```

- **key 级版本 `version`**：每次成功 `Put` 严格 +1，删除后重建从 1 开始。
  CAS 判定依据：`expected_version = -1` 不检查；`0` 要求 key 不存在；
  `>0` 要求当前版本严格相等。
- **全局修订号**：一次写操作 = 一条复制日志 = 一个全局日志 `index`。
  状态机内“全局 revision”恒等于已应用日志 index，所有节点在同一 index
  上得到相同状态。
- **租约**：key 与实例都可带 `ttl_ms`。绝对过期时间 `expire_at_ms` 只由
  leader 在写日志时按其单调时钟盖戳，避免客户端时钟漂移造成节点分歧。

## 2. 复制协议（静态 leader + 多数派提交）

### 角色

- `DISTCONF_LEADER_ID` 指定唯一 leader（3 节点即节点 1）。启动即确定，
  **无选举**。
- 所有写（Put/Delete/Register/Heartbeat/TTL 摘除）都只在 leader 受理，
  序列化为 `LogEntry{index, term=1, op}`；follower 收到写返回
  `FailedPrecondition` 并在消息中附带 `leader_hint=host:port`。
- follower 与 leader 建立**双向 gRPC 流** `AppendEntries`：
  1. follower 建流先报自己已应用位置 `ack_index`；
  2. leader 据此 catchup——落后在日志窗口内则补缺失后缀；
     leader 已做快照截断则先发全量 `Snapshot` 再发其后日志；
  3. 之后 leader 每条新日志立即推送，follower **按 index 严格连续应用**
     （收到 `index != applied+1` 直接报协议错误，靠重连+快照修复），
     逐条回 ack。

### 提交

- leader 内存中保留每个 follower 的 `match[id]`（已 ack 的最大连续 index）；
  流断开立即把该 follower 的 match 清零，**掉线节点不计票**。
- 单协程 `commitLoop` 负责两件事，状态机只被它触碰，无需额外 apply 锁：
  1. 从最新日志向前找最大的、满足 `1(leader自己) + match>=i 的 follower 数
     >= N/2+1` 的 index，推进 commit；
  2. 把 `(oldCommit, newCommit]` 按序应用到状态机，再唤醒对应提案的
     waiter（CAS 冲突等状态机结果也在此时返回给客户端）。
- 因此：日志 index 全局单调、不重复；同 key 并发 Put 在日志里天然有先后，
  CAS 在 **apply 阶段**判定（不是提议阶段），只有日志顺序里第一个满足
  版本前提的写成功，其余返回 `Aborted: cas conflict`，所有副本判定一致。

### 日志截断与快照

- leader 保留最近 `retainEntries=10000` 条日志。已提交日志超过窗口时做
  Raft 式快照：快照覆盖到当前 commit 点，之后只保留未提交尾巴
  （健康集群里通常为空）。新 follower / 长期离线 follower 重连先收快照。
- 快照同样会重置 follower 的 KV 事件历史（见第 3 节游标边界）。

## 3. 一致性模型

声明：**顺序一致（sequential consistency）+ 多数派已提交才生效**，
不是线性一致。

- 每个节点的状态都是“某个已提交日志前缀”，本地应用只进不退，所以
  任何节点上的读都不会读到比自己更新过的状态更旧的值（无回退），
  但 follower 可能落后 leader 若干毫秒——从 follower 读可能读到稍旧值。
- 写在 leader + 多数派 follower 持久化（内存）并应用后才返回成功。
- 读己之所写：写成功后读 **leader**（客户端 SDK 的默认策略：写走 leader，
  读轮询任意节点；需要强读己之所写时对同一客户端连续操作天然满足，
  因为其写后内部 leader 连接上的状态已包含该 index）。
- 少数派分区：leader 看不到多数派时写超时失败（`Unavailable`/ctx deadline），
  不会产生未确认写被当作成功；已经返回成功的写必然存在多数派，不会丢。

### leader 故障的行为边界（有意为之）

| 场景 | 行为 |
|---|---|
| follower 进程崩溃/重启 | 重启后空状态重连，leader 用快照+日志把它追到最新；期间集群多数派仍可写 |
| follower 与 leader 网络分区 | 该 follower 读停在旧前缀；重连自动追平 |
| **leader 进程存活但多数派失联** | 写失败（无多数派）；follower 读继续服务已提交前缀 |
| **leader 进程停止** | follower 仍可读到全部已提交数据；**所有写失败**，没有自动主切换；恢复 leader 进程后写入恢复 |
| leader 进程重启 | **本实现的明确边界**：存储是内嵌内存、无 WAL，leader 重启后为空日志；静态拓扑下 follower 会被它的空快照重置。要支持 leader 重启保数据，需要持久化 WAL 或加入选举（见“已知限制”）。验收中“leader 宕机”指停进程验证前两行，不断言重启后数据保留 |

## 4. Watch 为什么不丢事件

### 事件历史环

状态机维护容量 100000 的环形事件缓冲，记录每次 KV 变更
（PUT / DELETE / EXPIRED），**TTL 过期摘除与普通写走同一条日志，
所以过期事件也进历史环**。

### 每条事件有全局唯一、严格递增的游标

一个日志 index 内可能有多条事件（批量过期）。游标编码：

```
event_seq = log_index * SUB_LIMIT(4096) + 批内偏移
```

- 单 key 写的事件偏移为 0；批量过期中第 k 个被删 key 的偏移为 k。
- 游标严格单调、每条事件唯一，且能从游标反推出日志 index。
- 线上协议里 `WatchEvent.revision` 就是这个事件游标；`KeyValue.revision`
  仍是日志 index（与 version 语义配合做 CAS）。

### 订阅 = 历史补发 + 实时注册（同一把锁，无窗口）

`Store.WatchKV(ns, key, from_seq)` 在状态机同一把互斥锁内：

1. 从历史环中拷贝所有 `seq > from_seq` 且匹配 namespace/key 的事件
   （严格按序）作为补发；
2. 然后才注册实时订阅通道。

“补发到哪”和“从哪开始收实时”之间不存在能插入事件的临界区，
因此要么事件在补发里，要么在实时通道里，**不会丢**。

### 慢消费者与断线重连

- 实时通道缓冲 256；订阅者跟不上时服务端摘除该订阅（不拖慢状态机）。
- gRPC handler 发现通道关闭后，用**最后成功投递事件的游标**重新
  `WatchKV`，历史环补发缝隙中的事件，整个过程对客户端是同一条流；
  客户端 SDK 在网络断开时也用同一游标换节点重连。
- 游标落在历史环最老事件之前（离线太久）时，服务端明确返回
  `OutOfRange: history compacted`，客户端必须先全量 Get 再从当前游标
  订阅——这是唯一需要调用方介入的情况，属于显式边界而非静默丢事件。

### 通知有序性

所有事件按日志顺序（同日志按批内顺序）产生；一个订阅者的补发与实时
事件来自同一条全序，游标严格递增，因此通知有序。

## 5. TTL 与服务租约

- leader 的 sweeper（默认 200ms 一轮）扫描**逻辑已过期**（`expire_at <= now`）
  的 key / 实例，读路径同时做惰性过期判定。
- 摘除不是本地删除，而是各打一条 `ExpireKeys` / `ExpireServices` 批量日志，
  走多数派复制。所有节点在同一 index 删除同一批对象并向各自 Watch 方
  推 EXPIRED 事件/实例摘除增量。
- **扫描-提交窗口竞态**用 revision 守卫消除：扫描时记录对象当前 revision，
  删除日志只在 `当前 revision == guard` 时生效。对象在窗口期被重新
  Put/心跳（必带新 revision）就不会被旧的过期日志误删。
- Heartbeat 同样带 guard，并要求续租时租约未逻辑过期；实例已摘除则
  返回 `NotFound`，客户端（SDK 的 `Lease.KeepAlive`）自动重新 Register。
- 时钟语义：只使用 leader 时钟盖绝对时间戳；复制的是结果（expire_at）
  而非 TTL 倒计时，follower 时钟偏差不影响到期判定的一致性。

## 6. 接口与鉴权

- `KVService`: Get / Put / Delete / Watch（服务端流）
- `RegistryService`: Register / Heartbeat / Discover / WatchInstances（流）
- `ClusterService`: Info（角色、leader、commit/applied index、节点表）
- `ReplicationService`: AppendEntries（双向流，仅 leader 注册实现）
- gRPC 状态码：CAS 冲突 `Aborted`；实例不存在 `NotFound`；
  follower 写 `FailedPrecondition`（消息含 leader_hint）；
  无多数派 `Unavailable`；历史压缩 `OutOfRange`。
- 可选复制流令牌：环境变量 `DISTCONF_REPL_TOKEN`（服务端校验、
  follower 以 per-RPC metadata 携带）。**凭据只从环境变量读取**。

## 7. 目录结构

```
api/distconf/v1/        由 proto 生成的 gRPC 代码
proto/distconf/v1/      distconf.proto
internal/store/         确定性状态机、事件历史环、watch 分发、快照
internal/repl/          日志、quorum 提交、follower 流、快照 catchup
internal/sweeper/       leader 侧 TTL 扫描摘除
internal/server/        gRPC handler、节点组装、鉴权
client/                 Go SDK（leader 路由、watch 续传、租约 KeepAlive）
cmd/distconf/           单一二进制（serve + 内置客户端子命令）
scripts/                容器内/宿主验收脚本
```

## 8. 已知限制（超出“最小正确”范围）

1. 无 WAL 持久化：所有状态在内存，节点重启后靠 leader 快照恢复；
   leader 自身重启不保留数据（见第 3 节边界）。
2. 静态 leader、无选举：leader 宕机期间只读。演进路径是加入
   Raft RequestVote/Vote RPC（日志格式已含 term 字段）。
3. Watch 历史为固定 10 万条环形缓冲，极长期离线需全量重同步。
4. 未实现传输层 TLS（提供令牌 metadata；容器内网部署）。
