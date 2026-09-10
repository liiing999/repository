// Package repl 实现一个最小但正确的“静态 leader + 多数派提交”复制协议：
//
//   - 集群启动时通过 LEADER_ID 配置唯一 leader（节点 1..N），无选举；
//   - 所有写请求由 leader 序列化为 LogEntry（一条写 = 一个全局 index），
//     通过双向 gRPC 流推送给 follower，follower 应用后回 ack；
//   - leader 收到多数派（含自己）对某 index 的确认后提交，按序应用到
//     状态机并唤醒等待中的提案；
//   - follower 重连时按其 lastIndex 增量补日志；落后超出 leader 在内存
//     保留范围时发送全量快照。
//
// 一致性模型：读为顺序一致（sequential consistency）——任何节点上的读都
// 不会回退，读总是返回“某个已提交前缀”的状态；写在多数派确认后生效。
// 这不是线性一致：从 follower 读可能读到稍旧的值。需要读己之所写时，
// 客户端读 leader（SDK 默认如此）。
package repl

import (
	pb "github.com/example/distconf/api/distconf/v1"
	"github.com/example/distconf/internal/store"
)

// Applier 把日志条目确定性地应用到状态机。leader 与 follower 共用，
// 保证两端对同一条日志的解释完全一致。
type Applier interface {
	AppliedIndex() int64
	ApplyPut(index int64, op *pb.PutOp) (*pb.KeyValue, error)
	ApplyDelete(index int64, op *pb.DeleteOp) (bool, error)
	ApplyRegister(index int64, op *pb.RegisterOp) *pb.Instance
	ApplyHeartbeat(index int64, op *pb.HeartbeatOp) (*pb.Instance, error)
	ApplyExpireKeys(index int64, op *pb.ExpireKeysOp)
	ApplyExpireServices(index int64, op *pb.ExpireServicesOp)
	ApplySnapshot(snap *pb.Snapshot)
}

// StoreApplier 把 *store.Store 适配为 Applier。
type StoreApplier struct{ St *store.Store }

func (a StoreApplier) AppliedIndex() int64 { return a.St.AppliedRevision() }

func (a StoreApplier) ApplyPut(index int64, op *pb.PutOp) (*pb.KeyValue, error) {
	return a.St.ApplyPut(index, op)
}

func (a StoreApplier) ApplyExpireServices(index int64, op *pb.ExpireServicesOp) {
	a.St.ApplyExpireServices(index, op)
}

func (a StoreApplier) ApplyExpireKeys(index int64, op *pb.ExpireKeysOp) {
	a.St.ApplyExpireKeys(index, op)
}

func (a StoreApplier) ApplyHeartbeat(index int64, op *pb.HeartbeatOp) (*pb.Instance, error) {
	return a.St.ApplyHeartbeat(index, op)
}

func (a StoreApplier) ApplyRegister(index int64, op *pb.RegisterOp) *pb.Instance {
	return a.St.ApplyRegister(index, op)
}

func (a StoreApplier) ApplyDelete(index int64, op *pb.DeleteOp) (bool, error) {
	return a.St.ApplyDelete(index, op)
}

func (a StoreApplier) ApplySnapshot(snap *pb.Snapshot) {
	a.St.RestoreSnapshot(store.SnapshotFromProto(snap))
}

// applyOne 对单条日志执行状态机转换，返回 Put/Heartbeat 的结果与错误。
// CAS 冲突是“预期内的状态机结果”而非复制错误：该日志照常提交，
// 只是提案客户端收到冲突错误。
func applyOne(a Applier, e *pb.LogEntry) (kv *pb.KeyValue, inst *pb.Instance, found bool, err error) {
	switch op := e.Op.(type) {
	case *pb.LogEntry_Put:
		kv, err = a.ApplyPut(e.Index, op.Put)
	case *pb.LogEntry_Delete:
		found, err = a.ApplyDelete(e.Index, op.Delete)
	case *pb.LogEntry_Register:
		inst = a.ApplyRegister(e.Index, op.Register)
	case *pb.LogEntry_Heartbeat:
		inst, err = a.ApplyHeartbeat(e.Index, op.Heartbeat)
	case *pb.LogEntry_ExpireKeys:
		a.ApplyExpireKeys(e.Index, op.ExpireKeys)
	case *pb.LogEntry_ExpireServices:
		a.ApplyExpireServices(e.Index, op.ExpireServices)
	}
	return
}

// Config 是集群静态配置。
type Config struct {
	NodeID      int32
	LeaderID    int32
	NodeAddrs   map[int32]string // id -> host:port
	BatchWindow int64            // 内部日志保留条数（超出后新 follower 需要快照）
}

// Quorum 返回多数派大小。
func Quorum(n int) int { return n/2 + 1 }
