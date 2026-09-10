package store

import (
	"time"

	pb "github.com/example/distconf/api/distconf/v1"
)

func wallNowMs() int64 { return time.Now().UnixMilli() }

func kvToPB(ns, key string, e *kvEntry) *pb.KeyValue {
	return &pb.KeyValue{
		Namespace:  ns,
		Key:        key,
		Value:      append([]byte(nil), e.value...),
		Version:    e.version,
		Revision:   e.revision,
		TtlMs:      e.ttlMs,
		ExpireAtMs: e.expireAtMs,
	}
}

func instToPB(ns, service, id string, e *instEntry) *pb.Instance {
	return &pb.Instance{
		Namespace:  ns,
		Service:    service,
		InstanceId: id,
		Address:    e.address,
		Metadata:   cloneMap(e.metadata),
		LeaseTtlMs: e.leaseTtlMs,
		ExpireAtMs: e.expireAtMs,
		Revision:   e.revision,
	}
}

func cloneKV(kv *pb.KeyValue) *pb.KeyValue {
	if kv == nil {
		return nil
	}
	return &pb.KeyValue{
		Namespace:  kv.Namespace,
		Key:        kv.Key,
		Value:      append([]byte(nil), kv.Value...),
		Version:    kv.Version,
		Revision:   kv.Revision,
		TtlMs:      kv.TtlMs,
		ExpireAtMs: kv.ExpireAtMs,
	}
}

func cloneInstance(in *pb.Instance) *pb.Instance {
	if in == nil {
		return nil
	}
	return &pb.Instance{
		Namespace:  in.Namespace,
		Service:    in.Service,
		InstanceId: in.InstanceId,
		Address:    in.Address,
		Metadata:   cloneMap(in.Metadata),
		LeaseTtlMs: in.LeaseTtlMs,
		ExpireAtMs: in.ExpireAtMs,
		Revision:   in.Revision,
	}
}

func cloneMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// GetInstance 查询单个存活实例。
func (s *Store) GetInstance(namespace, service, id string) (*pb.Instance, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := svcKey{namespace, service, id}
	e, ok := s.instances[k]
	if !ok || !s.instAlive(e) {
		return nil, false
	}
	return instToPB(k.ns, k.service, k.id, e), true
}

// Snapshot 导出全量状态（leader 给落后 follower 发快照时使用）。
type Snapshot struct {
	Revision  int64
	KVs       []*pb.KeyValue
	Instances []*pb.Instance
}

// ExportSnapshot 返回当前全量状态的深拷贝。
func (s *Store) ExportSnapshot() *Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	snap := &Snapshot{Revision: s.rev}
	for k, e := range s.kvs {
		snap.KVs = append(snap.KVs, kvToPB(k.ns, k.key, e))
	}
	for k, e := range s.instances {
		snap.Instances = append(snap.Instances, instToPB(k.ns, k.service, k.id, e))
	}
	return snap
}

// RestoreSnapshot 用 leader 快照重置状态机（仅允许向更新的 revision 恢复）。
// 已存在的订阅者全部摘除，由上层 gRPC handler 重建。
func (s *Store) RestoreSnapshot(snap *Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if snap.Revision < s.rev {
		return
	}

	s.kvs = make(map[mapKey]*kvEntry, len(snap.KVs))
	for _, kv := range snap.KVs {
		s.kvs[mapKey{kv.Namespace, kv.Key}] = &kvEntry{
			value:      append([]byte(nil), kv.Value...),
			version:    kv.Version,
			revision:   kv.Revision,
			ttlMs:      kv.TtlMs,
			expireAtMs: kv.ExpireAtMs,
		}
	}
	s.instances = make(map[svcKey]*instEntry, len(snap.Instances))
	for _, in := range snap.Instances {
		s.instances[svcKey{in.Namespace, in.Service, in.InstanceId}] = &instEntry{
			address:    in.Address,
			metadata:   cloneMap(in.Metadata),
			leaseTtlMs: in.LeaseTtlMs,
			expireAtMs: in.ExpireAtMs,
			revision:   in.Revision,
		}
	}
	s.rev = snap.Revision
	s.sub = 0
	s.history = [historyCapacity]*Event{}
	s.head = 0
	s.count = 0
	// 快照点之前的事件历史不可得。基线取“快照 index 内最后一个可能的
	// 事件游标”，任何更老的 Watch 游标都会收到 ErrRevisionCompacted，
	// 客户端需全量重同步后再从当前游标订阅。
	s.baselineSeq = snap.Revision*subLimit + (subLimit - 1)
}

// ExpiredKV 是 sweeper 扫描到的一个到期 key。
type ExpiredKV struct {
	Namespace     string
	Key           string
	GuardRevision int64
}

// ExpiredInstance 是 sweeper 扫描到的一个到期实例。
type ExpiredInstance struct {
	Namespace     string
	Service       string
	InstanceID    string
	GuardRevision int64
}

// ScanExpiredKVs 返回当前已到期的 KV（快照式扫描，持锁时间短）。
func (s *Store) ScanExpiredKVs() []ExpiredKV {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.nowMs()
	var out []ExpiredKV
	for k, e := range s.kvs {
		if e.expireAtMs != 0 && e.expireAtMs <= now {
			out = append(out, ExpiredKV{k.ns, k.key, e.revision})
		}
	}
	return out
}

// ScanExpiredInstances 返回当前已到期的服务实例。
func (s *Store) ScanExpiredInstances() []ExpiredInstance {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.nowMs()
	var out []ExpiredInstance
	for k, e := range s.instances {
		if e.expireAtMs != 0 && e.expireAtMs <= now {
			out = append(out, ExpiredInstance{k.ns, k.service, k.id, e.revision})
		}
	}
	return out
}

// SetNowForTest 替换时间源（测试用）。
func (s *Store) SetNowForTest(f func() int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nowMs = f
}

// SnapshotToProto / ProtoToSnapshot 用于复制层传输。
func SnapshotToProto(snap *Snapshot) *pb.Snapshot {
	out := &pb.Snapshot{LastIncludedIndex: snap.Revision, Term: 1}
	for _, kv := range snap.KVs {
		out.Kvs = append(out.Kvs, &pb.KVRecord{Kv: cloneKV(kv)})
	}
	for _, in := range snap.Instances {
		out.Instances = append(out.Instances, &pb.InstanceRecord{Instance: cloneInstance(in)})
	}
	return out
}

// SnapshotFromProto 把 proto 快照转为内部结构（深拷贝）。
func SnapshotFromProto(p *pb.Snapshot) *Snapshot {
	snap := &Snapshot{Revision: p.LastIncludedIndex}
	for _, rec := range p.Kvs {
		snap.KVs = append(snap.KVs, cloneKV(rec.Kv))
	}
	for _, rec := range p.Instances {
		snap.Instances = append(snap.Instances, cloneInstance(rec.Instance))
	}
	return snap
}
