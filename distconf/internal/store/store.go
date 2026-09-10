// Package store 是系统的确定性状态机：配置 KV、服务实例表、
// 全局 revision（与复制日志 index 一一对应）、KV 事件历史环形缓冲，
// 以及 watch 订阅分发。
//
// 所有变更都只能通过 Apply* 系列方法发生；这些方法由复制层在
// “按日志 index 顺序提交”时调用，并显式传入该条日志的 index。
// 状态机内的全局 revision 恒等于已应用日志 index，因此在所有节点
// 上确定性地收敛。
package store

import (
	"sync"

	pb "github.com/example/distconf/api/distconf/v1"
)

// historyCapacity 是 KV 事件历史的保留条数（环形缓冲）。
// Watch 请求的游标落在最老事件之前会返回 ErrRevisionCompacted。
const historyCapacity = 100_000

// subLimit 是单条复制日志（一个日志 index）内最多产生的 KV 事件数
// （当前仅批量过期可能 >1，且每批对象数受对象总数约束）。
// 对外暴露的“事件 revision（游标）”编码为：
//
//	eventRev = logIndex*subLimit + 批内偏移(0 起)
//
// 它在全局严格单调、每条事件唯一；批内偏移在每条日志应用开始时清零。
// KeyValue.version/Revision 中记录的“key 级 revision”仍是日志 index，
// 而 WatchEvent.revision 是事件游标。
const subLimit = 4096

// watcherBuffer 是单个订阅者的实时事件缓冲长度；发送不下时丢弃该订阅者，
// 客户端会以最后收到的 revision 重新订阅，由历史补发保证不丢。
const watcherBuffer = 256

type kvEntry struct {
	value      []byte
	version    int64 // key 级版本，每次 Put +1；删除后重建从 1 开始
	revision   int64 // 最近一次 Put/Delete 的全局 revision（日志 index）
	ttlMs      int64
	expireAtMs int64 // 0 表示永不过期；过期时间由 leader 盖戳
}

type instEntry struct {
	address    string
	metadata   map[string]string
	leaseTtlMs int64
	expireAtMs int64 // 0 表示永不过期
	revision   int64 // 最近 register/heartbeat 的 revision，用作过期删除守卫
}

// Event 是进入历史缓冲的 KV 变更事件。
type Event struct {
	Seq  int64 // 全局唯一事件游标 = logIndex*subLimit + 批内偏移
	Kind pb.WatchEvent_Kind
	KV   *pb.KeyValue // KV.Revision 记录的是日志 index
}

type kvWatcher struct {
	ns  string
	key string // 空串 = 订阅整个 namespace
	ch  chan *pb.WatchEvent
}

type svcWatcher struct {
	ns      string
	service string
	ch      chan ServiceChange
}

// ServiceChange 是服务实例集合的一次增量变化。
type ServiceChange struct {
	Added   []*pb.Instance
	Removed []*pb.Instance
}

// Store 是内存状态机。零值不可用，用 New 构造。
type Store struct {
	mu sync.Mutex

	nowMs func() int64 // 可在测试中替换

	kvs       map[mapKey]*kvEntry
	instances map[svcKey]*instEntry

	rev int64 // 全局 revision == 已应用日志 index
	sub int   // 当前日志 index 内已发出的 KV 事件偏移

	// KV 事件历史环
	history [historyCapacity]*Event
	head    int // 最老事件下标
	count   int
	// 快照恢复后环为空时的基线事件游标；<= 它的事件不可得
	baselineSeq int64

	kvWatchers  map[int64]*kvWatcher
	svcWatchers map[int64]*svcWatcher
	watcherSeq  int64
}

type mapKey struct{ ns, key string }

type svcKey struct{ ns, service, id string }

// New 创建空状态机。
func New() *Store {
	return &Store{
		nowMs:       wallNowMs,
		kvs:         make(map[mapKey]*kvEntry),
		instances:   make(map[svcKey]*instEntry),
		kvWatchers:  make(map[int64]*kvWatcher),
		svcWatchers: make(map[int64]*svcWatcher),
	}
}

// EventSeq 把日志 index 编码为该日志最后一个事件的游标。
func EventSeq(logIndex int64) int64 { return logIndex * subLimit }

// AppliedRevision 返回已应用到的全局 revision（= 已应用日志 index）。
func (s *Store) AppliedRevision() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rev
}

// CurrentEventSeq 返回最近一条 KV 事件的全局游标（无事件时返回 0）。
// 客户端通常在 Get 后以它作为 Watch 起点。
func (s *Store) CurrentEventSeq() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.count > 0 {
		return s.history[(s.head+s.count-1)%historyCapacity].Seq
	}
	return s.baselineSeq
}

func (s *Store) aliveKV(e *kvEntry) bool {
	return e.expireAtMs == 0 || e.expireAtMs > s.nowMs()
}

// Get 读取一个 key；已过期视为不存在（惰性过期）。
func (s *Store) Get(namespace, key string) (*pb.KeyValue, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.kvs[mapKey{namespace, key}]
	if !ok || !s.aliveKV(e) {
		return nil, false
	}
	return kvToPB(namespace, key, e), true
}

// ApplyPut 应用 index 位置的一条 Put 日志。CAS 判定发生在这里
// （而不是提议时），从而所有副本按相同的日志顺序得到相同判定。
// 冲突时不改变任何状态（该日志 index 为空操作）。
func (s *Store) ApplyPut(index int64, op *pb.PutOp) (*pb.KeyValue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.rev = index
	s.sub = 0
	k := mapKey{op.Namespace, op.Key}
	cur, exists := s.kvs[k]
	if exists && !s.aliveKV(cur) {
		exists = false
		cur = nil
	}
	if err := checkVersion(op.ExpectedVersion, exists, curVersion(cur)); err != nil {
		return nil, err
	}

	e := &kvEntry{
		value:      append([]byte(nil), op.Value...),
		version:    curVersion(cur) + 1,
		revision:   index,
		ttlMs:      op.TtlMs,
		expireAtMs: op.ExpireAtMs,
	}
	s.kvs[k] = e

	rec := kvToPB(op.Namespace, op.Key, e)
	s.emitKVLocked(pb.WatchEvent_PUT, rec)
	return rec, nil
}

// ApplyDelete 应用一条 Delete 日志。key 不存在时为幂等 no-op（不产生事件）。
func (s *Store) ApplyDelete(index int64, op *pb.DeleteOp) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.rev = index
	s.sub = 0
	k := mapKey{op.Namespace, op.Key}
	cur, exists := s.kvs[k]
	if exists && !s.aliveKV(cur) {
		exists = false
		cur = nil
	}
	if !exists {
		if op.ExpectedVersion > 0 {
			return false, ErrCASConflict
		}
		return false, nil
	}
	if err := checkVersion(op.ExpectedVersion, true, cur.version); err != nil {
		return false, err
	}

	delete(s.kvs, k)
	rec := kvToPB(op.Namespace, op.Key, cur)
	rec.Revision = index
	s.emitKVLocked(pb.WatchEvent_DELETE, rec)
	return true, nil
}

// ApplyExpireKeys 应用一条 TTL 批量到期日志。多个 key 可以在同一条日志
// （同一个 revision）到期，它们作为多条事件共享同一 revision；Watch 的
// 补发/实时路径都在状态机锁下原子地看到整批。guard 保证只删除
// “自 leader 扫描以来未被重新 Put 过”的值。
func (s *Store) ApplyExpireKeys(index int64, op *pb.ExpireKeysOp) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.rev = index
	s.sub = 0
	for _, item := range op.Items {
		k := mapKey{item.Namespace, item.Key}
		cur, ok := s.kvs[k]
		if !ok || cur.revision != item.GuardRevision {
			continue // 已被删除或已被新 Put 刷新（新 Put 带新 revision）
		}
		delete(s.kvs, k)
		rec := kvToPB(item.Namespace, item.Key, cur)
		rec.Revision = index
		s.emitKVLocked(pb.WatchEvent_EXPIRED, rec)
	}
}

// ApplyRegister 注册（或重新注册）一个服务实例。
func (s *Store) ApplyRegister(index int64, op *pb.RegisterOp) *pb.Instance {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.rev = index
	k := svcKey{op.Namespace, op.Service, op.InstanceId}
	_, existed := s.instances[k]
	e := &instEntry{
		address:    op.Address,
		metadata:   cloneMap(op.Metadata),
		leaseTtlMs: op.LeaseTtlMs,
		expireAtMs: op.ExpireAtMs,
		revision:   index,
	}
	s.instances[k] = e
	inst := instToPB(op.Namespace, op.Service, op.InstanceId, e)
	if !existed {
		s.fanoutSvcLocked(ServiceChange{Added: []*pb.Instance{cloneInstance(inst)}})
	}
	return inst
}

// ApplyHeartbeat 续租。以下情况返回 ErrInstanceNotFound，客户端应重新
// Register：实例已被摘除；guard 与当前 revision 不符（提议与提交之间
// 恰好发生了到期摘除）；租约在 leader 时钟下已逻辑到期。
func (s *Store) ApplyHeartbeat(index int64, op *pb.HeartbeatOp) (*pb.Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.rev = index
	k := svcKey{op.Namespace, op.Service, op.InstanceId}
	e, ok := s.instances[k]
	if !ok || (op.GuardRevision != 0 && e.revision != op.GuardRevision) || !s.instAlive(e) {
		return nil, ErrInstanceNotFound
	}
	e.expireAtMs = op.NewExpireAtMs
	e.revision = index
	return instToPB(op.Namespace, op.Service, op.InstanceId, e), nil
}

// ApplyExpireServices 应用一条服务实例批量到期摘除日志。
func (s *Store) ApplyExpireServices(index int64, op *pb.ExpireServicesOp) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.rev = index
	var removed []*pb.Instance
	for _, item := range op.Items {
		k := svcKey{item.Namespace, item.Service, item.InstanceId}
		e, ok := s.instances[k]
		if !ok || e.revision != item.GuardRevision {
			continue
		}
		delete(s.instances, k)
		inst := instToPB(item.Namespace, item.Service, item.InstanceId, e)
		inst.Revision = index
		removed = append(removed, inst)
	}
	if len(removed) > 0 {
		s.fanoutSvcLocked(ServiceChange{Removed: removed})
	}
}

func curVersion(e *kvEntry) int64 {
	if e == nil {
		return 0
	}
	return e.version
}

// checkVersion 统一 CAS 判定：
// expectedVersion == -1 不检查；0 要求不存在；>0 要求版本严格相等。
func checkVersion(expected int64, exists bool, current int64) error {
	switch {
	case expected == -1:
		return nil
	case expected == 0 && exists:
		return ErrCASConflict
	case expected > 0 && (!exists || current != expected):
		return ErrCASConflict
	default:
		return nil
	}
}

// ----------------------------------------------------------------
// 历史 / Watch
// ----------------------------------------------------------------

// emitKVLocked 追加一条 KV 事件到历史环并实时扇出。
// 必须在“应用某条日志”的持锁区间内调用（s.sub 在该日志开始时清零）。
func (s *Store) emitKVLocked(kind pb.WatchEvent_Kind, kv *pb.KeyValue) {
	seq := kv.Revision*subLimit + int64(s.sub)
	s.sub++
	if s.sub >= subLimit {
		panic("store: KV events per log index exceed subLimit")
	}
	ev := &Event{Seq: seq, Kind: kind, KV: cloneKV(kv)}
	s.appendEventLocked(ev)
	s.fanoutKVLocked(&pb.WatchEvent{Kind: kind, Kv: cloneKV(kv), Revision: seq})
}

func (s *Store) appendEventLocked(ev *Event) {
	if s.count < historyCapacity {
		s.history[(s.head+s.count)%historyCapacity] = ev
		s.count++
	} else {
		s.history[s.head] = ev
		s.head = (s.head + 1) % historyCapacity
	}
}

// oldestSeq 返回当前可得的最老事件游标；环为空时返回 0。
func (s *Store) oldestSeq() int64 {
	if s.count == 0 {
		return 0
	}
	return s.history[s.head].Seq
}

func (s *Store) matchKV(ns, key string, ev *Event) bool {
	if ev.KV.Namespace != ns {
		return false
	}
	return key == "" || ev.KV.Key == key
}

// WatchKV 原子地完成“取历史补发 + 注册实时订阅”，消除二者之间的丢事件窗口。
// fromSeq 为客户端已收到的最后事件游标，服务端补发其后全部匹配事件
// （严格按 Seq 升序），再由实时通道续推。
// 调用方必须最终调用 CloseKVWatcher 释放。
func (s *Store) WatchKV(ns, key string, fromSeq int64) (int64, []*pb.WatchEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 可得边界：fromSeq==0 表示“从保留的最老历史开始”；
	// 否则必须 >= 最老事件的前一个游标或快照基线。
	if fromSeq > 0 {
		horizon := s.baselineSeq
		if s.count > 0 {
			horizon = s.oldestSeq() - 1
		}
		if fromSeq < horizon {
			return 0, nil, ErrRevisionCompacted
		}
	}

	var backlog []*pb.WatchEvent
	for i := 0; i < s.count; i++ {
		ev := s.history[(s.head+i)%historyCapacity]
		if ev.Seq > fromSeq && s.matchKV(ns, key, ev) {
			backlog = append(backlog, &pb.WatchEvent{
				Kind:     ev.Kind,
				Kv:       cloneKV(ev.KV),
				Revision: ev.Seq,
			})
		}
	}

	s.watcherSeq++
	id := s.watcherSeq
	s.kvWatchers[id] = &kvWatcher{
		ns:  ns,
		key: key,
		ch:  make(chan *pb.WatchEvent, watcherBuffer),
	}
	return id, backlog, nil
}

// KVWatcherChan 返回实时事件通道；订阅不存在（被摘除）时返回 nil。
func (s *Store) KVWatcherChan(id int64) <-chan *pb.WatchEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	if w, ok := s.kvWatchers[id]; ok {
		return w.ch
	}
	return nil
}

// CloseKVWatcher 关闭并移除订阅。
func (s *Store) CloseKVWatcher(id int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if w, ok := s.kvWatchers[id]; ok {
		close(w.ch)
		delete(s.kvWatchers, id)
	}
}

func (s *Store) fanoutKVLocked(ev *pb.WatchEvent) {
	for id, w := range s.kvWatchers {
		if w.ns != ev.Kv.Namespace || (w.key != "" && w.key != ev.Kv.Key) {
			continue
		}
		out := &pb.WatchEvent{Kind: ev.Kind, Kv: cloneKV(ev.Kv), Revision: ev.Revision}
		select {
		case w.ch <- out:
		default:
			// 慢消费者：摘除之。客户端按最后收到的事件游标重连，
			// 由 WatchKV 的历史补发路径兜底，事件不会丢。
			close(w.ch)
			delete(s.kvWatchers, id)
		}
	}
}

// ----------------------------------------------------------------
// 服务发现
// ----------------------------------------------------------------

func (s *Store) instAlive(e *instEntry) bool {
	return e.expireAtMs == 0 || e.expireAtMs > s.nowMs()
}

// Discover 返回某 service 当前存活实例（惰性过滤过期项）。
func (s *Store) Discover(namespace, service string) []*pb.Instance {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.discoverLocked(namespace, service)
}

func (s *Store) discoverLocked(namespace, service string) []*pb.Instance {
	var out []*pb.Instance
	for k, e := range s.instances {
		if k.ns != namespace || k.service != service || !s.instAlive(e) {
			continue
		}
		out = append(out, instToPB(k.ns, k.service, k.id, e))
	}
	return out
}

// WatchService 原子地返回当前快照并注册增量订阅。
// 调用方最终必须 CloseServiceWatcher。
func (s *Store) WatchService(namespace, service string) (int64, []*pb.Instance, <-chan ServiceChange) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.watcherSeq++
	id := s.watcherSeq
	w := &svcWatcher{ns: namespace, service: service, ch: make(chan ServiceChange, watcherBuffer)}
	s.svcWatchers[id] = w

	snapshot := s.discoverLocked(namespace, service)
	return id, snapshot, w.ch
}

// CloseServiceWatcher 释放服务订阅。
func (s *Store) CloseServiceWatcher(id int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if w, ok := s.svcWatchers[id]; ok {
		close(w.ch)
		delete(s.svcWatchers, id)
	}
}

func (s *Store) fanoutSvcLocked(c ServiceChange) {
	for id, w := range s.svcWatchers {
		added := filterInstances(w.ns, w.service, c.Added)
		removed := filterInstances(w.ns, w.service, c.Removed)
		if len(added) == 0 && len(removed) == 0 {
			continue
		}
		select {
		case w.ch <- ServiceChange{Added: added, Removed: removed}:
		default:
			close(w.ch)
			delete(s.svcWatchers, id)
		}
	}
}

func filterInstances(ns, service string, in []*pb.Instance) []*pb.Instance {
	var out []*pb.Instance
	for _, x := range in {
		if x.Namespace == ns && x.Service == service {
			out = append(out, x)
		}
	}
	return out
}
