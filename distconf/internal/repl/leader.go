package repl

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	pb "github.com/example/distconf/api/distconf/v1"
)

// ErrNoQuorum 在多数派 follower 不可达、无法推进提交时返回。
var ErrNoQuorum = errors.New("replication failed: no quorum available")

// ErrNotLeader 表示当前节点不是 leader（follower 上收到写请求时返回）。
var ErrNotLeader = errors.New("not the leader")

// retainEntries 是 leader 内存中保留的最近日志条数。
// follower 落后到这个窗口之前时，leader 改为发送全量快照 + 窗口内日志。
const retainEntries = 10_000

// Result 是一条已提交日志的状态机结果。
type Result struct {
	KV    *pb.KeyValue
	Inst  *pb.Instance
	Found bool
	Err   error
}

type waiter struct {
	ch chan Result
}

// Leader 是 leader 侧复制器：日志追加、quorum 提交、按序应用、
// follower 流推送、快照生成。
type Leader struct {
	cfg Config
	ap  Applier

	mu sync.Mutex
	// 日志窗口：log[0].Index == compactIndex+1
	log          []*pb.LogEntry
	compactIndex int64
	savedSnap    *pb.Snapshot // 截断点对应的快照（last_included_index == compactIndex）

	waiters map[int64]*waiter
	commit  int64 // 已提交 index
	applied int64 // 已应用 index（leader 本地）

	// match[id] = follower 已 ack 的最大 index；断线时清零，
	// 防止把掉线节点计入多数派。
	match map[int32]int64

	wakeCh chan struct{} // 只给 commitLoop：有新日志/新 ack
	ping   *broadcaster  // 给 follower 推送协程：有新日志

	stopCh  chan struct{}
	stopped atomic.Bool

	exportSnapshot func() *pb.Snapshot
}

// NewLeader 创建 leader 复制器。
func NewLeader(cfg Config, ap Applier) *Leader {
	return &Leader{
		cfg:     cfg,
		ap:      ap,
		waiters: make(map[int64]*waiter),
		match:   make(map[int32]int64),
		wakeCh:  make(chan struct{}, 1),
		ping:    newBroadcaster(),
		stopCh:  make(chan struct{}),
	}
}

// SetSnapshotExporter 注入状态机快照导出函数。
func (l *Leader) SetSnapshotExporter(f func() *pb.Snapshot) {
	l.exportSnapshot = f
}

// Start 启动提交推进循环。
func (l *Leader) Start() { go l.commitLoop() }

// Stop 停止 leader。
func (l *Leader) Stop() {
	if l.stopped.CompareAndSwap(false, true) {
		close(l.stopCh)
	}
}

func (l *Leader) wake() {
	select {
	case l.wakeCh <- struct{}{}:
	default:
	}
}

// notifyReplicators 通知所有 follower 推送协程“有新日志”。
func (l *Leader) notifyReplicators() { l.ping.notify() }

// SubscribeEntries 订阅“新日志已追加”信号。
func (l *Leader) SubscribeEntries() (int64, <-chan struct{}) { return l.ping.subscribe() }

// UnsubscribeEntries 退订。
func (l *Leader) UnsubscribeEntries(id int64) { l.ping.unsubscribe(id) }

// Propose 追加一条日志，等待多数派提交并按序应用后返回状态机结果。
func (l *Leader) Propose(ctx context.Context, e *pb.LogEntry) (Result, error) {
	if l.stopped.Load() {
		return Result{}, ErrNotLeader
	}

	l.mu.Lock()
	e.Index = l.compactIndex + int64(len(l.log)) + 1
	e.Term = 1
	w := &waiter{ch: make(chan Result, 1)}
	l.waiters[e.Index] = w
	l.log = append(l.log, e)
	l.mu.Unlock()
	l.wake()
	l.notifyReplicators()

	select {
	case r := <-w.ch:
		return r, nil
	case <-ctx.Done():
		// 客户端超时不撤销日志：它可能已经（或即将）提交。
		// CAS + version 使调用方重试是安全的。
		l.mu.Lock()
		delete(l.waiters, e.Index)
		l.mu.Unlock()
		return Result{}, ctx.Err()
	case <-l.stopCh:
		return Result{}, ErrNotLeader
	}
}

// commitLoop 串行完成 quorum 推进与按序应用；状态机只在本协程被触碰，
// 因而不需要额外的 apply 锁。
func (l *Leader) commitLoop() {
	for {
		select {
		case <-l.stopCh:
			return
		case <-l.wakeCh:
		}

		l.mu.Lock()
		lastIndex := l.compactIndex + int64(len(l.log))
		need := Quorum(len(l.cfg.NodeAddrs))

		// ack 是前缀性质：match>=i 即拥有 i。从后往前找最大可提交点。
		newCommit := l.commit
		for i := lastIndex; i > l.commit; i-- {
			votes := 1 // leader 自己
			for id := range l.cfg.NodeAddrs {
				if id != l.cfg.NodeID && l.match[id] >= i {
					votes++
				}
			}
			if votes >= need {
				newCommit = i
				break
			}
		}

		type signal struct {
			ch  chan Result
			res Result
		}
		var signals []signal
		if newCommit > l.commit {
			for idx := l.commit + 1; idx <= newCommit; idx++ {
				e := l.log[idx-1-l.compactIndex]
				kv, inst, found, err := applyOne(l.ap, e)
				l.applied = idx
				if w, ok := l.waiters[idx]; ok {
					signals = append(signals, signal{
						ch:  w.ch,
						res: Result{KV: kv, Inst: inst, Found: found, Err: err},
					})
					delete(l.waiters, idx)
				}
			}
			l.commit = newCommit
			l.compactLocked()
		}
		l.mu.Unlock()

		// 在锁外唤醒提案者（通道带缓冲，不会阻塞）。
		for _, s := range signals {
			s.ch <- s.res
		}
	}
}

// compactLocked 在已提交日志增长过大时做快照并截断。
// 采用 Raft 标准语义：快照覆盖到当前 commit（= applied）点，
// savedSnap.LastIncludedIndex == commit，日志中只保留 index > commit
// 的未提交尾巴（多数派健康时通常为空）。
// 调用时状态机恰好应用到 l.commit，导出来的就是该点的一致快照。
func (l *Leader) compactLocked() {
	if len(l.log) <= retainEntries || l.commit <= l.compactIndex {
		return
	}
	cut := l.commit
	if l.exportSnapshot != nil {
		l.savedSnap = l.exportSnapshot()
	}
	l.log = append([]*pb.LogEntry(nil), l.log[cut-l.compactIndex:]...)
	l.compactIndex = cut
}

// FollowerAck 记录某 follower 已应用到的 index。
func (l *Leader) FollowerAck(id int32, index int64) {
	l.mu.Lock()
	if index > l.match[id] {
		l.match[id] = index
	}
	l.mu.Unlock()
	l.wake()
}

// FollowerReset 在 follower 流断开时把其 ack 清零。
func (l *Leader) FollowerReset(id int32) {
	l.mu.Lock()
	l.match[id] = 0
	l.mu.Unlock()
	l.wake()
}

// Catchup 返回 follower(last=followerLast) 建立流时需要发送的内容：
// 若它落后到日志窗口之前，先发截断点快照，再发快照点之后的增量日志；
// 否则只发增量。
func (l *Leader) Catchup(followerLast int64) (*pb.Snapshot, []*pb.LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()

	firstKept := l.compactIndex + 1
	if followerLast >= firstKept {
		start := followerLast - l.compactIndex
		return nil, append([]*pb.LogEntry(nil), l.log[start:]...)
	}

	// follower 需要窗口之前的日志。
	if l.compactIndex == 0 {
		// 从未截断：follower 只是空节点，全部日志都在窗口内。
		return nil, append([]*pb.LogEntry(nil), l.log...)
	}

	// 截断点快照由 compactLocked 生成（LastIncludedIndex == compactIndex）。
	snap := l.savedSnap
	if snap == nil && l.exportSnapshot != nil {
		snap = l.exportSnapshot() // 防御性兜底
	}
	return snap, append([]*pb.LogEntry(nil), l.log...)
}

// LastIndex 返回 leader 日志最后 index。
func (l *Leader) LastIndex() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.compactIndex + int64(len(l.log))
}

// CommittedIndex 返回已提交 index。
func (l *Leader) CommittedIndex() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.commit
}

// MatchOf 返回 follower 当前 ack（Info/测试用）。
func (l *Leader) MatchOf(id int32) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.match[id]
}

// StopChan 返回停止通道。
func (l *Leader) StopChan() <-chan struct{} { return l.stopCh }

// EntriesAfter 返回 follower 在 followerLast 之后尚未收到的日志
// （窗口外时第二个返回值为 false，调用方应改用 Catchup 重连）。
func (l *Leader) EntriesAfter(followerLast int64) ([]*pb.LogEntry, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if followerLast < l.compactIndex {
		return nil, false
	}
	start := followerLast - l.compactIndex
	return append([]*pb.LogEntry(nil), l.log[start:]...), true
}

// HasQuorumPeers 判断当前是否有足够 follower 在线以形成多数派。
// 仅用于快速失败提示；真正的提交判定在 commitLoop。
func (l *Leader) HasQuorumPeers() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	need := Quorum(len(l.cfg.NodeAddrs))
	alive := 1 // leader
	for id := range l.cfg.NodeAddrs {
		if id != l.cfg.NodeID {
			if l.match[id] > 0 {
				alive++
			}
		}
	}
	return alive >= need
}
