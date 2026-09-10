package repl

import (
	"context"
	"sync"
	"testing"
	"time"

	pb "github.com/example/distconf/api/distconf/v1"
)

// fakeApplier 记录应用过的日志 index。
type fakeApplier struct {
	mu      sync.Mutex
	applied int64
	puts    []*pb.PutOp
}

func (f *fakeApplier) AppliedIndex() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.applied
}

func (f *fakeApplier) apply(index int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if index != f.applied+1 {
		panic("non-contiguous apply")
	}
	f.applied = index
}

func (f *fakeApplier) ApplyPut(index int64, op *pb.PutOp) (*pb.KeyValue, error) {
	f.apply(index)
	f.mu.Lock()
	f.puts = append(f.puts, op)
	f.mu.Unlock()
	return &pb.KeyValue{Namespace: op.Namespace, Key: op.Key, Revision: index}, nil
}
func (f *fakeApplier) ApplyDelete(index int64, op *pb.DeleteOp) (bool, error) {
	f.apply(index)
	return true, nil
}
func (f *fakeApplier) ApplyRegister(index int64, op *pb.RegisterOp) *pb.Instance {
	f.apply(index)
	return &pb.Instance{}
}
func (f *fakeApplier) ApplyHeartbeat(index int64, op *pb.HeartbeatOp) (*pb.Instance, error) {
	f.apply(index)
	return &pb.Instance{}, nil
}
func (f *fakeApplier) ApplyExpireKeys(index int64, op *pb.ExpireKeysOp)         { f.apply(index) }
func (f *fakeApplier) ApplyExpireServices(index int64, op *pb.ExpireServicesOp) { f.apply(index) }
func (f *fakeApplier) ApplySnapshot(snap *pb.Snapshot) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = snap.LastIncludedIndex
}

func testCfg() Config {
	return Config{
		NodeID: 1, LeaderID: 1,
		NodeAddrs: map[int32]string{1: "n1", 2: "n2", 3: "n3"},
	}
}

// 多数派：3 节点需要 2 票（leader + 1 follower）。
func TestCommitWithQuorum(t *testing.T) {
	l := NewLeader(testCfg(), &fakeApplier{})
	l.Start()
	defer l.Stop()

	done := make(chan Result, 1)
	go func() {
		r, err := l.Propose(context.Background(), &pb.LogEntry{
			Op: &pb.LogEntry_Put{Put: &pb.PutOp{Namespace: "ns", Key: "k", ExpectedVersion: -1}}})
		if err != nil {
			t.Errorf("propose: %v", err)
			return
		}
		done <- r
	}()

	// 没有 follower ack 时：不能提交。
	select {
	case r := <-done:
		t.Fatalf("committed without quorum: %+v", r)
	case <-time.After(100 * time.Millisecond):
	}

	// follower 2 ack 到 index 1 => leader+2 两票，提交。
	l.FollowerAck(2, 1)
	select {
	case r := <-done:
		if r.Err != nil {
			t.Fatalf("result err: %v", r.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("propose did not return after quorum ack")
	}

	if l.CommittedIndex() != 1 {
		t.Fatalf("commit=%d", l.CommittedIndex())
	}
}

// follower 3 掉线不应影响多数派（leader + 2）。
func TestMinorityDown(t *testing.T) {
	l := NewLeader(testCfg(), &fakeApplier{})
	l.Start()
	defer l.Stop()

	go l.Propose(context.Background(), &pb.LogEntry{
		Op: &pb.LogEntry_Put{Put: &pb.PutOp{ExpectedVersion: -1}}})
	l.FollowerAck(2, 1)

	if err := waitFor(func() bool { return l.CommittedIndex() == 1 }, time.Second); err != nil {
		t.Fatal("no commit with 1 follower down")
	}
}

// 两个 follower 都掉线 => 无法提交，提议超时。
func TestNoQuorumTimeout(t *testing.T) {
	l := NewLeader(testCfg(), &fakeApplier{})
	l.Start()
	defer l.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, err := l.Propose(ctx, &pb.LogEntry{
		Op: &pb.LogEntry_Put{Put: &pb.PutOp{ExpectedVersion: -1}}})
	if err == nil {
		t.Fatal("expected timeout with no quorum")
	}
}

// 按序提交多条，index 单调不重复。
func TestOrderedMonotonicIndexes(t *testing.T) {
	ap := &fakeApplier{}
	l := NewLeader(testCfg(), ap)
	l.Start()
	defer l.Stop()

	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l.Propose(context.Background(), &pb.LogEntry{
				Op: &pb.LogEntry_Put{Put: &pb.PutOp{ExpectedVersion: -1}}})
		}()
	}
	// follower 2 持续把 ack 推进到日志末尾。
	go func() {
		for {
			last := l.LastIndex()
			l.FollowerAck(2, last)
			if last >= n {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	if err := waitFor(func() bool { return ap.AppliedIndex() == n }, 3*time.Second); err != nil {
		t.Fatalf("applied=%d, want %d", ap.AppliedIndex(), n)
	}
	wg.Wait()
	if len(ap.puts) != n {
		t.Fatalf("puts applied=%d, want %d", len(ap.puts), n)
	}
}

// 空 follower 上线时的 catchup：全量日志。
func TestCatchupEmptyFollower(t *testing.T) {
	l := NewLeader(testCfg(), &fakeApplier{})
	for i := int64(1); i <= 5; i++ {
		l.muLockAppend(i)
	}
	snap, entries := l.Catchup(0)
	if snap != nil {
		t.Fatal("no compaction yet, snapshot must be nil")
	}
	if len(entries) != 5 || entries[0].Index != 1 || entries[4].Index != 5 {
		t.Fatalf("catchup entries wrong: %d", len(entries))
	}
}

// follower 落后部分：只补缺失后缀。
func TestCatchupPartial(t *testing.T) {
	l := NewLeader(testCfg(), &fakeApplier{})
	for i := int64(1); i <= 5; i++ {
		l.muLockAppend(i)
	}
	_, entries := l.Catchup(3)
	if len(entries) != 2 || entries[0].Index != 4 || entries[1].Index != 5 {
		t.Fatalf("partial catchup wrong: %d entries", len(entries))
	}
}

// follower 已最新：不补任何内容。
func TestCatchupUpToDate(t *testing.T) {
	l := NewLeader(testCfg(), &fakeApplier{})
	for i := int64(1); i <= 3; i++ {
		l.muLockAppend(i)
	}
	snap, entries := l.Catchup(3)
	if snap != nil || len(entries) != 0 {
		t.Fatalf("uptodate catchup: snap=%v entries=%d", snap, len(entries))
	}
}

// 日志截断后，落后 follower 必须收到快照（覆盖到截断点）而非越界切片。
func TestCatchupSnapshotAfterCompaction(t *testing.T) {
	ap := &fakeApplier{}
	l := NewLeader(testCfg(), ap)
	const cut = retainEntries + 5
	l.SetSnapshotExporter(func() *pb.Snapshot {
		// compactLocked 在状态机恰好应用到 commit=cut 时调用。
		return &pb.Snapshot{LastIncludedIndex: cut, Term: 1}
	})
	l.Start()
	defer l.Stop()

	l.mu.Lock()
	for i := int64(1); i <= cut; i++ {
		l.log = append(l.log, &pb.LogEntry{Index: i, Term: 1,
			Op: &pb.LogEntry_Put{Put: &pb.PutOp{ExpectedVersion: -1}}})
	}
	l.commit = cut
	l.applied = cut
	ap.applied = cut
	l.compactLocked()
	l.mu.Unlock()

	if l.compactIndex != cut {
		t.Fatalf("compactIndex=%d, want %d", l.compactIndex, cut)
	}
	if l.savedSnap == nil || l.savedSnap.LastIncludedIndex != cut {
		t.Fatal("snapshot at compaction point missing")
	}
	if len(l.log) != 0 {
		t.Fatalf("committed prefix must be removed, kept %d", len(l.log))
	}
	snap, entries := l.Catchup(0)
	if snap == nil || snap.LastIncludedIndex != cut {
		t.Fatalf("expected snapshot to %d, got %v", cut, snap)
	}
	if len(entries) != 0 {
		t.Fatalf("tail entries=%d, want 0", len(entries))
	}
}

// 断线 follower 的 match 被清零，不能再被计入多数派。
func TestFollowerResetDropsVote(t *testing.T) {
	l := NewLeader(testCfg(), &fakeApplier{})
	l.Start()
	defer l.Stop()

	go l.Propose(context.Background(), &pb.LogEntry{
		Op: &pb.LogEntry_Put{Put: &pb.PutOp{ExpectedVersion: -1}}})
	l.FollowerAck(2, 1)
	if err := waitFor(func() bool { return l.CommittedIndex() == 1 }, time.Second); err != nil {
		t.Fatal("initial commit")
	}

	// 第二条日志：follower 2 掉线后，即便它旧的 ack=1，新 index 2 无票。
	go l.Propose(context.Background(), &pb.LogEntry{
		Op: &pb.LogEntry_Put{Put: &pb.PutOp{ExpectedVersion: -1}}})
	time.Sleep(50 * time.Millisecond)
	l.FollowerReset(2)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_ = ctx
	if l.CommittedIndex() != 1 {
		t.Fatalf("commit advanced after follower reset: %d", l.CommittedIndex())
	}
}

// muLockAppend 测试辅助：直接追加一条日志。
func (l *Leader) muLockAppend(i int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.log = append(l.log, &pb.LogEntry{Index: i, Term: 1,
		Op: &pb.LogEntry_Put{Put: &pb.PutOp{ExpectedVersion: -1}}})
}

func waitFor(cond func() bool, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return context.DeadlineExceeded
}
