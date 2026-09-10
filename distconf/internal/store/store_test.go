package store

import (
	"errors"
	"sync"
	"testing"
	"time"

	pb "github.com/example/distconf/api/distconf/v1"
)

func putOp(ns, key, val string, expect int64, ttl, expireAt int64) *pb.PutOp {
	return &pb.PutOp{Namespace: ns, Key: key, Value: []byte(val),
		ExpectedVersion: expect, TtlMs: ttl, ExpireAtMs: expireAt}
}

func TestCASVersions(t *testing.T) {
	s := New()

	kv, err := s.ApplyPut(1, putOp("ns", "k", "v1", 0, 0, 0))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if kv.Version != 1 || kv.Revision != 1 {
		t.Fatalf("got version=%d rev=%d, want 1/1", kv.Version, kv.Revision)
	}

	// 用旧版本号并发语义：错误的期望版本必须冲突。
	if _, err := s.ApplyPut(2, putOp("ns", "k", "v2", 5, 0, 0)); !errors.Is(err, ErrCASConflict) {
		t.Fatalf("stale version: want ErrCASConflict, got %v", err)
	}
	// 冲突不推进 revision，也不覆盖值。
	if got, _ := s.Get("ns", "k"); string(got.Value) != "v1" {
		t.Fatalf("conflicting put must not overwrite, got %q", got.Value)
	}
	// 冲突条目不产生事件、不覆盖值，但仍占用日志 index，
	// 状态机 applied revision 照常推进（所有节点一致）。
	if s.AppliedRevision() != 2 {
		t.Fatalf("applied revision want 2 (conflict still consumes log index), got %d", s.AppliedRevision())
	}

	// 正确版本号更新成功，version +1。
	kv, err = s.ApplyPut(3, putOp("ns", "k", "v2", 1, 0, 0))
	if err != nil {
		t.Fatalf("update v1->v2: %v", err)
	}
	if kv.Version != 2 {
		t.Fatalf("want version 2, got %d", kv.Version)
	}

	// expected=0 对已存在 key 冲突。
	if _, err := s.ApplyPut(4, putOp("ns", "k", "v3", 0, 0, 0)); !errors.Is(err, ErrCASConflict) {
		t.Fatalf("create-existing: want conflict, got %v", err)
	}
	// expected=-1 无条件。
	if _, err := s.ApplyPut(5, putOp("ns", "k", "v3", -1, 0, 0)); err != nil {
		t.Fatalf("blind put: %v", err)
	}

	// Delete 的 CAS。
	if _, err := s.ApplyDelete(6, &pb.DeleteOp{Namespace: "ns", Key: "k", ExpectedVersion: 99}); !errors.Is(err, ErrCASConflict) {
		t.Fatalf("delete cas: want conflict, got %v", err)
	}
	found, err := s.ApplyDelete(7, &pb.DeleteOp{Namespace: "ns", Key: "k", ExpectedVersion: 3})
	if err != nil || !found {
		t.Fatalf("delete v3: found=%v err=%v", found, err)
	}
	if _, ok := s.Get("ns", "k"); ok {
		t.Fatal("key still present after delete")
	}
	// 删除后重建 version 从 1 开始。
	kv, _ = s.ApplyPut(8, putOp("ns", "k", "v4", 0, 0, 0))
	if kv.Version != 1 {
		t.Fatalf("recreated key version: want 1, got %d", kv.Version)
	}
}

func TestConcurrentPutsSingleWinner(t *testing.T) {
	s := New()
	// 所有 goroutine 都基于 version=0（key 不存在）做创建：只有一个成功。
	const n = 50
	var wg sync.WaitGroup
	var wins, conflicts int64
	var mu sync.Mutex
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// 用串行 index 模拟日志顺序（ApplyPut 本身就是序列化点）。
			_, err := s.ApplyPut(int64(i+1), putOp("ns", "k", "v", 0, 0, 0))
			mu.Lock()
			if err == nil {
				wins++
			} else if errors.Is(err, ErrCASConflict) {
				conflicts++
			}
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()
	if wins != 1 || conflicts != n-1 {
		t.Fatalf("wins=%d conflicts=%d, want 1/%d", wins, conflicts, n-1)
	}
}

func TestWatchBacklogAndReconnect(t *testing.T) {
	s := New()
	for i := 1; i <= 5; i++ {
		if _, err := s.ApplyPut(int64(i), putOp("ns", "k", "v", -1, 0, 0)); err != nil {
			t.Fatal(err)
		}
	}

	// 从游标 0 订阅：应收到 5 条有序历史。
	id, backlog, err := s.WatchKV("ns", "k", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(backlog) != 5 {
		t.Fatalf("backlog=%d, want 5", len(backlog))
	}
	var prev int64
	for _, ev := range backlog {
		if ev.Revision <= prev {
			t.Fatal("backlog not strictly ordered")
		}
		prev = ev.Revision
	}

	// 续订阅：从最后游标开始，历史为空，实时收到下一条。
	last := backlog[4].Revision
	s.CloseKVWatcher(id)
	id, backlog, err = s.WatchKV("ns", "k", last)
	if err != nil || len(backlog) != 0 {
		t.Fatalf("resume: backlog=%d err=%v", len(backlog), err)
	}
	s.ApplyPut(6, putOp("ns", "k", "v6", -1, 0, 0))
	ev := <-s.KVWatcherChan(id)
	if ev.Revision <= last || ev.Kind != pb.WatchEvent_PUT {
		t.Fatalf("live event wrong: %+v", ev)
	}
	s.CloseKVWatcher(id)
}

func TestWatchNamespacePrefix(t *testing.T) {
	s := New()
	id, backlog, err := s.WatchKV("ns", "", 0) // 整个 namespace，先订阅
	if err != nil {
		t.Fatal(err)
	}
	if len(backlog) != 0 {
		t.Fatalf("fresh watcher backlog=%d, want 0", len(backlog))
	}
	s.ApplyPut(1, putOp("ns", "a", "1", 0, 0, 0))
	s.ApplyPut(2, putOp("ns", "b", "2", 0, 0, 0))
	s.ApplyPut(3, putOp("other", "a", "x", 0, 0, 0))
	s.ApplyDelete(4, &pb.DeleteOp{Namespace: "ns", Key: "a", ExpectedVersion: -1})

	// 实时应收到 ns 下 3 条（a put, b put, a delete），不含 other。
	ch := s.KVWatcherChan(id)
	var got []*pb.WatchEvent
	for len(got) < 3 {
		select {
		case ev := <-ch:
			got = append(got, ev)
		case <-time.After(time.Second):
			t.Fatalf("only got %d live events", len(got))
		}
	}
	if got[0].Kind != pb.WatchEvent_PUT || got[0].Kv.Key != "a" ||
		got[1].Kv.Key != "b" ||
		got[2].Kind != pb.WatchEvent_DELETE {
		t.Fatalf("unexpected events: %+v", got)
	}
	s.CloseKVWatcher(id)
}

func TestBatchExpiryEventsSameRevision(t *testing.T) {
	s := New()
	// 3 个 key，同一条过期日志（index 4）内过期。
	for i, key := range []string{"a", "b", "c"} {
		if _, err := s.ApplyPut(int64(i+1), putOp("ns", key, "v", -1, 0, 0)); err != nil {
			t.Fatal(err)
		}
	}
	op := &pb.ExpireKeysOp{Items: []*pb.ExpireKey{
		{Namespace: "ns", Key: "a", GuardRevision: 1},
		{Namespace: "ns", Key: "b", GuardRevision: 2},
		{Namespace: "ns", Key: "c", GuardRevision: 3},
	}}
	s.ApplyExpireKeys(4, op)

	id, backlog, err := s.WatchKV("ns", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	// 3 put + 3 expired。
	if len(backlog) != 6 {
		t.Fatalf("events=%d, want 6", len(backlog))
	}
	expired := backlog[3:]
	for i, ev := range expired {
		if ev.Kind != pb.WatchEvent_EXPIRED {
			t.Fatalf("event %d not EXPIRED", i)
		}
	}
	// 游标必须严格递增且每条唯一。
	for i := 1; i < len(backlog); i++ {
		if backlog[i].Revision <= backlog[i-1].Revision {
			t.Fatalf("event cursors not strictly increasing at %d", i)
		}
	}
	// 从第 2 个过期事件的游标续传，只能收到第 3 个（不能重复/丢失）。
	cursor := expired[1].Revision
	s.CloseKVWatcher(id)
	_, backlog2, err := s.WatchKV("ns", "", cursor)
	if err != nil {
		t.Fatal(err)
	}
	if len(backlog2) != 1 || backlog2[0].Kv.Key != "c" {
		t.Fatalf("mid-batch resume wrong: %d events", len(backlog2))
	}
}

func TestExpiryGuard(t *testing.T) {
	s := New()
	kv, _ := s.ApplyPut(1, putOp("ns", "k", "v1", 0, 0, 0))
	// 扫描后、提交前 key 被重写（revision 变为 2）。
	s.ApplyPut(2, putOp("ns", "k", "v2", kv.Version, 0, 0))
	// 旧扫描结果（guard=1）不得删除新值。
	s.ApplyExpireKeys(3, &pb.ExpireKeysOp{Items: []*pb.ExpireKey{
		{Namespace: "ns", Key: "k", GuardRevision: 1},
	}})
	got, ok := s.Get("ns", "k")
	if !ok || string(got.Value) != "v2" {
		t.Fatalf("guard failed: ok=%v value=%q", ok, got.GetValue())
	}
}

func TestTTLExpiryLazyAndSweep(t *testing.T) {
	now := int64(1_000_000)
	s := New()
	s.SetNowForTest(func() int64 { return now })

	s.ApplyPut(1, putOp("ns", "k", "v", 0, 5_000, now+5_000))
	if _, ok := s.Get("ns", "k"); !ok {
		t.Fatal("should be alive")
	}
	now += 5_001
	if _, ok := s.Get("ns", "k"); ok {
		t.Fatal("should be logically expired")
	}
	expired := s.ScanExpiredKVs()
	if len(expired) != 1 || expired[0].GuardRevision != 1 {
		t.Fatalf("scan: %+v", expired)
	}
}

func TestServiceLeaseLifecycle(t *testing.T) {
	now := int64(1_000_000)
	s := New()
	s.SetNowForTest(func() int64 { return now })

	reg := &pb.RegisterOp{Namespace: "ns", Service: "svc", InstanceId: "i1",
		Address: "10.0.0.1:80", LeaseTtlMs: 5_000, ExpireAtMs: now + 5_000}
	inst := s.ApplyRegister(1, reg)
	if inst.ExpireAtMs != now+5_000 {
		t.Fatal("register expiry wrong")
	}
	if list := s.Discover("ns", "svc"); len(list) != 1 {
		t.Fatalf("discover: %d", len(list))
	}

	// 心跳续租。
	if _, err := s.ApplyHeartbeat(2, &pb.HeartbeatOp{
		Namespace: "ns", Service: "svc", InstanceId: "i1",
		GuardRevision: 1, NewExpireAtMs: now + 10_000,
	}); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	now += 6_000
	// 原租约已过，但心跳续租到 10005，应仍存活。
	if list := s.Discover("ns", "svc"); len(list) != 1 {
		t.Fatalf("after hb: discover=%d", len(list))
	}

	// guard 不匹配（实例已被摘除后重放的心跳）→ not found。
	s.ApplyExpireServices(3, &pb.ExpireServicesOp{Items: []*pb.ExpireService{
		{Namespace: "ns", Service: "svc", InstanceId: "i1", GuardRevision: 2},
	}})
	if _, err := s.ApplyHeartbeat(4, &pb.HeartbeatOp{
		Namespace: "ns", Service: "svc", InstanceId: "i1", GuardRevision: 2, NewExpireAtMs: now + 999,
	}); !errors.Is(err, ErrInstanceNotFound) {
		t.Fatalf("hb after expiry: want not found, got %v", err)
	}
	if list := s.Discover("ns", "svc"); len(list) != 0 {
		t.Fatalf("discover after expiry: %d", len(list))
	}
}

func TestSnapshotRestore(t *testing.T) {
	s := New()
	s.ApplyPut(1, putOp("ns", "k1", "a", 0, 0, 0))
	s.ApplyPut(2, putOp("ns", "k2", "b", 0, 0, 0))
	s.ApplyRegister(3, &pb.RegisterOp{Namespace: "ns", Service: "svc",
		InstanceId: "i1", Address: "h:1"})

	snap := s.ExportSnapshot()
	s2 := New()
	s2.RestoreSnapshot(snap)
	if s2.AppliedRevision() != 3 {
		t.Fatalf("restored rev=%d", s2.AppliedRevision())
	}
	if v, ok := s2.Get("ns", "k2"); !ok || string(v.Value) != "b" {
		t.Fatal("restored kv missing")
	}
	if list := s2.Discover("ns", "svc"); len(list) != 1 {
		t.Fatalf("restored instance missing: %d", len(list))
	}
	// 恢复后继续应用，revision 接着涨。
	s2.ApplyPut(4, putOp("ns", "k3", "c", 0, 0, 0))
	if v, ok := s2.Get("ns", "k3"); !ok || v.Revision != 4 {
		t.Fatal("put after restore wrong")
	}
}
