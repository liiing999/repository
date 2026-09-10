package sweeper

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	pb "github.com/example/distconf/api/distconf/v1"
	"github.com/example/distconf/internal/repl"
	"github.com/example/distconf/internal/store"
)

// memProposer 把提议的日志立即按序应用到真实 store，模拟
// “单节点可提交的 leader”（无需网络/复制层）。
type memProposer struct {
	st *store.Store
	ap repl.Applier
}

func (m *memProposer) Propose(_ context.Context, e *pb.LogEntry) (repl.Result, error) {
	e.Index = m.st.AppliedRevision() + 1
	e.Term = 1
	kv, inst, found, err := applyOne(m.ap, e)
	return repl.Result{KV: kv, Inst: inst, Found: found, Err: err}, nil
}

func applyOne(a repl.Applier, e *pb.LogEntry) (*pb.KeyValue, *pb.Instance, bool, error) {
	switch op := e.Op.(type) {
	case *pb.LogEntry_Put:
		kv, err := a.ApplyPut(e.Index, op.Put)
		return kv, nil, false, err
	case *pb.LogEntry_Delete:
		found, err := a.ApplyDelete(e.Index, op.Delete)
		return nil, nil, found, err
	case *pb.LogEntry_Register:
		return nil, a.ApplyRegister(e.Index, op.Register), false, nil
	case *pb.LogEntry_Heartbeat:
		inst, err := a.ApplyHeartbeat(e.Index, op.Heartbeat)
		return nil, inst, false, err
	case *pb.LogEntry_ExpireKeys:
		a.ApplyExpireKeys(e.Index, op.ExpireKeys)
	case *pb.LogEntry_ExpireServices:
		a.ApplyExpireServices(e.Index, op.ExpireServices)
	}
	return nil, nil, false, nil
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestSweeperRemovesExpiredKVAndInstances(t *testing.T) {
	st := store.New()
	now := int64(1_000_000)
	clock := now
	st.SetNowForTest(func() int64 { return clock })

	ap := repl.StoreApplier{St: st}
	prop := &memProposer{st: st, ap: ap}
	sw := New(st, prop, time.Hour, quietLogger())

	// 写入一个 TTL=1000 的 key 和实例。
	if _, err := prop.Propose(context.Background(), &pb.LogEntry{Op: &pb.LogEntry_Put{
		Put: &pb.PutOp{Namespace: "ns", Key: "k", Value: []byte("v"),
			TtlMs: 1000, ExpireAtMs: now + 1000, ExpectedVersion: 0},
	}}); err != nil {
		t.Fatal(err)
	}
	prop.Propose(context.Background(), &pb.LogEntry{Op: &pb.LogEntry_Register{
		Register: &pb.RegisterOp{Namespace: "ns", Service: "svc", InstanceId: "i",
			Address: "h:1", LeaseTtlMs: 1000, ExpireAtMs: now + 1000},
	}})

	if _, ok := st.Get("ns", "k"); !ok {
		t.Fatal("kv should exist")
	}
	if len(st.Discover("ns", "svc")) != 1 {
		t.Fatal("instance should exist")
	}

	// 未到期：sweep 不应删。
	sw.TickOnce()
	if _, ok := st.Get("ns", "k"); !ok {
		t.Fatal("kv removed too early")
	}

	// 到期：一轮 sweep 后两者都应消失。
	clock = now + 1001
	sw.TickOnce()

	if _, ok := st.Get("ns", "k"); ok {
		t.Fatal("expired kv not removed")
	}
	if len(st.Discover("ns", "svc")) != 0 {
		t.Fatal("expired instance not removed")
	}

	// 历史中应能查到 EXPIRED 事件。
	_, backlog, err := st.WatchKV("ns", "k", 0)
	if err != nil {
		t.Fatal(err)
	}
	var sawExpired bool
	for _, ev := range backlog {
		if ev.Kind == pb.WatchEvent_EXPIRED {
			sawExpired = true
		}
	}
	if !sawExpired {
		t.Fatal("EXPIRED event missing from history")
	}
}

func TestSweeperGuardDoesNotDeleteRefreshedKey(t *testing.T) {
	st := store.New()
	now := int64(2_000_000)
	clock := now
	st.SetNowForTest(func() int64 { return clock })

	ap := repl.StoreApplier{St: st}
	prop := &memProposer{st: st, ap: ap}
	sw := New(st, prop, time.Hour, quietLogger())

	prop.Propose(context.Background(), &pb.LogEntry{Op: &pb.LogEntry_Put{
		Put: &pb.PutOp{Namespace: "ns", Key: "k", Value: []byte("v1"),
			TtlMs: 1000, ExpireAtMs: now + 1000, ExpectedVersion: 0},
	}})
	clock = now + 1001
	// 扫描发现到期后、sweep 提议前，key 被带新 TTL 的 Put 刷新
	// （过期 key 已视为不存在，故无条件写）。
	prop.Propose(context.Background(), &pb.LogEntry{Op: &pb.LogEntry_Put{
		Put: &pb.PutOp{Namespace: "ns", Key: "k", Value: []byte("v2"),
			TtlMs: 10_000, ExpireAtMs: clock + 10_000, ExpectedVersion: -1},
	}})
	sw.TickOnce()

	got, ok := st.Get("ns", "k")
	if !ok || string(got.Value) != "v2" {
		t.Fatalf("refreshed key wrongly swept: ok=%v", ok)
	}
}

func TestSweeperStartStop(t *testing.T) {
	st := store.New()
	ap := repl.StoreApplier{St: st}
	sw := New(st, &memProposer{st: st, ap: ap}, 20*time.Millisecond, quietLogger())
	sw.Start()
	sw.Stop()
	sw.Stop() // 幂等
}
