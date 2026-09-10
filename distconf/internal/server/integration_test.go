package server_test

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/example/distconf/api/distconf/v1"
	"github.com/example/distconf/client"
	"github.com/example/distconf/internal/server"
)

type testCluster struct {
	nodes map[int32]*server.Node
	addrs map[int32]string
}

func startCluster(t *testing.T) *testCluster {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(&testLogWriter{t}, &slog.HandlerOptions{Level: slog.LevelError}))

	addrs := map[int32]string{}
	listeners := map[int32]net.Listener{}
	for id := int32(1); id <= 3; id++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[id] = lis
		addrs[id] = lis.Addr().String()
	}

	c := &testCluster{nodes: map[int32]*server.Node{}, addrs: addrs}
	for id := int32(1); id <= 3; id++ {
		cfg := server.NodeConfig{
			NodeID:        id,
			ListenAddr:    lisAddr(listeners[id]),
			AdvertiseAddr: addrs[id],
			LeaderID:      1,
			Peers:         addrs,
			SweepInterval: 50 * time.Millisecond,
			Listener:      listeners[id],
		}
		n := server.NewNode(cfg, logger)
		if err := n.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		c.nodes[id] = n
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			n.Stop()
		}
	})
	return c
}

func lisAddr(l net.Listener) string { return l.Addr().String() }

type testLogWriter struct{ t *testing.T }

func (w *testLogWriter) Write(p []byte) (int, error) { w.t.Logf("%s", p); return len(p), nil }

func waitFor(t *testing.T, desc string, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", desc)
}

func newClientFor(c *testCluster) *client.Client {
	cl, err := client.New([]string{c.addrs[1], c.addrs[2], c.addrs[3]})
	if err != nil {
		panic(err)
	}
	return cl
}

// 1) 写入 leader，在两个 follower 上都能读到（读己之所写后跨节点可见）。
func TestIntegrationReplicationReadOnFollowers(t *testing.T) {
	c := startCluster(t)
	cl := newClientFor(c)
	defer cl.Close()
	ctx := context.Background()

	put, err := cl.Put(ctx, &pb.PutRequest{
		Namespace: "cfg", Key: "db.host", Value: []byte("10.0.0.5:5432"),
		ExpectedVersion: -1,
	})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if put.Kv.Version != 1 {
		t.Fatalf("version=%d", put.Kv.Version)
	}

	for _, id := range []int32{2, 3} {
		id := id
		waitFor(t, fmt.Sprintf("read on node %d", id), func() bool {
			kv, ok := c.nodes[id].Store().Get("cfg", "db.host")
			return ok && string(kv.Value) == "10.0.0.5:5432"
		}, 3*time.Second)
	}

	// 通过 gRPC 客户端直连 follower 地址读。
	followerOnly, err := client.New([]string{c.addrs[2]})
	if err != nil {
		t.Fatal(err)
	}
	defer followerOnly.Close()
	resp, err := followerOnly.Get(ctx, "cfg", "db.host")
	if err != nil {
		t.Fatalf("get via follower: %v", err)
	}
	if !resp.Found || string(resp.Kv.Value) != "10.0.0.5:5432" {
		t.Fatalf("follower read wrong: %+v", resp)
	}
}

// 2) follower 拒绝写，并给出 leader 提示；客户端 SDK 自动重定向成功。
func TestIntegrationWriteRedirectedFromFollower(t *testing.T) {
	c := startCluster(t)
	followerOnly, err := client.New([]string{c.addrs[2]})
	if err != nil {
		t.Fatal(err)
	}
	defer followerOnly.Close()

	// SDK 不知道 leader，先用 Info 发现。
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	info, err := followerOnly.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.LeaderId != 1 || !info.IsLeader == false && info.NodeId == 2 {
		// node 2 视角下 leader=1
	}
	if info.LeaderId != 1 {
		t.Fatalf("leader=%d", info.LeaderId)
	}

	resp, err := followerOnly.Put(ctx, &pb.PutRequest{
		Namespace: "cfg", Key: "a", Value: []byte("1"), ExpectedVersion: -1,
	})
	if err != nil {
		t.Fatalf("sdk should auto-redirect, got %v", err)
	}
	if resp.LeaderId != 1 {
		t.Fatalf("response leader=%d", resp.LeaderId)
	}

	// 直接打 follower 的 gRPC，应收到 FailedPrecondition。
	conn, err := dialRaw(c.addrs[2])
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, err = pb.NewKVServiceClient(conn).Put(ctx, &pb.PutRequest{
		Namespace: "cfg", Key: "b", Value: []byte("1"), ExpectedVersion: -1,
	})
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		t.Fatalf("want FailedPrecondition, got %v (%v)", st.Code(), err)
	}
}

// 3) Watch 在 follower 上收到有序通知；TTL 过期也产生 EXPIRED 事件。
func TestIntegrationWatchAndExpiry(t *testing.T) {
	c := startCluster(t)
	cl := newClientFor(c)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	// 直连 follower 3 订阅。
	watcherCl, err := client.New([]string{c.addrs[3]})
	if err != nil {
		t.Fatal(err)
	}
	defer watcherCl.Close()
	w := watcherCl.Watch(ctx, "cfg", "", 0)

	// 给流一点建立时间。
	time.Sleep(150 * time.Millisecond)

	if _, err := cl.Put(ctx, &pb.PutRequest{
		Namespace: "cfg", Key: "k1", Value: []byte("v1"), ExpectedVersion: -1,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Put(ctx, &pb.PutRequest{
		Namespace: "cfg", Key: "k2", Value: []byte("v2"), TtlMs: 300, ExpectedVersion: -1,
	}); err != nil {
		t.Fatal(err)
	}

	got := map[string]pb.WatchEvent_Kind{}
	var lastSeq int64 = -1
	deadline := time.Now().Add(5 * time.Second)
	for len(got) < 3 && time.Now().Before(deadline) {
		select {
		case ev, ok := <-w.Events():
			if !ok {
				t.Fatalf("watcher closed: %v", <-w.Err())
			}
			if ev.Revision <= lastSeq {
				t.Fatalf("out-of-order event seq=%d last=%d", ev.Revision, lastSeq)
			}
			lastSeq = ev.Revision
			got[ev.Kv.Key+":"+ev.Kind.String()] = ev.Kind
		case err := <-w.Err():
			t.Fatalf("watch err: %v", err)
		}
	}
	if got["k1:PUT"] != pb.WatchEvent_PUT {
		t.Fatalf("missing k1 PUT: %+v", got)
	}
	if got["k2:PUT"] != pb.WatchEvent_PUT || got["k2:EXPIRED"] != pb.WatchEvent_EXPIRED {
		t.Fatalf("missing k2 put/expired: %+v", got)
	}

	// 过期后所有节点都读不到。
	for _, id := range []int32{1, 2, 3} {
		waitFor(t, "expiry replicated", func() bool {
			_, ok := c.nodes[id].Store().Get("cfg", "k2")
			return !ok
		}, 3*time.Second)
	}
}

// 4) CAS 并发：同一 key 的并发创建只有一个成功。
func TestIntegrationConcurrentCAS(t *testing.T) {
	c := startCluster(t)
	cl := newClientFor(c)
	defer cl.Close()

	const n = 20
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			_, err := cl.Put(context.Background(), &pb.PutRequest{
				Namespace: "ns", Key: "singleton", Value: []byte("x"), ExpectedVersion: 0,
			})
			errs <- err
		}()
	}
	ok, conflict := 0, 0
	for i := 0; i < n; i++ {
		err := <-errs
		switch {
		case err == nil:
			ok++
		case err == client.ErrCASConflict:
			conflict++
		default:
			t.Fatalf("unexpected err: %v", err)
		}
	}
	if ok != 1 || conflict != n-1 {
		t.Fatalf("ok=%d conflict=%d", ok, conflict)
	}
}

// 5) 服务注册发现 + 心跳续租 + 到期摘除。
func TestIntegrationServiceLease(t *testing.T) {
	c := startCluster(t)
	cl := newClientFor(c)
	defer cl.Close()
	ctx := context.Background()

	reg, err := cl.Register(ctx, &pb.RegisterRequest{
		Namespace: "ns", Service: "orders", InstanceId: "i-1",
		Address: "10.0.0.1:8080", LeaseTtlMs: 600,
		Metadata: map[string]string{"zone": "a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if reg.Instance.ExpireAtMs == 0 {
		t.Fatal("expire_at not set")
	}

	// 每 150ms 心跳，租约不应过期。
	hbCtx, cancelHB := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(150 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				_, _ = cl.Heartbeat(ctx, &pb.HeartbeatRequest{
					Namespace: "ns", Service: "orders", InstanceId: "i-1",
				})
			}
		}
	}()
	time.Sleep(1500 * time.Millisecond)
	list, err := cl.Discover(ctx, &pb.DiscoverRequest{Namespace: "ns", Service: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Instances) != 1 {
		t.Fatalf("instance expired despite heartbeats: %d", len(list.Instances))
	}
	cancelHB()

	// 停心跳后应在约 TTL + sweep 间隔内被摘除（全集群）。
	for _, id := range []int32{1, 2, 3} {
		waitFor(t, "instance expiry replicated", func() bool {
			return len(c.nodes[id].Store().Discover("ns", "orders")) == 0
		}, 5*time.Second)
	}

	// 实例摘除后心跳返回 NotFound，需要重新注册。
	_, err = cl.Heartbeat(ctx, &pb.HeartbeatRequest{
		Namespace: "ns", Service: "orders", InstanceId: "i-1",
	})
	if err != client.ErrInstanceGone {
		t.Fatalf("want ErrInstanceGone, got %v", err)
	}
}

// 6) leader 宕机边界（与设计声明一致）：
//   - follower 读仍可用（停在已提交前缀）；
//   - 写失败（无自动提升）；
//   - leader 恢复后（新进程，空状态，静态配置）follower 会从它收到快照，
//     但该场景下 leader 数据为空 —— 这正是“无持久化、无选举”的声明边界，
//     因此恢复测试只验证“进程存活时 follower 不回退”，不验证 leader 重启。
func TestIntegrationLeaderFailureBoundary(t *testing.T) {
	c := startCluster(t)
	cl := newClientFor(c)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	if _, err := cl.Put(ctx, &pb.PutRequest{
		Namespace: "cfg", Key: "stable", Value: []byte("v1"), ExpectedVersion: -1,
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "replicated before leader stop", func() bool {
		_, ok := c.nodes[2].Store().Get("cfg", "stable")
		return ok
	}, 3*time.Second)

	// 停掉 leader。
	c.nodes[1].Stop()

	// follower 本地读仍返回已提交数据。
	if _, ok := c.nodes[2].Store().Get("cfg", "stable"); !ok {
		t.Fatal("committed data vanished on follower")
	}
	followerOnly, err := client.New([]string{c.addrs[2]})
	if err != nil {
		t.Fatal(err)
	}
	defer followerOnly.Close()
	resp, err := followerOnly.Get(ctx, "cfg", "stable")
	if err != nil || !resp.Found {
		t.Fatalf("follower read during leader outage: %v %+v", err, resp)
	}

	// 写必须失败：follower 返回 not-leader；SDK 无法发现可达 leader。
	_, err = followerOnly.Put(ctx, &pb.PutRequest{
		Namespace: "cfg", Key: "new", Value: []byte("x"), ExpectedVersion: -1,
	})
	if err == nil {
		t.Fatal("write unexpectedly succeeded with leader down")
	}

	// 直连 follower 的原始写仍应得到 FailedPrecondition。
	conn, err := dialRaw(c.addrs[3])
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, err = pb.NewKVServiceClient(conn).Put(ctx, &pb.PutRequest{
		Namespace: "cfg", Key: "new2", Value: []byte("x"), ExpectedVersion: -1,
	})
	if st, ok := status.FromError(err); !ok || st.Code() != codes.FailedPrecondition {
		t.Fatalf("want FailedPrecondition on follower, got %v", err)
	}
}
