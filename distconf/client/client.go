package client

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "github.com/example/distconf/api/distconf/v1"
)

// ErrNoEndpoints 在未提供任何节点地址时返回。
var ErrNoEndpoints = errors.New("distconf: no endpoints provided")

// Client 是连接到一个 distconf 集群的客户端。内部维护到各节点的
// 懒连接；写自动找 leader，读写互不阻塞。
type Client struct {
	endpoints []string

	mu      sync.RWMutex
	conns   map[string]*grpc.ClientConn
	leader  string
	rrIndex uint64
}

// New 创建客户端。endpoints 是一个或多个节点地址（用于读/发现 leader）。
func New(endpoints []string) (*Client, error) {
	if len(endpoints) == 0 {
		return nil, ErrNoEndpoints
	}
	return &Client{
		endpoints: append([]string(nil), endpoints...),
		conns:     make(map[string]*grpc.ClientConn),
	}, nil
}

// Close 关闭全部底层连接。
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var firstErr error
	for addr, conn := range c.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(c.conns, addr)
	}
	return firstErr
}

func (c *Client) conn(addr string) (*grpc.ClientConn, error) {
	c.mu.RLock()
	conn, ok := c.conns[addr]
	c.mu.RUnlock()
	if ok {
		return conn, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok = c.conns[addr]; ok {
		return conn, nil
	}
	// grpc.NewClient 懒连接，不会因为节点暂时不可达而失败；
	// 单 host:port 走默认 pick_first 策略。
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c.conns[addr] = conn
	return conn, nil
}

// pickEndpoint 轮询返回一个节点地址。
func (c *Client) pickEndpoint() string {
	idx := atomic.AddUint64(&c.rrIndex, 1)
	return c.endpoints[int(idx-1)%len(c.endpoints)]
}

func (c *Client) kvClient(addr string) (pb.KVServiceClient, error) {
	conn, err := c.conn(addr)
	if err != nil {
		return nil, err
	}
	return pb.NewKVServiceClient(conn), nil
}

func (c *Client) registryClient(addr string) (pb.RegistryServiceClient, error) {
	conn, err := c.conn(addr)
	if err != nil {
		return nil, err
	}
	return pb.NewRegistryServiceClient(conn), nil
}

func (c *Client) clusterClient(addr string) (pb.ClusterServiceClient, error) {
	conn, err := c.conn(addr)
	if err != nil {
		return nil, err
	}
	return pb.NewClusterServiceClient(conn), nil
}

// Info 查询任意可达节点的集群信息（可用于发现 leader）。
func (c *Client) Info(ctx context.Context) (*pb.InfoResponse, error) {
	var lastErr error
	start := int(atomic.AddUint64(&c.rrIndex, 1) - 1)
	for i := 0; i < len(c.endpoints); i++ {
		addr := c.endpoints[(start+i)%len(c.endpoints)]
		cl, err := c.clusterClient(addr)
		if err != nil {
			lastErr = err
			continue
		}
		info, err := cl.Info(ctx, &pb.InfoRequest{})
		if err == nil {
			return info, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

// discoverLeader 通过任意可达节点的 Info 找到 leader 地址。
func (c *Client) discoverLeader(ctx context.Context) (string, error) {
	var lastErr error
	// 从轮询起点开始尝试，避免每次都打同一个节点。
	start := int(atomic.AddUint64(&c.rrIndex, 1) - 1)
	for i := 0; i < len(c.endpoints); i++ {
		addr := c.endpoints[(start+i)%len(c.endpoints)]
		cl, err := c.clusterClient(addr)
		if err != nil {
			lastErr = err
			continue
		}
		info, err := cl.Info(ctx, &pb.InfoRequest{})
		if err != nil {
			lastErr = err
			continue
		}
		if la := leaderAddress(info, c.endpoints); la != "" {
			c.mu.Lock()
			c.leader = la
			c.mu.Unlock()
			return la, nil
		}
	}
	return "", lastErr
}

// leaderAddress 从 Info 中解析 leader 对外地址。
func leaderAddress(info *pb.InfoResponse, endpoints []string) string {
	for _, n := range info.Nodes {
		if n.Id == info.LeaderId {
			return n.Address
		}
	}
	return ""
}

func (c *Client) knownLeader() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.leader
}

// ensureLeader 返回当前 leader，必要时主动发现。
func (c *Client) ensureLeader(ctx context.Context) (string, error) {
	if l := c.knownLeader(); l != "" {
		return l, nil
	}
	return c.discoverLeader(ctx)
}

// ----------------------------------------------------------------
// KV
// ----------------------------------------------------------------

// Get 从任意节点读取（顺序一致：可能稍旧但不会回退）。
func (c *Client) Get(ctx context.Context, namespace, key string) (*pb.GetResponse, error) {
	var lastErr error
	start := int(atomic.AddUint64(&c.rrIndex, 1) - 1)
	for i := 0; i < len(c.endpoints); i++ {
		addr := c.endpoints[(start+i)%len(c.endpoints)]
		cl, err := c.kvClient(addr)
		if err != nil {
			lastErr = err
			continue
		}
		resp, err := cl.Get(ctx, &pb.GetRequest{Namespace: namespace, Key: key})
		if err == nil {
			return resp, nil
		}
		lastErr = mapStatus(err)
		if !isRetryableTransport(err) {
			return nil, mapStatus(err)
		}
	}
	return nil, lastErr
}

// Put 写 leader；遇到 leader 提示/传输错误时自动发现并重试一次。
// req.ExpectedVersion 语义：0=key 必须不存在（proto 默认值即此语义），
// -1=无条件，>0=版本必须相等。
func (c *Client) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	for attempt := 0; attempt < 2; attempt++ {
		addr, err := c.ensureLeader(ctx)
		if err != nil {
			return nil, err
		}
		cl, err := c.kvClient(addr)
		if err != nil {
			return nil, err
		}
		resp, err := cl.Put(ctx, req)
		if err == nil {
			return resp, nil
		}
		mapped := mapStatus(err)
		if mapped == ErrNotLeader {
			if hint := parseLeaderHint(err); hint != "" {
				c.mu.Lock()
				c.leader = hint
				c.mu.Unlock()
			} else {
				_, _ = c.discoverLeader(ctx)
			}
			continue
		}
		if isRetryableTransport(err) && attempt == 0 {
			c.mu.Lock()
			c.leader = ""
			c.mu.Unlock()
			continue
		}
		return nil, mapped
	}
	return nil, ErrNoQuorum
}

// Delete 删除 key（expectedVersion=-1 无条件）。
func (c *Client) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if req.ExpectedVersion == 0 {
		req.ExpectedVersion = -1
	}
	for attempt := 0; attempt < 2; attempt++ {
		addr, err := c.ensureLeader(ctx)
		if err != nil {
			return nil, err
		}
		cl, err := c.kvClient(addr)
		if err != nil {
			return nil, err
		}
		resp, err := cl.Delete(ctx, req)
		if err == nil {
			return resp, nil
		}
		if mapStatus(err) == ErrNotLeader {
			if hint := parseLeaderHint(err); hint != "" {
				c.mu.Lock()
				c.leader = hint
				c.mu.Unlock()
			} else {
				_, _ = c.discoverLeader(ctx)
			}
			continue
		}
		if isRetryableTransport(err) && attempt == 0 {
			c.mu.Lock()
			c.leader = ""
			c.mu.Unlock()
			continue
		}
		return nil, mapStatus(err)
	}
	return nil, ErrNoQuorum
}

// ----------------------------------------------------------------
// Watch
// ----------------------------------------------------------------

// Watcher 是一次持续订阅。
type Watcher struct {
	events chan *pb.WatchEvent
	err    chan error
	done   chan struct{}
	once   sync.Once
}

// Events 返回有序事件通道（游标严格递增、不丢事件）。
func (w *Watcher) Events() <-chan *pb.WatchEvent { return w.events }

// Err 在订阅终止（历史被压缩或 ctx 取消且无法重连）时返回错误。
func (w *Watcher) Err() <-chan error { return w.err }

// Close 主动结束订阅。
func (w *Watcher) Close() {
	w.once.Do(func() { close(w.done) })
}

// Watch 订阅 namespace/key（key 为空 = 整个 namespace）。
// fromSeq 为上次收到的最后事件游标，0 表示只接收订阅之后的新事件。
// 底层流断开时自动用最后游标换节点重连续传；只有历史已被压缩
// （ErrHistoryCompacted）或调用方 ctx 取消时才终止。
func (c *Client) Watch(ctx context.Context, namespace, key string, fromSeq int64) *Watcher {
	w := &Watcher{
		events: make(chan *pb.WatchEvent, 64),
		err:    make(chan error, 1),
		done:   make(chan struct{}),
	}
	go c.watchLoop(ctx, w, namespace, key, fromSeq)
	return w
}

func (c *Client) watchLoop(ctx context.Context, w *Watcher, namespace, key string, fromSeq int64) {
	defer close(w.events)
	cursor := fromSeq
	backoff := 200 * time.Millisecond

	for {
		select {
		case <-ctx.Done():
			w.err <- ctx.Err()
			return
		case <-w.done:
			return
		default:
		}

		addr := c.pickEndpoint()
		cl, err := c.kvClient(addr)
		if err == nil {
			var stream pb.KVService_WatchClient
			stream, err = cl.Watch(ctx, &pb.WatchRequest{
				Namespace:    namespace,
				Key:          key,
				FromRevision: cursor,
			})
			if err == nil {
				err = c.pumpWatch(stream, w, &cursor)
			}
		}

		if err == nil {
			return // w.done / ctx 结束
		}
		if mapStatus(err) == ErrHistoryCompacted {
			w.err <- ErrHistoryCompacted
			return
		}
		// 传输错误/流结束：换节点退避重连续传。
		select {
		case <-ctx.Done():
			w.err <- ctx.Err()
			return
		case <-w.done:
			return
		case <-time.After(backoff):
		}
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
}

func (c *Client) pumpWatch(stream pb.KVService_WatchClient, w *Watcher, cursor *int64) error {
	for {
		ev, err := stream.Recv()
		if err != nil {
			return err
		}
		select {
		case w.events <- ev:
			*cursor = ev.Revision
		case <-w.done:
			return nil
		}
	}
}

// isRetryableTransport 判断是否为可换节点重试的连接类错误。
func isRetryableTransport(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	if !ok {
		return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
	}
	return st.Code() == codes.Unavailable
}

// ----------------------------------------------------------------
// 服务注册 / 发现
// ----------------------------------------------------------------

// Register 向 leader 注册一个带租约的实例。
func (c *Client) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	for attempt := 0; attempt < 2; attempt++ {
		addr, err := c.ensureLeader(ctx)
		if err != nil {
			return nil, err
		}
		cl, err := c.registryClient(addr)
		if err != nil {
			return nil, err
		}
		resp, err := cl.Register(ctx, req)
		if err == nil {
			return resp, nil
		}
		if mapStatus(err) == ErrNotLeader || (isRetryableTransport(err) && attempt == 0) {
			c.mu.Lock()
			c.leader = ""
			c.mu.Unlock()
			if hint := parseLeaderHint(err); hint != "" {
				c.mu.Lock()
				c.leader = hint
				c.mu.Unlock()
			} else {
				_, _ = c.discoverLeader(ctx)
			}
			continue
		}
		return nil, mapStatus(err)
	}
	return nil, ErrNoQuorum
}

// Heartbeat 续租。
func (c *Client) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	for attempt := 0; attempt < 2; attempt++ {
		addr, err := c.ensureLeader(ctx)
		if err != nil {
			return nil, err
		}
		cl, err := c.registryClient(addr)
		if err != nil {
			return nil, err
		}
		resp, err := cl.Heartbeat(ctx, req)
		if err == nil {
			return resp, nil
		}
		if mapStatus(err) == ErrNotLeader || (isRetryableTransport(err) && attempt == 0) {
			c.mu.Lock()
			c.leader = ""
			c.mu.Unlock()
			if hint := parseLeaderHint(err); hint != "" {
				c.mu.Lock()
				c.leader = hint
				c.mu.Unlock()
			} else {
				_, _ = c.discoverLeader(ctx)
			}
			continue
		}
		return nil, mapStatus(err)
	}
	return nil, ErrNoQuorum
}

// Discover 从任意节点读取实例集合。
func (c *Client) Discover(ctx context.Context, req *pb.DiscoverRequest) (*pb.DiscoverResponse, error) {
	var lastErr error
	start := int(atomic.AddUint64(&c.rrIndex, 1) - 1)
	for i := 0; i < len(c.endpoints); i++ {
		addr := c.endpoints[(start+i)%len(c.endpoints)]
		cl, err := c.registryClient(addr)
		if err != nil {
			lastErr = err
			continue
		}
		resp, err := cl.Discover(ctx, req)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if !isRetryableTransport(err) {
			return nil, mapStatus(err)
		}
	}
	return nil, mapStatus(lastErr)
}

// DiscoverWatcher 持续观察实例集合变化。
type DiscoverWatcher struct {
	updates chan *pb.DiscoverResponse
	err     chan error
	done    chan struct{}
	once    sync.Once
}

// Updates 先收到全量快照，之后收到增量。
func (d *DiscoverWatcher) Updates() <-chan *pb.DiscoverResponse { return d.updates }
func (d *DiscoverWatcher) Err() <-chan error                    { return d.err }
func (d *DiscoverWatcher) Close() {
	d.once.Do(func() { close(d.done) })
}

// WatchInstances 订阅实例集合，断线自动重连（重连首条为全量快照）。
func (c *Client) WatchInstances(ctx context.Context, req *pb.DiscoverRequest) *DiscoverWatcher {
	d := &DiscoverWatcher{
		updates: make(chan *pb.DiscoverResponse, 32),
		err:     make(chan error, 1),
		done:    make(chan struct{}),
	}
	go func() {
		defer close(d.updates)
		backoff := 200 * time.Millisecond
		for {
			select {
			case <-ctx.Done():
				d.err <- ctx.Err()
				return
			case <-d.done:
				return
			default:
			}
			addr := c.pickEndpoint()
			cl, err := c.registryClient(addr)
			if err == nil {
				stream, serr := cl.WatchInstances(ctx, req)
				if serr == nil {
					for {
						resp, rerr := stream.Recv()
						if rerr != nil {
							err = rerr
							break
						}
						select {
						case d.updates <- resp:
						case <-d.done:
							return
						}
					}
				} else {
					err = serr
				}
			}
			select {
			case <-ctx.Done():
				d.err <- ctx.Err()
				return
			case <-d.done:
				return
			case <-time.After(backoff):
			}
			if backoff < 2*time.Second {
				backoff *= 2
			}
		}
	}()
	return d
}
