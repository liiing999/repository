package client

import (
	"context"
	"sync/atomic"
	"time"

	pb "github.com/example/distconf/api/distconf/v1"
)

// Lease 管理一个服务实例的注册与自动心跳：
// 按 TTL 的 1/3 间隔续租；续租失败（实例过期/摘除/无多数派）时自动
// 重新 Register。租约 TTL 建议 >= 3s，过短的 TTL 应直接重新注册。
type Lease struct {
	client   *Client
	req      *pb.RegisterRequest
	ttl      time.Duration
	interval time.Duration

	registered atomic.Bool
	stopCh     chan struct{}
	doneCh     chan struct{}
}

// NewLease 构造租约（未启动）。
func NewLease(c *Client, req *pb.RegisterRequest) *Lease {
	ttl := time.Duration(req.LeaseTtlMs) * time.Millisecond
	if ttl <= 0 {
		ttl = 10 * time.Second
		req.LeaseTtlMs = ttl.Milliseconds()
	}
	interval := ttl / 3
	if interval < time.Second {
		interval = time.Second
	}
	return &Lease{
		client:   c,
		req:      req,
		ttl:      ttl,
		interval: interval,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

// KeepAlive 注册并启动后台心跳，阻塞直到租约结束（Stop 或 ctx 取消）。
// 初次注册失败会按间隔重试。
func (l *Lease) KeepAlive(ctx context.Context) error {
	defer close(l.doneCh)

	if err := l.registerWithRetry(ctx); err != nil {
		return err
	}

	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-l.stopCh:
			return nil
		case <-ticker.C:
			if err := l.heartbeatOnce(ctx); err != nil {
				// 实例已过期/被摘除或 leader 不可用：重新注册。
				_ = l.registerWithRetry(ctx)
			}
		}
	}
}

// Stop 结束租约（不主动注销）。
func (l *Lease) Stop() {
	close(l.stopCh)
	<-l.doneCh
}

func (l *Lease) heartbeatOnce(ctx context.Context) error {
	cctx, cancel := context.WithTimeout(ctx, l.interval+2*time.Second)
	defer cancel()
	_, err := l.client.Heartbeat(cctx, &pb.HeartbeatRequest{
		Namespace:  l.req.Namespace,
		Service:    l.req.Service,
		InstanceId: l.req.InstanceId,
	})
	return err
}

func (l *Lease) registerWithRetry(ctx context.Context) error {
	for {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := l.client.Register(cctx, l.req)
		cancel()
		if err == nil {
			l.registered.Store(true)
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-l.stopCh:
			return nil
		case <-time.After(l.interval):
		}
	}
}
