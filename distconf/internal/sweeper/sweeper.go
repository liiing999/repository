// Package sweeper 只在 leader 上运行：周期性扫描逻辑上已到期的
// KV 与服务实例，把删除作为普通日志条目走多数派复制。
//
// 为什么过期也写日志而不是本地直接删：只有 leader 删会导致副本状态
// 发散；过期删除与普通 Put 走同一条有序日志后，所有节点在同一
// revision 上摘除同一批对象，并向各自的 Watch 订阅者发出 EXPIRED 事件。
//
// 扫描与提交之间存在窗口，guard（扫描时的 revision）消除竞争：
// 若对象在窗口内被重新 Put/心跳（revision 变化），到期日志不会误删它。
package sweeper

import (
	"context"
	"log/slog"
	"sync"
	"time"

	pb "github.com/example/distconf/api/distconf/v1"
	"github.com/example/distconf/internal/repl"
	"github.com/example/distconf/internal/store"
)

// Proposer 抽象 leader.Propose，便于测试。
type Proposer interface {
	Propose(ctx context.Context, e *pb.LogEntry) (repl.Result, error)
}

// Sweeper 周期性把到期对象通过复制日志摘除。
type Sweeper struct {
	st             *store.Store
	leader         Proposer
	interval       time.Duration
	proposeTimeout time.Duration
	log            *slog.Logger

	stopCh   chan struct{}
	doneCh   chan struct{}
	stopOnce sync.Once
}

// New 创建 sweeper（未启动）。
func New(st *store.Store, leader Proposer, interval time.Duration, logger *slog.Logger) *Sweeper {
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	return &Sweeper{
		st:             st,
		leader:         leader,
		interval:       interval,
		proposeTimeout: 5 * time.Second,
		log:            logger,
		stopCh:         make(chan struct{}),
		doneCh:         make(chan struct{}),
	}
}

// Start 启动后台扫描循环。
func (s *Sweeper) Start() { go s.loop() }

// Stop 等待扫描循环退出，可重复调用。
func (s *Sweeper) Stop() {
	s.stopOnce.Do(func() { close(s.stopCh) })
	<-s.doneCh
}

func (s *Sweeper) loop() {
	defer close(s.doneCh)
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-t.C:
			s.tick()
		}
	}
}

// tick 执行一轮扫描与摘除（导出给测试直接驱动）。
func (s *Sweeper) tick() {
	s.expireKVs()
	s.expireServices()
}

// TickOnce 手动执行一轮（测试用）。
func (s *Sweeper) TickOnce() { s.tick() }

func (s *Sweeper) expireKVs() {
	items := s.st.ScanExpiredKVs()
	if len(items) == 0 {
		return
	}
	op := &pb.ExpireKeysOp{}
	for _, it := range items {
		op.Items = append(op.Items, &pb.ExpireKey{
			Namespace:     it.Namespace,
			Key:           it.Key,
			GuardRevision: it.GuardRevision,
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.proposeTimeout)
	defer cancel()
	if _, err := s.leader.Propose(ctx, &pb.LogEntry{Op: &pb.LogEntry_ExpireKeys{ExpireKeys: op}}); err != nil {
		// 无多数派/超时：对象已被读路径视为过期，下一轮重试即可。
		s.log.Debug("expire-keys propose failed, will retry", "count", len(items), "err", err)
	}
}

func (s *Sweeper) expireServices() {
	items := s.st.ScanExpiredInstances()
	if len(items) == 0 {
		return
	}
	op := &pb.ExpireServicesOp{}
	for _, it := range items {
		op.Items = append(op.Items, &pb.ExpireService{
			Namespace:     it.Namespace,
			Service:       it.Service,
			InstanceId:    it.InstanceID,
			GuardRevision: it.GuardRevision,
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.proposeTimeout)
	defer cancel()
	if _, err := s.leader.Propose(ctx, &pb.LogEntry{Op: &pb.LogEntry_ExpireServices{ExpireServices: op}}); err != nil {
		s.log.Debug("expire-services propose failed, will retry", "count", len(items), "err", err)
	}
}
