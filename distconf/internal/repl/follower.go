package repl

import (
	"context"
	"io"
	"log/slog"
	"time"

	pb "github.com/example/distconf/api/distconf/v1"
	"google.golang.org/grpc"
)

// FollowerRunner 是 follower 侧复制循环：与 leader 建立双向流，
// 断线/失败后按固定间隔重连，直到 ctx 取消。
type FollowerRunner struct {
	selfID       int32
	leaderAddr   string
	ap           Applier
	dialOpts     []grpc.DialOption
	retryBackoff time.Duration
	log          *slog.Logger
}

// NewFollowerRunner 创建 follower 复制循环（未启动）。
func NewFollowerRunner(selfID int32, leaderAddr string, ap Applier, dialOpts []grpc.DialOption, logger *slog.Logger) *FollowerRunner {
	return &FollowerRunner{
		selfID:       selfID,
		leaderAddr:   leaderAddr,
		ap:           ap,
		dialOpts:     dialOpts,
		retryBackoff: 500 * time.Millisecond,
		log:          logger,
	}
}

// Run 阻塞执行重连循环，直到 ctx 取消。
func (r *FollowerRunner) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := r.once(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			if err != io.EOF {
				r.log.Warn("replication stream ended, retrying",
					"leader", r.leaderAddr, "err", err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(r.retryBackoff):
		}
	}
}

func (r *FollowerRunner) once(ctx context.Context) error {
	conn, err := grpc.NewClient(r.leaderAddr, r.dialOpts...)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewReplicationServiceClient(conn)
	stream, err := client.AppendEntries(ctx)
	if err != nil {
		return err
	}
	r.log.Info("replication stream established", "leader", r.leaderAddr,
		"applied", r.ap.AppliedIndex())
	return ApplyStream(r.ap, stream, r.selfID)
}
