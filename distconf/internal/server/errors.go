// Package server 组装存储状态机、复制层、TTL sweeper 与 gRPC 服务，
// 表示集群中的一个节点（leader 或 follower）。
package server

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/example/distconf/internal/repl"
	"github.com/example/distconf/internal/store"
)

// proposeTimeout 是单条写等待多数派提交的时间。
const proposeTimeout = 5 * time.Second

func toGRPCError(err error) error {
	switch {
	case errors.Is(err, store.ErrCASConflict):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, store.ErrInstanceNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, store.ErrRevisionCompacted):
		return status.Error(codes.OutOfRange, err.Error())
	case errors.Is(err, repl.ErrNotLeader):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, repl.ErrNoQuorum), errors.Is(err, context.DeadlineExceeded):
		// 多数派不可达 / 等待提交超时：客户端可安全重试
		//（CAS 保证不会产生语义重复）。
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// withLeaderHint 在“非 leader / 不可用”错误消息中附带 leader 地址，
// 客户端 SDK 解析后自动重定向写请求。
func withLeaderHint(err error, addr string) error {
	if addr == "" {
		return toGRPCError(err)
	}
	st, ok := status.FromError(toGRPCError(err))
	if !ok {
		return toGRPCError(err)
	}
	return status.New(st.Code(), st.Message()+"; leader_hint="+addr).Err()
}
