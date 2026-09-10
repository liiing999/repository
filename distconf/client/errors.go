// Package client 是 distconf 的 Go 客户端 SDK：
//   - 写请求自动路由到 leader，遇到 leader 变更按错误中的提示重定向；
//   - 读请求可发任意节点；
//   - Watch 在断线/重连后用“最后收到的事件游标”续传，不丢事件；
//   - 服务实例租约提供自动心跳 KeepAlive，过期后自动重新注册。
package client

import (
	"errors"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// 与服务端一一对应的可判定错误。
var (
	ErrCASConflict      = errors.New("cas conflict: version mismatch")
	ErrInstanceGone     = errors.New("service instance not found (lease expired)")
	ErrHistoryCompacted = errors.New("watch history compacted: full resync required")
	ErrNoQuorum         = errors.New("no quorum available")
	ErrNotLeader        = errors.New("not the leader")
)

func mapStatus(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	switch st.Code() {
	case codes.Aborted:
		return ErrCASConflict
	case codes.NotFound:
		return ErrInstanceGone
	case codes.OutOfRange:
		return ErrHistoryCompacted
	case codes.Unavailable:
		if strings.Contains(st.Message(), "no quorum") {
			return ErrNoQuorum
		}
		return err
	case codes.FailedPrecondition:
		return ErrNotLeader
	default:
		return err
	}
}

// parseLeaderHint 从“not the leader; leader_hint=host:port”类消息中取地址。
func parseLeaderHint(err error) string {
	st, ok := status.FromError(err)
	if !ok {
		return ""
	}
	msg := st.Message()
	const key = "leader_hint="
	if i := strings.Index(msg, key); i >= 0 {
		rest := msg[i+len(key):]
		if j := strings.IndexAny(rest, ";, "); j >= 0 {
			rest = rest[:j]
		}
		return rest
	}
	return ""
}
