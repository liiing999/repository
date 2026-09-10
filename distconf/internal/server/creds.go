package server

import (
	"context"
)

// replCreds 是 follower 连 leader 复制流时使用的 per-RPC 凭据。
type replCreds struct{ token string }

func (c replCreds) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	if c.token == "" {
		return nil, nil
	}
	return map[string]string{replTokenKey: c.token}, nil
}

func (c replCreds) RequireTransportSecurity() bool { return false }
