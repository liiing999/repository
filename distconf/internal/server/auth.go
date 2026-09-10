package server

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const replTokenKey = "x-distconf-repl-token"
const replMethod = "/distconf.v1.ReplicationService/AppendEntries"

// replTokenInterceptor 仅保护节点间复制流；未配置令牌时不校验。
func replTokenInterceptor(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if token != "" && info.FullMethod == replMethod {
			if err := checkToken(ctx, token); err != nil {
				return nil, err
			}
		}
		return handler(ctx, req)
	}
}

// streamInterceptor 版本。
func streamReplTokenInterceptor(token string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if token != "" && info.FullMethod == replMethod {
			if err := checkToken(ss.Context(), token); err != nil {
				return err
			}
		}
		return handler(srv, ss)
	}
}

func checkToken(ctx context.Context, want string) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing replication token")
	}
	vals := md.Get(replTokenKey)
	if len(vals) == 0 || vals[0] != want {
		return status.Error(codes.Unauthenticated, "invalid replication token")
	}
	return nil
}
