package server_test

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func dialRaw(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}
