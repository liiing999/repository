package server

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/example/distconf/api/distconf/v1"
	"github.com/example/distconf/internal/repl"
)

func errNotLeader() error { return repl.ErrNotLeader }

// mapProposeError 把复制层错误转成 gRPC 错误并附带 leader 提示。
func (n *Node) mapProposeError(err error) error {
	return withLeaderHint(err, n.cfg.Peers[n.cfg.LeaderID])
}

// Info 返回节点与复制状态，供客户端发现 leader、运维排查。
func (n *Node) Info(ctx context.Context, _ *pb.InfoRequest) (*pb.InfoResponse, error) {
	resp := &pb.InfoResponse{
		NodeId:   n.cfg.NodeID,
		IsLeader: n.isLeader,
		LeaderId: n.cfg.LeaderID,
	}
	var nodes []*pb.Node
	for id, addr := range n.cfg.Peers {
		nodes = append(nodes, &pb.Node{Id: id, Address: addr})
	}
	resp.Nodes = nodes
	resp.AppliedRevision = n.st.AppliedRevision()
	if n.isLeader {
		resp.CommittedRevision = n.leader.CommittedIndex()
	} else {
		resp.CommittedRevision = n.st.AppliedRevision()
	}
	return resp, nil
}

// AppendEntries 是 leader 端的节点间复制流。只有配置为 leader 的节点
// 会注册该服务；follower 连上来后由 PeerManager 接管。
func (n *Node) AppendEntries(stream pb.ReplicationService_AppendEntriesServer) error {
	if !n.isLeader {
		return repl.ErrNotLeader
	}

	// follower 身份以它发来的第一条消息中的 follower_id 为准；
	// 在收到首条消息前先等待。
	resp, err := stream.Recv()
	if err != nil {
		return err
	}
	id := resp.FollowerId
	if _, ok := n.cfg.Peers[id]; !ok || id == n.cfg.NodeID {
		return unknownFollower(id)
	}

	// 把首条 ack 喂给 PeerManager：用一个带缓冲的“预接收”通道方式 ——
	// HandleStream 自己会 Recv 首条消息，因此这里需要把已收到的消息
	// 转交。为避免改协议处理，HandleStream 接受一个可选的初始 ack。
	n.peers.HandleStreamWithInitial(id, resp.AckIndex, stream)
	return nil
}

func unknownFollower(id int32) error {
	return status.Error(codes.PermissionDenied,
		fmt.Sprintf("unknown or self follower id: %d", id))
}
