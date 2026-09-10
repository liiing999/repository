package server

import (
	"context"
	"time"

	pb "github.com/example/distconf/api/distconf/v1"
	"github.com/example/distconf/internal/store"
)

// Register 注册一个带租约的服务实例。租约 TTL 由 leader 盖绝对过期时间戳。
func (n *Node) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	if !n.isLeader {
		return nil, withLeaderHint(errNotLeader(), n.cfg.Peers[n.cfg.LeaderID])
	}
	ttl := req.LeaseTtlMs
	var expireAt int64
	if ttl > 0 {
		expireAt = time.Now().UnixMilli() + ttl
	}

	cctx, cancel := context.WithTimeout(ctx, proposeTimeout)
	defer cancel()
	res, err := n.leader.Propose(cctx, &pb.LogEntry{Op: &pb.LogEntry_Register{Register: &pb.RegisterOp{
		Namespace:  req.Namespace,
		Service:    req.Service,
		InstanceId: req.InstanceId,
		Address:    req.Address,
		Metadata:   req.Metadata,
		LeaseTtlMs: ttl,
		ExpireAtMs: expireAt,
	}}})
	if err != nil {
		return nil, n.mapProposeError(err)
	}
	return &pb.RegisterResponse{Instance: res.Inst, LeaderId: n.cfg.LeaderID}, nil
}

// Heartbeat 续租。实例已到期/被摘除返回 NotFound，客户端应重新 Register。
// 返回的实例包含新的绝对过期时间，客户端可据此安排下一次心跳。
func (n *Node) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if !n.isLeader {
		return nil, withLeaderHint(errNotLeader(), n.cfg.Peers[n.cfg.LeaderID])
	}

	// 先读当前实例：取得 TTL 与 revision 守卫。实例已逻辑过期则直接拒绝。
	cur, ok := n.st.GetInstance(req.Namespace, req.Service, req.InstanceId)
	if !ok {
		return nil, toGRPCError(store.ErrInstanceNotFound)
	}
	var expireAt int64
	if cur.LeaseTtlMs > 0 {
		expireAt = time.Now().UnixMilli() + cur.LeaseTtlMs
	}

	cctx, cancel := context.WithTimeout(ctx, proposeTimeout)
	defer cancel()
	res, err := n.leader.Propose(cctx, &pb.LogEntry{Op: &pb.LogEntry_Heartbeat{Heartbeat: &pb.HeartbeatOp{
		Namespace:     req.Namespace,
		Service:       req.Service,
		InstanceId:    req.InstanceId,
		GuardRevision: cur.Revision,
		NewExpireAtMs: expireAt,
	}}})
	if err != nil {
		return nil, n.mapProposeError(err)
	}
	if res.Err != nil {
		return nil, toGRPCError(res.Err)
	}
	return &pb.HeartbeatResponse{Instance: res.Inst, LeaderId: n.cfg.LeaderID}, nil
}

// Discover 一次性返回当前存活实例集合（可在任意节点读，顺序一致）。
func (n *Node) Discover(ctx context.Context, req *pb.DiscoverRequest) (*pb.DiscoverResponse, error) {
	instances := n.st.Discover(req.Namespace, req.Service)
	return &pb.DiscoverResponse{
		Instances:       instances,
		AppliedRevision: n.st.AppliedRevision(),
	}, nil
}

// WatchInstances 先推全量快照(incremental=false)，再推实例增删增量。
// 订阅者消费过慢被摘除（通道关闭）时，重新取快照+订阅完成重同步，
// 因此不会漏掉任何“最终集合变化”，重同步窗口内的瞬时增删可能被
// 合并进下一份快照（服务发现语义可接受）。
func (n *Node) WatchInstances(req *pb.DiscoverRequest, stream pb.RegistryService_WatchInstancesServer) error {
	for {
		id, snapshot, ch := n.st.WatchService(req.Namespace, req.Service)
		if err := stream.Send(&pb.DiscoverResponse{
			Instances:       snapshot,
			AppliedRevision: n.st.AppliedRevision(),
		}); err != nil {
			n.st.CloseServiceWatcher(id)
			return err
		}

		closed := false
		for !closed {
			select {
			case <-stream.Context().Done():
				n.st.CloseServiceWatcher(id)
				return stream.Context().Err()
			case change, ok := <-ch:
				if !ok {
					closed = true
					break
				}
				if err := stream.Send(&pb.DiscoverResponse{
					Incremental:     true,
					Added:           change.Added,
					Removed:         change.Removed,
					AppliedRevision: n.st.AppliedRevision(),
				}); err != nil {
					n.st.CloseServiceWatcher(id)
					return err
				}
			}
		}
		n.st.CloseServiceWatcher(id) // 幂等；通道已关闭也安全
	}
}
