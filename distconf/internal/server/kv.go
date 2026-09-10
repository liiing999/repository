package server

import (
	"context"
	"time"

	pb "github.com/example/distconf/api/distconf/v1"
)

// Get 在任意节点本地读取。返回的是该节点“某个已提交前缀”上的值，
// 属于顺序一致读（不会回退，但可能比 leader 稍旧）。
func (n *Node) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	kv, found := n.st.Get(req.Namespace, req.Key)
	resp := &pb.GetResponse{
		Found:           found,
		AppliedRevision: n.st.AppliedRevision(),
		LeaderId:        n.cfg.LeaderID,
	}
	if found {
		resp.Kv = kv
	}
	return resp, nil
}

// Put 仅在 leader 处理；CAS 判定在状态机 apply 阶段进行，
// 因此同一 key 并发 Put 只有与日志顺序一致的那一个会成功。
func (n *Node) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if !n.isLeader {
		return nil, withLeaderHint(errNotLeader(), n.cfg.Peers[n.cfg.LeaderID])
	}

	expected := req.ExpectedVersion
	// proto 默认值 0 在 proto3 中无法与“显式传 0”区分：
	// 0 恰好就是“要求 key 不存在”，语义一致，无需特殊处理。
	var ttl, expireAt int64
	if req.TtlMs > 0 {
		ttl = req.TtlMs
		expireAt = time.Now().UnixMilli() + req.TtlMs
	}

	cctx, cancel := context.WithTimeout(ctx, proposeTimeout)
	defer cancel()
	res, err := n.leader.Propose(cctx, &pb.LogEntry{Op: &pb.LogEntry_Put{Put: &pb.PutOp{
		Namespace:       req.Namespace,
		Key:             req.Key,
		Value:           req.Value,
		TtlMs:           ttl,
		ExpireAtMs:      expireAt,
		ExpectedVersion: expected,
	}}})
	if err != nil {
		return nil, n.mapProposeError(err)
	}
	if res.Err != nil {
		return nil, toGRPCError(res.Err)
	}
	return &pb.PutResponse{Kv: res.KV, LeaderId: n.cfg.LeaderID}, nil
}

// Delete 删除 key；ExpectedVersion 为可选 CAS（-1 无条件；0 要求不存在）。
func (n *Node) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if !n.isLeader {
		return nil, withLeaderHint(errNotLeader(), n.cfg.Peers[n.cfg.LeaderID])
	}
	expected := req.ExpectedVersion
	if expected == 0 {
		expected = -1 // Delete 的默认语义为无条件删除
	}

	cctx, cancel := context.WithTimeout(ctx, proposeTimeout)
	defer cancel()
	res, err := n.leader.Propose(cctx, &pb.LogEntry{Op: &pb.LogEntry_Delete{Delete: &pb.DeleteOp{
		Namespace:       req.Namespace,
		Key:             req.Key,
		ExpectedVersion: expected,
	}}})
	if err != nil {
		return nil, n.mapProposeError(err)
	}
	if res.Err != nil {
		return nil, toGRPCError(res.Err)
	}
	return &pb.DeleteResponse{
		Found:    res.Found,
		Revision: n.st.AppliedRevision(),
		LeaderId: n.cfg.LeaderID,
	}, nil
}

// Watch 按 namespace/key 推送有序变更：
//  1. 先原子补发 (from_revision, 当前] 的历史（含 TTL 过期事件）；
//  2. 再推实时事件；
//  3. 若订阅者因消费过慢被摘除（通道关闭），自动用“最后成功投递的
//     revision”重新订阅，由历史补发消除缝隙。
//
// 同一条日志（尤其批量过期）可能产生多条 revision 相同的事件；
// 重连补发按 revision 严格大于过滤，因此补发起点是“已投递 revision”，
// 同 revision 中未投递的尾部事件需要服务端额外保留 sub-index ——
// 本实现中批量过期在同一 revision 下追加的多条事件连续存放于历史环，
// 见 watchLoop 中以 (revision, 环内位置游标) 二元组实现的去重续传。
func (n *Node) Watch(req *pb.WatchRequest, stream pb.KVService_WatchServer) error {
	return n.watchLoop(stream.Context(), req, func(ev *pb.WatchEvent) error {
		return stream.Send(ev)
	})
}
