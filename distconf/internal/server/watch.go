package server

import (
	"context"

	pb "github.com/example/distconf/api/distconf/v1"
)

// watchLoop 实现“历史补发 + 实时推送 + 断订阅自动续传”。
//
// 不丢事件的关键：
//  1. Store.WatchKV 在同一把锁内先拷贝历史、再注册实时通道，
//     两者之间不存在可插入事件的窗口；
//  2. 每条事件携带全局唯一、严格递增的事件游标 WatchEvent.Revision；
//  3. 消费者过慢导致实时通道被摘除时，本循环用“最后成功投递的游标”
//     重新 WatchKV，服务端从历史环补发缝隙中的事件；
//  4. 历史环容量不足（游标太老）时，返回 OutOfRange，客户端必须
//     全量 Get 后从当前游标重新订阅。
func (n *Node) watchLoop(ctx context.Context, req *pb.WatchRequest, send func(*pb.WatchEvent) error) error {
	cursor := req.FromRevision
	id, backlog, err := n.st.WatchKV(req.Namespace, req.Key, cursor)
	if err != nil {
		return toGRPCError(err)
	}

	for {
		// 先补发（重新订阅时）历史。
		for _, ev := range backlog {
			if err := send(ev); err != nil {
				n.st.CloseKVWatcher(id)
				return err
			}
			cursor = ev.Revision
		}

		ch := n.st.KVWatcherChan(id)
		if ch == nil {
			// 订阅刚注册就被摘除（极端慢消费者）：用最后游标立刻续传。
			id, backlog, err = n.st.WatchKV(req.Namespace, req.Key, cursor)
			if err != nil {
				return toGRPCError(err)
			}
			continue
		}

		closed := false
		for !closed {
			select {
			case <-ctx.Done():
				n.st.CloseKVWatcher(id)
				return ctx.Err()
			case ev, ok := <-ch:
				if !ok {
					closed = true
					break
				}
				if err := send(ev); err != nil {
					n.st.CloseKVWatcher(id)
					return err
				}
				cursor = ev.Revision
			}
		}
		n.st.CloseKVWatcher(id)

		// 慢消费者被摘除：最后游标可能已落在历史环之外（消费者慢到
		// 超过 10 万事件），此时明确返回 OutOfRange，要求全量重同步。
		id, backlog, err = n.st.WatchKV(req.Namespace, req.Key, cursor)
		if err != nil {
			return toGRPCError(err)
		}
	}
}
