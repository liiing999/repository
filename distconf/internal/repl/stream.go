package repl

import (
	"errors"
	"io"
	"sync"

	pb "github.com/example/distconf/api/distconf/v1"
)

// ApplyStream 是 follower 侧：消费 leader 推来的 AppendEntriesRequest 流，
// 先收快照（可能没有），随后按 index 严格连续地应用日志，并逐条回报 ack。
//
// 流上的契约：
//  1. 第一条（若有）必须是 snapshot；
//  2. 之后全部是 entry，且 entry.Index 必须 == 本地 applied+1 连续；
//     任何不连续都视为协议错误（leader 应在重连时用快照修复）。
func ApplyStream(ap Applier, stream pb.ReplicationService_AppendEntriesClient, followerID int32) error {
	sendAck := func() error {
		return stream.Send(&pb.AppendEntriesResponse{
			FollowerId: followerID,
			AckIndex:   ap.AppliedIndex(),
			Success:    true,
		})
	}
	// 建流后先汇报当前位置，leader 据此决定补快照还是补日志。
	if err := sendAck(); err != nil {
		return err
	}

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch p := req.Payload.(type) {
		case *pb.AppendEntriesRequest_Snapshot:
			ap.ApplySnapshot(p.Snapshot)
		case *pb.AppendEntriesRequest_Entry:
			e := p.Entry
			want := ap.AppliedIndex() + 1
			if e.Index < want {
				continue // 已应用的重复条目
			}
			if e.Index != want {
				return &ProtocolError{Msg: "non-contiguous log entry", Got: e.Index, Want: want}
			}
			_, _, _, _ = applyOne(ap, e)
		default:
			return errors.New("unknown appendentries payload")
		}

		if err := sendAck(); err != nil {
			return err
		}
	}
}

// ProtocolError 表示复制流上的不变量被破坏。
type ProtocolError struct {
	Msg  string
	Got  int64
	Want int64
}

func (e *ProtocolError) Error() string {
	return "replication protocol error: " + e.Msg
}

// ----------------------------------------------------------------
// Leader 端：follower 流的生命周期与推送
// ----------------------------------------------------------------

// peerSender 封装一个 follower 流的线程安全发送。
type peerSender struct {
	id     int32
	stream pb.ReplicationService_AppendEntriesServer
	mu     sync.Mutex
}

func (p *peerSender) send(req *pb.AppendEntriesRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stream.Send(req)
}

// PeerManager 管理 leader 上所有 follower 推送协程。
type PeerManager struct {
	leader *Leader

	mu    sync.Mutex
	peers map[int32]*peerSender
}

// NewPeerManager 创建推送管理器。
func NewPeerManager(l *Leader) *PeerManager {
	return &PeerManager{leader: l, peers: make(map[int32]*peerSender)}
}

// HandleStream 处理一个 follower 的整条复制流（首条消息尚未读取）。
func (pm *PeerManager) HandleStream(id int32, stream pb.ReplicationService_AppendEntriesServer) {
	pm.HandleStreamWithInitial(id, -1, stream)
}

// HandleStreamWithInitial 处理复制流，返回时流结束：
//   - 接收协程：follower 首条 ack 决定 catchup 起点，其余 ack 推进 match；
//     initialAck>=0 表示首条 ack 已由调用方读出（0 是合法值）；
//   - 推送协程：先 catchup（快照/缺失日志），再持续 tail 新日志。
func (pm *PeerManager) HandleStreamWithInitial(id int32, initialAck int64, stream pb.ReplicationService_AppendEntriesServer) {
	ps := &peerSender{id: id, stream: stream}
	pm.mu.Lock()
	if old, ok := pm.peers[id]; ok {
		// 同一 follower 重连产生的旧流：标记替换（旧协程会因 Send 失败退出）。
		_ = old
	}
	pm.peers[id] = ps
	pm.mu.Unlock()
	defer func() {
		pm.mu.Lock()
		// 仅当 map 里仍是自己时才删除并清 match，避免旧流退出时
		// 把重连后新流的进度清零。
		if pm.peers[id] == ps {
			delete(pm.peers, id)
			pm.mu.Unlock()
			pm.leader.FollowerReset(id)
			return
		}
		pm.mu.Unlock()
	}()

	initialCh := make(chan int64, 1)
	recvDone := make(chan struct{})

	// 接收协程
	go func() {
		defer close(recvDone)
		waitFirst := true
		if initialAck >= 0 {
			initialCh <- initialAck
			waitFirst = false
		}
		for {
			resp, err := stream.Recv()
			if err != nil {
				return
			}
			if waitFirst {
				waitFirst = false
				select {
				case initialCh <- resp.AckIndex:
				default:
				}
				continue
			}
			pm.leader.FollowerAck(resp.FollowerId, resp.AckIndex)
		}
	}()

	// 推送协程
	pushDone := make(chan struct{})
	go func() {
		defer close(pushDone)

		var last int64
		select {
		case last = <-initialCh:
		case <-pm.leader.StopChan():
			return
		}

		if err := pm.sendCatchup(ps, last); err != nil {
			return
		}
		// 已发送到 leader 当前末尾（follower 会顺序应用并 ack；
		// quorum 判定只认真 ack，所以这里按“已发送位置”tail 是安全的）。
		last = pm.leader.LastIndex()

		subID, subCh := pm.leader.SubscribeEntries()
		defer pm.leader.UnsubscribeEntries(subID)
		for {
			select {
			case <-subCh:
			case <-pm.leader.StopChan():
				return
			}
			for {
				li := pm.leader.LastIndex()
				if last >= li {
					break
				}
				entries, ok := pm.leader.EntriesAfter(last)
				if !ok {
					// follower 落后到日志窗口之外：快照 + 窗口全量重放。
					if err := pm.sendCatchup(ps, last); err != nil {
						return
					}
					last = pm.leader.LastIndex()
					continue
				}
				for _, e := range entries {
					if err := ps.send(&pb.AppendEntriesRequest{
						Payload: &pb.AppendEntriesRequest_Entry{Entry: e},
					}); err != nil {
						return
					}
				}
				last = li
			}
		}
	}()

	select {
	case <-recvDone:
	case <-pushDone:
	case <-pm.leader.StopChan():
	}
}

// sendCatchup 给 follower 发送快照（若需要）与缺失日志。
func (pm *PeerManager) sendCatchup(ps *peerSender, followerLast int64) error {
	snap, entries := pm.leader.Catchup(followerLast)
	if snap != nil {
		if err := ps.send(&pb.AppendEntriesRequest{
			Payload: &pb.AppendEntriesRequest_Snapshot{Snapshot: snap},
		}); err != nil {
			return err
		}
	}
	for _, e := range entries {
		if err := ps.send(&pb.AppendEntriesRequest{
			Payload: &pb.AppendEntriesRequest_Entry{Entry: e},
		}); err != nil {
			return err
		}
	}
	return nil
}

// OnlineCount 返回当前在线 follower 流数量。
func (pm *PeerManager) OnlineCount() int {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return len(pm.peers)
}
