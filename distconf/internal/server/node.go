package server

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/example/distconf/api/distconf/v1"
	"github.com/example/distconf/internal/repl"
	"github.com/example/distconf/internal/store"
	"github.com/example/distconf/internal/sweeper"
)

// NodeConfig 是单节点配置。
type NodeConfig struct {
	NodeID        int32
	ListenAddr    string // 本节点 gRPC 监听地址 host:port（通常用 :9000）
	AdvertiseAddr string // 对外地址（docker 内服务名:端口）
	LeaderID      int32
	Peers         map[int32]string // 全部节点（含自己）id -> advertise 地址
	SweepInterval time.Duration
	Token         string // 复制流可选鉴权令牌（来自环境变量 DISTCONF_REPL_TOKEN）

	// Listener 非空时直接使用它（测试用 :0 预分配端口）。
	Listener net.Listener
}

// Node 是一个运行中的集群节点。
type Node struct {
	pb.UnimplementedKVServiceServer
	pb.UnimplementedRegistryServiceServer
	pb.UnimplementedClusterServiceServer
	pb.UnimplementedReplicationServiceServer

	cfg NodeConfig
	log *slog.Logger

	st *store.Store

	isLeader bool
	leader   *repl.Leader
	peers    *repl.PeerManager
	sweeper  *sweeper.Sweeper
	follower *repl.FollowerRunner

	grpcServer *grpc.Server
	listener   net.Listener

	dialMu    sync.Mutex
	dialConns map[int32]*grpc.ClientConn // 节点间懒连接（保留以便扩展）

	cancel   context.CancelFunc
	wg       sync.WaitGroup
	stopOnce sync.Once
}

// NewNode 组装节点（不启动）。
func NewNode(cfg NodeConfig, logger *slog.Logger) *Node {
	st := store.New()
	ap := repl.StoreApplier{St: st}

	n := &Node{
		cfg:       cfg,
		log:       logger,
		st:        st,
		isLeader:  cfg.NodeID == cfg.LeaderID,
		dialConns: make(map[int32]*grpc.ClientConn),
	}

	if n.isLeader {
		n.leader = repl.NewLeader(repl.Config{
			NodeID:    cfg.NodeID,
			LeaderID:  cfg.LeaderID,
			NodeAddrs: cfg.Peers,
		}, ap)
		n.leader.SetSnapshotExporter(func() *pb.Snapshot {
			return store.SnapshotToProto(st.ExportSnapshot())
		})
		n.peers = repl.NewPeerManager(n.leader)
		n.sweeper = sweeper.New(st, n.leader, cfg.SweepInterval, logger)
	} else {
		n.follower = repl.NewFollowerRunner(cfg.NodeID, cfg.Peers[cfg.LeaderID], ap,
			n.internalDialOpts(), logger)
	}
	return n
}

func (n *Node) internalDialOpts() []grpc.DialOption {
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if n.cfg.Token != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(replCreds{token: n.cfg.Token}))
	}
	return opts
}

// Start 监听端口、启动 gRPC 与复制循环。
func (n *Node) Start(ctx context.Context) error {
	ctx, n.cancel = context.WithCancel(ctx)

	lis := n.cfg.Listener
	if lis == nil {
		l, err := net.Listen("tcp", n.cfg.ListenAddr)
		if err != nil {
			return err
		}
		lis = l
	}
	n.listener = lis

	var opts []grpc.ServerOption
	if n.cfg.Token != "" {
		opts = append(opts,
			grpc.UnaryInterceptor(replTokenInterceptor(n.cfg.Token)),
			grpc.StreamInterceptor(streamReplTokenInterceptor(n.cfg.Token)),
		)
	}
	n.grpcServer = grpc.NewServer(opts...)
	pb.RegisterKVServiceServer(n.grpcServer, n)
	pb.RegisterRegistryServiceServer(n.grpcServer, n)
	pb.RegisterClusterServiceServer(n.grpcServer, n)
	if n.isLeader {
		pb.RegisterReplicationServiceServer(n.grpcServer, n)
		n.leader.Start()
		n.sweeper.Start()
	} else {
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			n.follower.Run(ctx)
		}()
	}

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		if err := n.grpcServer.Serve(lis); err != nil {
			n.log.Error("grpc serve stopped", "err", err)
		}
	}()
	n.log.Info("node started", "id", n.cfg.NodeID, "leader", n.isLeader,
		"addr", n.cfg.ListenAddr)
	return nil
}

// Stop 优雅停止节点，可重复调用。
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		if n.cancel != nil {
			n.cancel()
		}
		if n.isLeader {
			if n.sweeper != nil {
				n.sweeper.Stop()
			}
			if n.leader != nil {
				n.leader.Stop()
			}
		}
		if n.grpcServer != nil {
			n.grpcServer.GracefulStop()
		}
		n.dialMu.Lock()
		for _, c := range n.dialConns {
			_ = c.Close()
		}
		n.dialConns = map[int32]*grpc.ClientConn{}
		n.dialMu.Unlock()
		n.wg.Wait()
	})
}

// Store 暴露状态机（测试/快照用）。
func (n *Node) Store() *store.Store { return n.st }

// Addr 返回实际监听地址（端口由 :0 分配时用于测试）。
func (n *Node) Addr() string {
	if n.listener == nil {
		return ""
	}
	return n.listener.Addr().String()
}

// IsLeader 返回节点角色。
func (n *Node) IsLeader() bool { return n.isLeader }

// Leader 暴露 leader 复制器（仅 leader 节点非 nil）。
func (n *Node) Leader() *repl.Leader { return n.leader }
