// distconf 是单一二进制：默认 `serve` 启动集群节点；
// 也内置客户端子命令 put/get/delete/watch/register/heartbeat/discover/info，
// 便于在不安装额外工具的情况下验证。
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/example/distconf/internal/server"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	args := os.Args[1:]
	cmd := "serve"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd = args[0]
		args = args[1:]
	}

	var err error
	switch cmd {
	case "serve":
		err = runServe(args, logger)
	case "put", "get", "delete", "watch", "register", "heartbeat", "discover", "info":
		err = runClient(cmd, args)
	case "-h", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `distconf — minimal distributed config center + service registry

Usage:
  distconf serve [flags]                  Start a cluster node (default)
  distconf info   --endpoints host:port...
  distconf put    --ns N --key K --value V [--ttl-ms N] [--expect-version V]
  distconf get    --ns N --key K
  distconf delete --ns N --key K [--expect-version V]
  distconf watch  --ns N [--key K] [--from-seq N]
  distconf register  --ns N --service S --id I --address A [--ttl-ms N] [--meta k=v,...]
  distconf heartbeat --ns N --service S --id I
  distconf discover  --ns N --service S [--watch]

Node configuration (flags or environment variables; credentials are env-only):
  --node-id N        DISTCONF_NODE_ID       this node's id (1..N)
  --listen ADDR      DISTCONF_LISTEN        listen address, default :9000
  --advertise ADDR   DISTCONF_ADVERTISE     address peers/clients dial
  --leader-id N      DISTCONF_LEADER_ID     static leader node id
  --peers SPEC       DISTCONF_PEERS         "1=host1:9000,2=host2:9000,3=host3:9000"
  --sweep-interval D DISTCONF_SWEEP_INTERVAL TTL scan interval (default 200ms)
                     DISTCONF_REPL_TOKEN    optional token for replication streams
`)
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func runServe(args []string, logger *slog.Logger) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	nodeID := fs.Int("node-id", atoiOr(env("DISTCONF_NODE_ID", "0"), 0), "node id")
	listen := fs.String("listen", env("DISTCONF_LISTEN", ":9000"), "listen address")
	advertise := fs.String("advertise", env("DISTCONF_ADVERTISE", ""), "advertise address")
	leaderID := fs.Int("leader-id", atoiOr(env("DISTCONF_LEADER_ID", "1"), 0), "static leader id")
	peersSpec := fs.String("peers", env("DISTCONF_PEERS", ""), "peers spec")
	sweep := fs.Duration("sweep-interval", durOr(env("DISTCONF_SWEEP_INTERVAL", "200ms"), 200*time.Millisecond), "sweep interval")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *nodeID <= 0 {
		return fmt.Errorf("--node-id / DISTCONF_NODE_ID is required")
	}
	if *advertise == "" {
		// peers 映射才是节点间寻址的权威来源；advertise 仅在单节点
		// 开发模式（无 peers）下使用，默认回环地址。
		port := *listen
		if i := strings.LastIndex(*listen, ":"); i >= 0 {
			port = (*listen)[i+1:]
		}
		*advertise = "127.0.0.1:" + port
	}
	peers, err := parsePeers(*peersSpec)
	if err != nil {
		return err
	}
	if len(peers) == 0 {
		// 单节点开发模式：只含自己。
		peers = map[int32]string{int32(*nodeID): *advertise}
	}

	cfg := server.NodeConfig{
		NodeID:        int32(*nodeID),
		ListenAddr:    *listen,
		AdvertiseAddr: *advertise,
		LeaderID:      int32(*leaderID),
		Peers:         peers,
		SweepInterval: *sweep,
		Token:         os.Getenv("DISTCONF_REPL_TOKEN"), // 凭据只来自环境变量
	}

	rootCtx, cancel := signalContext()
	defer cancel()

	node := server.NewNode(cfg, logger)
	if err := node.Start(rootCtx); err != nil {
		return err
	}
	logger.Info("node serving", "node", cfg.NodeID, "leader", cfg.NodeID == cfg.LeaderID,
		"peers", len(cfg.Peers))

	<-rootCtx.Done()
	logger.Info("shutting down")
	node.Stop()
	return nil
}

func signalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-ch
		cancel()
	}()
	return ctx, cancel
}

func parsePeers(spec string) (map[int32]string, error) {
	out := map[int32]string{}
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return out, nil
	}
	for _, item := range strings.Split(spec, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		kv := strings.SplitN(item, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("bad peer spec %q, want id=host:port", item)
		}
		id, err := strconv.Atoi(strings.TrimSpace(kv[0]))
		if err != nil {
			return nil, fmt.Errorf("bad peer id %q: %w", kv[0], err)
		}
		out[int32(id)] = strings.TrimSpace(kv[1])
	}
	return out, nil
}

func atoiOr(s string, def int) int {
	if v, err := strconv.Atoi(s); err == nil {
		return v
	}
	return def
}

func durOr(s string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(s); err == nil {
		return d
	}
	return def
}
