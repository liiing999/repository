package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	pb "github.com/example/distconf/api/distconf/v1"
	"github.com/example/distconf/client"
)

func defaultEndpoints() string {
	return env("DISTCONF_ENDPOINTS", "127.0.0.1:9000")
}

func endpointsFlag(fs *flag.FlagSet) *string {
	return fs.String("endpoints", defaultEndpoints(), "comma-separated node addresses")
}

func newClient(v string) (*client.Client, error) {
	var eps []string
	for _, e := range strings.Split(v, ",") {
		if e = strings.TrimSpace(e); e != "" {
			eps = append(eps, e)
		}
	}
	return client.New(eps)
}

func runClient(cmd string, args []string) error {
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	endpoints := endpointsFlag(fs)
	ns := fs.String("ns", "default", "namespace")
	key := fs.String("key", "", "key")
	service := fs.String("service", "", "service name")
	id := fs.String("id", "", "instance id")
	value := fs.String("value", "", "value (string bytes)")
	ttl := fs.Int64("ttl-ms", 0, "ttl in milliseconds")
	expect := fs.Int64("expect-version", 0, "CAS expected version (0=must-not-exist, -1=any)")
	fromSeq := fs.Int64("from-seq", 0, "watch cursor (event revision)")
	address := fs.String("address", "", "instance address")
	meta := fs.String("meta", "", "metadata k=v,k2=v2")
	watchFlag := fs.Bool("watch", false, "keep streaming discovery updates")
	timeout := fs.Duration("timeout", 10*time.Second, "request timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}

	c, err := newClient(*endpoints)
	if err != nil {
		return err
	}
	defer c.Close()

	// 长连接类命令自行管理 ctx，不受 --timeout 限制。
	switch cmd {
	case "watch":
		return cmdWatch(c, *ns, *key, *fromSeq)
	case "discover":
		if *watchFlag {
			return cmdDiscoverWatch(c, *ns, *service)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	switch cmd {
	case "info":
		return cmdInfo(c, ctx)
	case "put":
		return cmdPut(c, ctx, *ns, *key, *value, *ttl, *expect)
	case "get":
		return cmdGet(c, ctx, *ns, *key)
	case "delete":
		return cmdDelete(c, ctx, *ns, *key, *expect)
	case "register":
		return cmdRegister(c, ctx, *ns, *service, *id, *address, *ttl, *meta)
	case "heartbeat":
		return cmdHeartbeat(c, ctx, *ns, *service, *id)
	case "discover":
		return cmdDiscover(c, ctx, *ns, *service)
	default:
		return fmt.Errorf("unknown client command %q", cmd)
	}
}

func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func cmdInfo(c *client.Client, ctx context.Context) error {
	// Info 走任意节点：用 Discover 同款轮询，这里直接用内部无导出的方法，
	// 故通过一次轻量读间接不可行——在 client 上加 Info。
	info, err := c.Info(ctx)
	if err != nil {
		return err
	}
	printJSON(info)
	return nil
}

func cmdPut(c *client.Client, ctx context.Context, ns, key, value string, ttl, expect int64) error {
	if key == "" {
		return errors.New("--key required")
	}
	resp, err := c.Put(ctx, &pb.PutRequest{
		Namespace:       ns,
		Key:             key,
		Value:           []byte(value),
		TtlMs:           ttl,
		ExpectedVersion: expect,
	})
	if err != nil {
		return err
	}
	printJSON(resp.Kv)
	return nil
}

func cmdGet(c *client.Client, ctx context.Context, ns, key string) error {
	resp, err := c.Get(ctx, ns, key)
	if err != nil {
		return err
	}
	if !resp.Found {
		fmt.Println("(not found)")
		return nil
	}
	printJSON(resp.Kv)
	return nil
}

func cmdDelete(c *client.Client, ctx context.Context, ns, key string, expect int64) error {
	resp, err := c.Delete(ctx, &pb.DeleteRequest{
		Namespace:       ns,
		Key:             key,
		ExpectedVersion: expect,
	})
	if err != nil {
		return err
	}
	fmt.Printf("deleted=%v revision=%d\n", resp.Found, resp.Revision)
	return nil
}

func cmdWatch(c *client.Client, ns, key string, fromSeq int64) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := c.Watch(ctx, ns, key, fromSeq)
	for {
		select {
		case ev, ok := <-w.Events():
			if !ok {
				select {
				case err := <-w.Err():
					return err
				default:
					return nil
				}
			}
			printJSON(ev)
		case err := <-w.Err():
			return err
		}
	}
}

func parseMeta(spec string) map[string]string {
	m := map[string]string{}
	if spec == "" {
		return m
	}
	for _, kv := range strings.Split(spec, ",") {
		kv = strings.TrimSpace(kv)
		if kv == "" {
			continue
		}
		if i := strings.Index(kv, "="); i > 0 {
			m[kv[:i]] = kv[i+1:]
		}
	}
	return m
}

func cmdRegister(c *client.Client, ctx context.Context, ns, svc, id, addr string, ttl int64, meta string) error {
	if svc == "" || id == "" || addr == "" {
		return errors.New("--service, --id, --address required")
	}
	resp, err := c.Register(ctx, &pb.RegisterRequest{
		Namespace:  ns,
		Service:    svc,
		InstanceId: id,
		Address:    addr,
		Metadata:   parseMeta(meta),
		LeaseTtlMs: ttl,
	})
	if err != nil {
		return err
	}
	printJSON(resp.Instance)
	return nil
}

func cmdHeartbeat(c *client.Client, ctx context.Context, ns, svc, id string) error {
	resp, err := c.Heartbeat(ctx, &pb.HeartbeatRequest{
		Namespace: ns, Service: svc, InstanceId: id,
	})
	if err != nil {
		return err
	}
	printJSON(resp.Instance)
	return nil
}

func cmdDiscover(c *client.Client, ctx context.Context, ns, svc string) error {
	if svc == "" {
		return errors.New("--service required")
	}
	resp, err := c.Discover(ctx, &pb.DiscoverRequest{Namespace: ns, Service: svc})
	if err != nil {
		return err
	}
	printJSON(resp.Instances)
	return nil
}

func cmdDiscoverWatch(c *client.Client, ns, svc string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := c.WatchInstances(ctx, &pb.DiscoverRequest{Namespace: ns, Service: svc})
	for resp := range w.Updates() {
		printJSON(resp)
	}
	select {
	case err := <-w.Err():
		return err
	default:
		return nil
	}
}
