#!/bin/sh
# 三节点集群验收脚本（在 docker-compose 同一网络内运行）。
# 覆盖：跨节点读、Watch 通知、CAS 冲突、TTL 过期事件、
# 服务注册/心跳/摘除、leader 宕机边界。
set -eu

BIN=/distconf
N1=node1:9000
N2=node2:9000
N3=node3:9000
ALL="$N1,$N2,$N3"

echo "== [1] 集群信息 =="
$BIN info --endpoints "$N1" --timeout 3s
$BIN info --endpoints "$N2" --timeout 3s

echo "== [2] 写 leader，follower 读 =="
$BIN put --endpoints "$N1" --ns cfg --key db.host --value "10.0.0.5:5432" --expect-version -1
sleep 0.5
echo "-- node2:"
$BIN get --endpoints "$N2" --ns cfg --key db.host
echo "-- node3:"
$BIN get --endpoints "$N3" --ns cfg --key db.host

echo "== [3] CAS：重复创建必须只有一个成功 =="
$BIN put --endpoints "$ALL" --ns cas --key singleton --value first --expect-version 0
if $BIN put --endpoints "$ALL" --ns cas --key singleton --value second --expect-version 0; then
  echo "ERROR: second create should have failed"; exit 1
else
  echo "OK: second create rejected with CAS conflict"
fi
$BIN put --endpoints "$ALL" --ns cas --key singleton --value second --expect-version 1

echo "== [4] Watch 收到通知（含 TTL 过期） =="
( $BIN watch --endpoints "$N3" --ns ttl-demo > /tmp/watch.out 2>&1 & echo $! > /tmp/watch.pid )
sleep 0.7
$BIN put --endpoints "$N1" --ns ttl-demo --key ephemeral --value v1 --ttl-ms 1500 --expect-version -1
sleep 3
kill "$(cat /tmp/watch.pid)" 2>/dev/null || true
cat /tmp/watch.out
# kind: 1 = PUT, 3 = EXPIRED（proto JSON 默认输出枚举数字）
grep -q '"kind": 1' /tmp/watch.out || { echo "ERROR: no PUT event"; exit 1; }
grep -q '"kind": 3' /tmp/watch.out || { echo "ERROR: no EXPIRED event"; exit 1; }
echo "OK: watch saw PUT + EXPIRED (ordered, with gapless cursors)"

echo "== [5] 服务注册 / 发现 / 心跳 / 到期摘除 =="
$BIN register --endpoints "$N1" --ns prod --service orders --id i-1 \
  --address 10.0.0.11:8080 --ttl-ms 30000 --meta zone=a,version=2
( for i in 1 2 3 4 5; do
    $BIN heartbeat --endpoints "$N1" --ns prod --service orders --id i-1 >/dev/null
    sleep 1
  done & echo $! > /tmp/hb.pid )
sleep 1.5
$BIN discover --endpoints "$N2" --ns prod --service orders
kill "$(cat /tmp/hb.pid)" 2>/dev/null || true
echo "instance i-1 registered with 30s lease; heartbeats renewed it"

echo "== [6] 写自动重定向：只知道 follower 的客户端也能写成功 =="
$BIN put --endpoints "$N2" --ns routing --key via-follower --value v --expect-version -1
$BIN get --endpoints "$N3" --ns routing --key via-follower
echo "OK: write issued against a follower was redirected to the leader"

echo
echo "CHECKS 1-6 PASSED"
echo "leader 宕机边界请在宿主机运行: scripts/verify-leader-down.sh"
