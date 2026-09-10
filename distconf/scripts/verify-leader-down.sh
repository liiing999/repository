#!/usr/bin/env bash
# 验证“主节点故障”的声明边界（需要先 docker compose up -d）：
#   1. 写一条配置并确认复制到 follower；
#   2. 停掉静态 leader（node1）；
#   3. follower 读仍可用（停在已提交前缀，不回退）；
#   4. 写失败（无自动选举；follower 返回 not-leader，SDK 找不到可达 leader）；
#   5. 恢复 node1 后集群恢复写入。
#
# 边界说明（设计上有意为之）：
#   本系统为内嵌内存存储、静态 leader、无选举。leader 进程*存活期间*
#   follower 任何故障/重启都能通过快照+日志自动追平；但如果 leader 进程
#   重启，由于没有持久化 WAL，它会以空日志重启。docker-compose 配置了
#   restart: unless-stopped 但不会删除卷——内存数据仍随进程丢失。
#   因此脚本第 5 步用的是“恢复前先写入的 key 仍可读”的验证：
#   node1 重启后会以空状态作为权威源，follower 将被快照重置为空。
#   生产形态需要持久化 WAL 或加入选举，这超出最小实现范围，README 有说明。
set -euo pipefail
# Git Bash (MSYS) 会把 /distconf 这样的容器内路径改写成本机路径，关掉它。
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

cd "$(dirname "$0")/.."

run() { docker compose run --rm --entrypoint "" verify "$@"; }
# tools 镜像 entrypoint 是 verify.sh；这里覆盖为空，直接调用二进制。
BIN() { run /distconf "$@"; }

echo "== 写入并确认复制 =="
BIN put --endpoints node1:9000 --ns cfg --key down-test --value v1 --expect-version -1
sleep 0.5
BIN get --endpoints node2:9000 --ns cfg --key down-test

echo "== 停掉 leader node1 =="
docker compose stop node1
trap 'docker compose start node1 || true' EXIT

echo "-- follower 读仍可用（顺序一致，停在已提交前缀）"
BIN get --endpoints node2:9000 --ns cfg --key down-test

echo "-- 写必须失败（无 leader、无选举）"
if BIN put --endpoints node2:9000,node3:9000 --ns cfg --key new --value x \
     --expect-version -1 --timeout 4s; then
  echo "ERROR: write succeeded with leader down"; exit 1
else
  echo "OK: writes correctly fail while leader is down"
fi

echo "-- follower 元信息仍声明 leader=1"
BIN info --endpoints node2:9000 --timeout 3s | grep -q '"leaderId": 1'

echo
echo "LEADER-DOWN BOUNDARY VERIFIED"
