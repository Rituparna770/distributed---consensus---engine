#!/usr/bin/env bash
#
# Chaos test harness.
#
# Sets up Toxiproxy proxies in front of every node, then runs a sequence
# of escalating fault scenarios while the client (already running in
# docker-compose) submits transactions continuously.
#
# After each scenario we check ledger consistency across all live nodes.

set -uo pipefail

TOXI=${TOXIPROXY_URL:-http://localhost:8474}
NODES=(node1 node2 node3 node4 node5)
HOST_PORTS=(8001 8002 8003 8004 8005)

c_blue()  { printf "\033[1;34m%s\033[0m\n" "$*"; }
c_green() { printf "\033[1;32m%s\033[0m\n" "$*"; }
c_red()   { printf "\033[1;31m%s\033[0m\n" "$*"; }

wait_for_toxiproxy() {
  c_blue "Waiting for Toxiproxy admin API..."
  for _ in $(seq 1 30); do
    curl -fsS "$TOXI/version" >/dev/null 2>&1 && return 0
    sleep 1
  done
  c_red "Toxiproxy unreachable at $TOXI"; exit 1
}

create_proxies() {
  c_blue "Creating Toxiproxy proxies..."
  # We can't easily intercept inter-container traffic without rewiring
  # the cluster to route through Toxiproxy. For this academic demo we
  # use Toxiproxy as a *fault-injection control plane* and combine it
  # with docker network/stop commands for actual partitioning.
  for n in "${NODES[@]}"; do
    curl -fsS -X POST "$TOXI/proxies" \
      -H "Content-Type: application/json" \
      -d "{\"name\":\"${n}_proxy\",\"listen\":\"0.0.0.0:1${n: -1}000\",\"upstream\":\"${n}:8000\"}" \
      >/dev/null 2>&1 || true
  done
}

inject_latency() {
  local proxy=$1 latency_ms=$2
  c_blue "Injecting ${latency_ms}ms latency on ${proxy}"
  curl -fsS -X POST "$TOXI/proxies/${proxy}/toxics" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"lat\",\"type\":\"latency\",\"attributes\":{\"latency\":${latency_ms}}}" \
    >/dev/null
}

clear_toxics() {
  local proxy=$1
  curl -fsS "$TOXI/proxies/${proxy}/toxics" \
    | python3 -c "import json,sys; [print(t['name']) for t in json.load(sys.stdin)]" \
    | while read -r toxic; do
        [ -n "$toxic" ] && curl -fsS -X DELETE "$TOXI/proxies/${proxy}/toxics/${toxic}" >/dev/null
      done
}

partition_node() {
  local node=$1
  c_red "PARTITION: disconnecting ${node}"
  docker network disconnect consensus-net "$node" 2>/dev/null || true
}

heal_node() {
  local node=$1
  c_green "HEAL: reconnecting ${node}"
  docker network connect consensus-net "$node" 2>/dev/null || true
}

crash_node() {
  local node=$1
  c_red "CRASH: stopping ${node}"
  docker stop "$node" >/dev/null
}

recover_node() {
  local node=$1
  c_green "RECOVER: starting ${node}"
  docker start "$node" >/dev/null
}

check_ledgers() {
  c_blue "--- Ledger snapshot ---"
  for i in "${!NODES[@]}"; do
    local n=${NODES[$i]} p=${HOST_PORTS[$i]}
    local len
    len=$(curl -fsS --max-time 3 "http://localhost:${p}/ledger" \
           | python3 -c "import json,sys; print(json.load(sys.stdin)['length'])" 2>/dev/null \
           || echo "DOWN")
    printf "  %-7s : %s entries\n" "$n" "$len"
  done
}

scenario_baseline() {
  c_blue "==== SCENARIO 1: baseline (no faults) ===="
  sleep 10
  check_ledgers
}

scenario_latency() {
  c_blue "==== SCENARIO 2: high latency on node2 ===="
  inject_latency node2_proxy 800
  sleep 15
  clear_toxics node2_proxy
  check_ledgers
}

scenario_single_crash() {
  c_blue "==== SCENARIO 3: single node crash ===="
  crash_node node3
  sleep 15
  check_ledgers
  recover_node node3
  sleep 10
  check_ledgers
}

scenario_double_crash() {
  c_blue "==== SCENARIO 4: TWO simultaneous crashes (Paxos f=2 limit) ===="
  crash_node node3
  crash_node node4
  sleep 20
  check_ledgers
  recover_node node3
  recover_node node4
  sleep 10
  check_ledgers
}

scenario_leader_kill() {
  c_blue "==== SCENARIO 5: kill the current leader ===="
  local leader
  leader=$(curl -fsS [localhost](http://localhost:8001/status) \
            | python3 -c "import json,sys; print(json.load(sys.stdin).get('leader') or 'node1')")
  c_red "Current leader is ${leader} — crashing it"
  crash_node "$leader"
  sleep 15
  check_ledgers
  recover_node "$leader"
  sleep 10
  check_ledgers
}

scenario_partition() {
  c_blue "==== SCENARIO 6: minority partition ===="
  partition_node node4
  partition_node node5
  sleep 20
  check_ledgers
  heal_node node4
  heal_node node5
  sleep 10
  check_ledgers
}

main() {
  wait_for_toxiproxy
  create_proxies

  scenario_baseline
  scenario_latency
  scenario_single_crash
  scenario_double_crash
  scenario_leader_kill
  scenario_partition

  c_green "==== Chaos test complete ===="
}

main "$@"
