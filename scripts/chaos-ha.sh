#!/usr/bin/env bash
set -euo pipefail

# Ayder HA ports (internal cluster ports from RF_HA_NODES)
HA_PORTS="7000,8000,9000,10000,11000"
CHAIN="AYDER_HA_CHAOS"

# Container names (fixed by compose)
declare -A CNAME=(
  [node1]="ayder-node1"
  [node2]="ayder-node2"
  [node3]="ayder-node3"
  [node4]="ayder-node4"
  [node5]="ayder-node5"
)

# Static IPs from compose
declare -A IP=(
  [node1]="10.77.0.11"
  [node2]="10.77.0.12"
  [node3]="10.77.0.13"
  [node4]="10.77.0.14"
  [node5]="10.77.0.15"
)

NODES=(node1 node2 node3 node4 node5)

usage() {
  cat <<'EOF'
Usage:
  chaos-ha.sh status
  chaos-ha.sh heal
  chaos-ha.sh leader-minority        # split node1,node2  vs  node3,node4,node5
  chaos-ha.sh isolate <node>         # isolate one node from the other 4 (HA ports only)
  chaos-ha.sh split <csvA> <csvB>    # generic split, e.g. "node1,node2" "node3,node4,node5"
  chaos-ha.sh delay <node> <ms> [jitter_ms] [loss_percent]
  chaos-ha.sh clear-netem <node>
  chaos-ha.sh clear-netem-all
  chaos-ha.sh metrics                # quick HTTP checks from host

Examples:
  ./scripts/chaos-ha.sh leader-minority
  ./scripts/chaos-ha.sh isolate node1
  ./scripts/chaos-ha.sh delay node4 250 50 5
  ./scripts/chaos-ha.sh heal
EOF
}

need_docker() {
  command -v docker >/dev/null 2>&1 || { echo "docker not found"; exit 1; }
}

exec_in() {
  local node="$1"; shift
  docker exec -u 0 "${CNAME[$node]}" bash -lc "$*"
}

check_tools_in_container() {
  local node="$1"
  exec_in "$node" 'command -v iptables >/dev/null && command -v tc >/dev/null' || {
    echo "ERROR: iptables/tc missing in ${CNAME[$node]}."
    echo "Patch Dockerfile runtime stage to install: iproute2 iptables"
    exit 1
  }
}

ensure_chain() {
  local node="$1"
  exec_in "$node" "
    iptables -w -N ${CHAIN} 2>/dev/null || true
    iptables -w -C INPUT  -j ${CHAIN} 2>/dev/null || iptables -w -I INPUT 1 -j ${CHAIN}
    iptables -w -C OUTPUT -j ${CHAIN} 2>/dev/null || iptables -w -I OUTPUT 1 -j ${CHAIN}
  "
}

clear_chain() {
  local node="$1"
  exec_in "$node" "
    iptables -w -D INPUT  -j ${CHAIN} 2>/dev/null || true
    iptables -w -D OUTPUT -j ${CHAIN} 2>/dev/null || true
    iptables -w -F ${CHAIN} 2>/dev/null || true
    iptables -w -X ${CHAIN} 2>/dev/null || true
  "
}

clear_all_chains() {
  for n in "${NODES[@]}"; do
    clear_chain "$n" || true
  done
}

block_peer_ha() {
  local src="$1"
  local dst="$2"
  local dst_ip="${IP[$dst]}"

  ensure_chain "$src"
  exec_in "$src" "
    # Drop outbound HA traffic to peer HA listener ports
    iptables -w -A ${CHAIN} -p tcp -d ${dst_ip} -m multiport --dports ${HA_PORTS} -j DROP
    # Drop inbound HA traffic coming from peer HA listener ports
    iptables -w -A ${CHAIN} -p tcp -s ${dst_ip} -m multiport --sports ${HA_PORTS} -j DROP
  "
}

csv_to_array() {
  local csv="$1"
  IFS=',' read -r -a _TMP <<< "$csv"
  printf '%s\n' "${_TMP[@]}"
}

split_groups() {
  local csvA="$1"
  local csvB="$2"

  clear_all_chains

  mapfile -t A < <(csv_to_array "$csvA")
  mapfile -t B < <(csv_to_array "$csvB")

  echo "[*] Applying HA partition:"
  echo "    A: ${csvA}"
  echo "    B: ${csvB}"

  for a in "${A[@]}"; do
    [[ -n "${CNAME[$a]:-}" ]] || { echo "Unknown node: $a"; exit 1; }
  done
  for b in "${B[@]}"; do
    [[ -n "${CNAME[$b]:-}" ]] || { echo "Unknown node: $b"; exit 1; }
  done

  for a in "${A[@]}"; do
    for b in "${B[@]}"; do
      block_peer_ha "$a" "$b"
      block_peer_ha "$b" "$a"
    done
  done

  echo "[+] Partition applied (HA ports only). HTTP ports remain reachable."
}

heal_all() {
  echo "[*] Healing cluster (iptables + netem cleanup)..."
  clear_all_chains
  for n in "${NODES[@]}"; do
    exec_in "$n" 'tc qdisc del dev eth0 root 2>/dev/null || true' || true
  done
  echo "[+] Healed."
}

delay_node() {
  local node="$1"
  local delay_ms="$2"
  local jitter_ms="${3:-0}"
  local loss_pct="${4:-0}"

  [[ -n "${CNAME[$node]:-}" ]] || { echo "Unknown node: $node"; exit 1; }

  check_tools_in_container "$node"

  # Replace root qdisc on container eth0
  exec_in "$node" "
    tc qdisc replace dev eth0 root netem delay ${delay_ms}ms ${jitter_ms}ms loss ${loss_pct}%
    tc qdisc show dev eth0
  "

  echo "[+] Applied netem on $node: delay=${delay_ms}ms jitter=${jitter_ms}ms loss=${loss_pct}%"
}

clear_netem() {
  local node="$1"
  [[ -n "${CNAME[$node]:-}" ]] || { echo "Unknown node: $node"; exit 1; }
  exec_in "$node" 'tc qdisc del dev eth0 root 2>/dev/null || true'
  echo "[+] Cleared netem on $node"
}

clear_netem_all() {
  for n in "${NODES[@]}"; do
    clear_netem "$n" || true
  done
}

status() {
  for n in "${NODES[@]}"; do
    echo "===== $n (${CNAME[$n]}) ${IP[$n]} ====="
    exec_in "$n" "
      echo '[iptables hooks]'
      iptables -w -S INPUT  | grep '${CHAIN}' || true
      iptables -w -S OUTPUT | grep '${CHAIN}' || true
      echo '[iptables chain]'
      iptables -w -S ${CHAIN} 2>/dev/null || echo '(no chain)'
      echo '[qdisc]'
      tc qdisc show dev eth0 || true
    " || true
    echo
  done
}

metrics() {
  for p in 7001 8001 9001 10001 11001; do
    echo "=== :$p /health ==="
    curl -fsS "http://127.0.0.1:${p}/health" || true
    echo
    echo "=== :$p /admin/sealed/status ==="
    curl -fsS "http://127.0.0.1:${p}/admin/sealed/status" || true
    echo
    echo "=== :$p /metrics_ha (first 25 lines) ==="
    curl -fsS "http://127.0.0.1:${p}/metrics_ha" | head -25 || true
    echo
  done
}

main() {
  need_docker

  case "${1:-}" in
    status)
      status
      ;;
    heal)
      heal_all
      ;;
    leader-minority)
      split_groups "node1,node2" "node3,node4,node5"
      ;;
    isolate)
      [[ $# -ge 2 ]] || { usage; exit 1; }
      local_node="$2"
      [[ -n "${CNAME[$local_node]:-}" ]] || { echo "Unknown node: $local_node"; exit 1; }
      others=()
      for n in "${NODES[@]}"; do
        [[ "$n" == "$local_node" ]] || others+=("$n")
      done
      split_groups "$local_node" "$(IFS=,; echo "${others[*]}")"
      ;;
    split)
      [[ $# -eq 3 ]] || { usage; exit 1; }
      split_groups "$2" "$3"
      ;;
    delay)
      [[ $# -ge 3 ]] || { usage; exit 1; }
      delay_node "$2" "$3" "${4:-0}" "${5:-0}"
      ;;
    clear-netem)
      [[ $# -eq 2 ]] || { usage; exit 1; }
      clear_netem "$2"
      ;;
    clear-netem-all)
      clear_netem_all
      ;;
    metrics)
      metrics
      ;;
    *)
      usage
      exit 1
      ;;
  esac
}

main "$@"