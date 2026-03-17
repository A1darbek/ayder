#!/usr/bin/env bash
set -eo pipefail

CHAIN="$CHAIN"; [ -n "$CHAIN" ] || CHAIN="AYDER_HA_CHAOS_LOCAL"
SUDO_BIN="$SUDO_BIN"; [ -n "$SUDO_BIN" ] || SUDO_BIN="sudo"
AUTH_BEARER="$AUTH_BEARER"; [ -n "$AUTH_BEARER" ] || AUTH_BEARER="dev"
IPTABLES_BIN="$IPTABLES_BIN"
MATCH_MODE="$MATCH_MODE"; [ -n "$MATCH_MODE" ] || MATCH_MODE="auto"
CGROUP_BASE="$CGROUP_BASE"; [ -n "$CGROUP_BASE" ] || CGROUP_BASE="ayder-chaos"
CURL_CONNECT_TIMEOUT_SEC="$CURL_CONNECT_TIMEOUT_SEC"; [ -n "$CURL_CONNECT_TIMEOUT_SEC" ] || CURL_CONNECT_TIMEOUT_SEC="0.2"
CURL_MAX_TIME_SEC="$CURL_MAX_TIME_SEC"; [ -n "$CURL_MAX_TIME_SEC" ] || CURL_MAX_TIME_SEC="0.4"
DETECT_LEADER_TIMEOUT_SEC="$DETECT_LEADER_TIMEOUT_SEC"; [ -n "$DETECT_LEADER_TIMEOUT_SEC" ] || DETECT_LEADER_TIMEOUT_SEC="2"
DETECT_LEADER_POLL_SEC="$DETECT_LEADER_POLL_SEC"; [ -n "$DETECT_LEADER_POLL_SEC" ] || DETECT_LEADER_POLL_SEC="0.2"
ISOLATE_LEADER_FALLBACK="$ISOLATE_LEADER_FALLBACK"; [ -n "$ISOLATE_LEADER_FALLBACK" ] || ISOLATE_LEADER_FALLBACK="auto-split"

NODES="node1 node2 node3 node4 node5 node6 node7"

usage() {
  cat <<'USAGE'
Usage:
  chaos-ha-local.sh check
  chaos-ha-local.sh status
  chaos-ha-local.sh heal
  chaos-ha-local.sh isolate <node>
  chaos-ha-local.sh isolate-leader
  chaos-ha-local.sh split <csvA> <csvB>
  chaos-ha-local.sh leader-minority
  chaos-ha-local.sh leader-minority6
USAGE
}

need() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing: $1" >&2; exit 1; }
}

as_root() {
  if [ "$(id -u)" = "0" ]; then
    "$@"
  else
    "$SUDO_BIN" -n "$@"
  fi
}

raft_port_for_node() {
  case "$1" in
    node1) echo 7000 ;;
    node2) echo 8000 ;;
    node3) echo 9000 ;;
    node4) echo 10000 ;;
    node5) echo 11000 ;;
    node6) echo 12000 ;;
    node7) echo 13000 ;;
    *) return 1 ;;
  esac
}

http_port_for_node() {
  case "$1" in
    node1) echo 7001 ;;
    node2) echo 8001 ;;
    node3) echo 9001 ;;
    node4) echo 10001 ;;
    node5) echo 11001 ;;
    node6) echo 12001 ;;
    node7) echo 13001 ;;
    *) return 1 ;;
  esac
}

resolve_iptables_bin() {
  if [ -n "$IPTABLES_BIN" ]; then return; fi
  IPTABLES_BIN="$(command -v iptables || true)"
  if [ -z "$IPTABLES_BIN" ]; then
    local root_dir
    root_dir="$(cd "$(dirname "$0")/.." && pwd)"
    local bundled="$root_dir/tests/jepsen/.toolchain/bin/iptables"
    if [ -x "$bundled" ]; then IPTABLES_BIN="$bundled"; fi
  fi
  [ -n "$IPTABLES_BIN" ] || { echo "missing iptables" >&2; exit 1; }
}

ipt() {
  as_root "$IPTABLES_BIN" "$@"
}

match_mode_auto_detect() {
  if "$IPTABLES_BIN" -m owner -h 2>&1 | grep -q -- "--pid-owner"; then
    echo "pid-owner"; return
  fi
  if "$IPTABLES_BIN" -m cgroup -h 2>&1 | grep -q -- "--path"; then
    echo "cgroup-path"; return
  fi
  echo "unsupported"
}

resolve_match_mode() {
  if [ "$MATCH_MODE" = "auto" ]; then MATCH_MODE="$(match_mode_auto_detect)"; fi
  case "$MATCH_MODE" in
    pid-owner)
      "$IPTABLES_BIN" -m owner -h 2>&1 | grep -q -- "--pid-owner" || { echo "pid-owner unavailable" >&2; exit 1; }
      ;;
    cgroup-path)
      "$IPTABLES_BIN" -m cgroup -h 2>&1 | grep -q -- "--path" || { echo "cgroup path unavailable" >&2; exit 1; }
      [ -d /sys/fs/cgroup ] || { echo "cgroup v2 required" >&2; exit 1; }
      ;;
    *)
      echo "No supported local partition mode" >&2
      exit 1
      ;;
  esac
}

check_root_ready() {
  if [ "$(id -u)" = "0" ]; then return; fi
  "$SUDO_BIN" -n true >/dev/null 2>&1 || { echo "Need root privileges" >&2; exit 1; }
}

ensure_chain() {
  ipt -w -N "$CHAIN" 2>/dev/null || true
  ipt -w -C OUTPUT -j "$CHAIN" 2>/dev/null || ipt -w -I OUTPUT 1 -j "$CHAIN"
}

clear_chain() {
  ipt -w -D OUTPUT -j "$CHAIN" 2>/dev/null || true
  ipt -w -F "$CHAIN" 2>/dev/null || true
  ipt -w -X "$CHAIN" 2>/dev/null || true
}

csv_to_nodes() {
  echo "$1" | tr ',' '\n' | sed '/^$/d'
}

raft_ports_for_csv() {
  local out=""
  local n p
  while IFS= read -r n; do
    p="$(raft_port_for_node "$n" || true)"
    [ -n "$p" ] || { echo "unknown node: $n" >&2; exit 1; }
    if [ -n "$out" ]; then out="$out,$p"; else out="$p"; fi
  done < <(csv_to_nodes "$1")
  echo "$out"
}

all_other_nodes_csv() {
  local out=""
  local n
  for n in $NODES; do
    [ "$n" = "$1" ] && continue
    if [ -n "$out" ]; then out="$out,$n"; else out="$n"; fi
  done
  echo "$out"
}

pids_for_node() {
  local http_port
  http_port="$(http_port_for_node "$1" || true)"
  [ -n "$http_port" ] || return 0
  pgrep -f "(ayder|ramforge).*--port[[:space:]]+$http_port" || true
}

node_is_running() {
  local p
  p="$(pids_for_node "$1" || true)"
  [ -n "$p" ]
}

node_cgroup_rel_path() {
  echo "$CGROUP_BASE/$1"
}

move_node_pids_to_cgroup() {
  local node="$1"
  local cg_abs="/sys/fs/cgroup/$(node_cgroup_rel_path "$node")"
  as_root mkdir -p "$cg_abs"
  local moved=0
  local pid
  while IFS= read -r pid; do
    [ -n "$pid" ] || continue
    if printf '%s\n' "$pid" | as_root tee "$cg_abs/cgroup.procs" >/dev/null 2>&1; then moved=1; fi
  done < <(pids_for_node "$node")
  if [ "$moved" = "0" ]; then
    echo "[WARN] no matching pids moved to cgroup for $node" >&2
  fi
}

apply_pid_drop_rules() {
  local dst_ports
  dst_ports="$(raft_ports_for_csv "$2")"
  local src_node pid
  while IFS= read -r src_node; do
    while IFS= read -r pid; do
      [ -n "$pid" ] || continue
      ipt -w -A "$CHAIN" -p tcp -m owner --pid-owner "$pid" -m multiport --dports "$dst_ports" -j DROP
    done < <(pids_for_node "$src_node")
  done < <(csv_to_nodes "$1")
}

apply_cgroup_drop_rules() {
  local dst_ports
  dst_ports="$(raft_ports_for_csv "$2")"
  local src_node cg_rel
  while IFS= read -r src_node; do
    [ -n "$src_node" ] || continue
    move_node_pids_to_cgroup "$src_node"
    cg_rel="$(node_cgroup_rel_path "$src_node")"
    ipt -w -A "$CHAIN" -p tcp -m cgroup --path "$cg_rel" -m multiport --dports "$dst_ports" -j DROP
  done < <(csv_to_nodes "$1")
}

split_groups() {
  clear_chain
  ensure_chain
  case "$MATCH_MODE" in
    pid-owner)
      apply_pid_drop_rules "$1" "$2"
      apply_pid_drop_rules "$2" "$1"
      ;;
    cgroup-path)
      apply_cgroup_drop_rules "$1" "$2"
      apply_cgroup_drop_rules "$2" "$1"
      ;;
    *)
      echo "unsupported MATCH_MODE=$MATCH_MODE" >&2
      exit 1
      ;;
  esac
  echo "[+] Applied local HA split"
  echo "    mode: $MATCH_MODE"
  echo "    A: $1"
  echo "    B: $2"
}

isolate_node() {
  raft_port_for_node "$1" >/dev/null || { echo "unknown node: $1" >&2; exit 1; }
  split_groups "$1" "$(all_other_nodes_csv "$1")"
}

detect_leader_node() {
  local start now n hp m role
  start="$(date +%s)"
  while true; do
    for n in $NODES; do
      hp="$(http_port_for_node "$n" || true)"
      [ -n "$hp" ] || continue
      node_is_running "$n" || continue
      if [ -n "$AUTH_BEARER" ]; then
        m="$(curl --connect-timeout "$CURL_CONNECT_TIMEOUT_SEC" --max-time "$CURL_MAX_TIME_SEC" -fsS -H "Authorization: Bearer $AUTH_BEARER" "http://127.0.0.1:$hp/metrics_ha" 2>/dev/null || true)"
      else
        m="$(curl --connect-timeout "$CURL_CONNECT_TIMEOUT_SEC" --max-time "$CURL_MAX_TIME_SEC" -fsS "http://127.0.0.1:$hp/metrics_ha" 2>/dev/null || true)"
      fi
      [ -n "$m" ] || continue
      role="$(awk '/^ramforge_ha_role /{print $2}' <<< "$m" | tail -1)"
      if [ "$role" = "0" ]; then
        echo "$n"
        return 0
      fi
    done
    now="$(date +%s)"
    if [ $((now - start)) -ge "$DETECT_LEADER_TIMEOUT_SEC" ]; then
      return 1
    fi
    sleep "$DETECT_LEADER_POLL_SEC"
  done
}

fallback_isolate_leader() {
  case "$ISOLATE_LEADER_FALLBACK" in
    auto-split)
      if node_is_running node6; then
        split_groups "node1,node2" "node3,node4,node5,node6"
      else
        split_groups "node1,node2" "node3,node4,node5"
      fi
      ;;
    node1)
      isolate_node node1
      ;;
    none)
      return 1
      ;;
    *)
      echo "unknown ISOLATE_LEADER_FALLBACK=$ISOLATE_LEADER_FALLBACK" >&2
      return 1
      ;;
  esac
}

status_cmd() {
  check_root_ready
  echo "[mode] $MATCH_MODE"
  echo "[hooks]"
  ipt -w -S OUTPUT | grep "$CHAIN" || true
  echo "[chain]"
  ipt -w -S "$CHAIN" 2>/dev/null || echo "(no chain)"
}

check_cmd() {
  check_root_ready
  resolve_match_mode
  echo "[+] local chaos prerequisites OK (mode=$MATCH_MODE)"
}

heal_cmd() {
  check_root_ready
  clear_chain
  echo "[+] Healed local HA chaos rules"
}

main() {
  resolve_iptables_bin
  resolve_match_mode
  need pgrep
  need curl
  need awk

  case "$1" in
    status)
      status_cmd
      ;;
    check)
      check_cmd
      ;;
    heal)
      heal_cmd
      ;;
    isolate)
      [ "$#" = "2" ] || { usage; exit 1; }
      check_root_ready
      isolate_node "$2"
      ;;
    isolate-leader)
      [ "$#" = "1" ] || { usage; exit 1; }
      check_root_ready
      leader="$(detect_leader_node || true)"
      if [ -z "$leader" ]; then
        echo "[WARN] could not detect leader; applying fallback partition mode '$ISOLATE_LEADER_FALLBACK'" >&2
        fallback_isolate_leader || { echo "could not detect leader" >&2; exit 1; }
      else
        isolate_node "$leader"
      fi
      ;;
    split)
      [ "$#" = "3" ] || { usage; exit 1; }
      check_root_ready
      split_groups "$2" "$3"
      ;;
    leader-minority)
      check_root_ready
      split_groups "node1,node2" "node3,node4,node5"
      ;;
    leader-minority6)
      check_root_ready
      split_groups "node1,node2,node3" "node4,node5,node6"
      ;;
    *)
      usage
      exit 1
      ;;
  esac
}

main "$@"


