#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER_ROOT="${CLUSTER_ROOT:-$ROOT_DIR/cluster}"
AYDER_BIN="${AYDER_BIN:-$ROOT_DIR/ayder}"
WORKERS="${WORKERS:-4}"
TOKEN="${TOKEN:-${AYDER_JEPSEN_TOKEN:-dev}}"
AUTH_HEADER="Authorization: Bearer ${TOKEN}"
READINESS_WAIT_SEC="${READINESS_WAIT_SEC:-120}"
FORCE_RESTART_ALL="${FORCE_RESTART_ALL:-auto}"
WAIT_FOR_READY="${WAIT_FOR_READY:-0}"
CURL_CONNECT_TIMEOUT_SEC="${CURL_CONNECT_TIMEOUT_SEC:-1}"
CURL_MAX_TIME_SEC="${CURL_MAX_TIME_SEC:-2}"
NODE_BOOT_TIMEOUT_SEC="${NODE_BOOT_TIMEOUT_SEC:-30}"
NODE_BOOT_RETRIES="${NODE_BOOT_RETRIES:-3}"
RECOVERY_BACKOFF_SEC="${RECOVERY_BACKOFF_SEC:-1}"
CLEANUP_SHARED_IPC="${CLEANUP_SHARED_IPC:-1}"
SHARED_STORAGE_SHM_PATH="${SHARED_STORAGE_SHM_PATH:-/dev/shm/ramforge_storage_v2}"
METRICS_SHM_PATH="${METRICS_SHM_PATH:-/dev/shm/ramforge_metrics}"

NODES=(node1 node2 node3 node4 node5 node6)

log() {
  printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

need() {
  command -v "$1" >/dev/null 2>&1 || {
    log "ERROR: missing command: $1"
    exit 1
  }
}

http_port_for_node() {
  case "$1" in
    node1) echo 7001 ;;
    node2) echo 8001 ;;
    node3) echo 9001 ;;
    node4) echo 10001 ;;
    node5) echo 11001 ;;
    node6) echo 12001 ;;
    *) return 1 ;;
  esac
}

node_url() {
  local port
  port="$(http_port_for_node "$1")"
  printf 'http://127.0.0.1:%s' "$port"
}

node_log_file() {
  printf '/tmp/ayder-%s.log' "$1"
}

curl_auth_ok() {
  local url="$1"
  curl -fsS --connect-timeout "$CURL_CONNECT_TIMEOUT_SEC" --max-time "$CURL_MAX_TIME_SEC" \
    -H "$AUTH_HEADER" "$url" >/dev/null 2>&1
}

curl_auth_text() {
  local url="$1"
  curl -fsS --connect-timeout "$CURL_CONNECT_TIMEOUT_SEC" --max-time "$CURL_MAX_TIME_SEC" \
    -H "$AUTH_HEADER" "$url" 2>/dev/null || true
}

node_healthy() {
  local url
  url="$(node_url "$1")"
  curl_auth_ok "$url/health"
}

leader_fresh_quorum() {
  local node url metrics role fq
  for node in "${NODES[@]}"; do
    url="$(node_url "$node")"
    metrics="$(curl_auth_text "$url/metrics_ha")"
    [[ -n "$metrics" ]] || continue

    role="$(awk '/^ramforge_ha_role /{print $2; exit}' <<<"$metrics")"
    fq="$(awk '/^ramforge_ha_fresh_quorum /{print $2; exit}' <<<"$metrics")"
    if [[ "$role" == '0' && "$fq" == '1' ]]; then
      return 0
    fi
  done
  return 1
}

node_running() {
  local node_id="$1"
  local port
  port="$(http_port_for_node "$node_id")"
  pgrep -f "[a]yder --port ${port}" >/dev/null 2>&1
}

any_node_process_running() {
  local node
  for node in "${NODES[@]}"; do
    if node_running "$node"; then
      return 0
    fi
  done
  return 1
}

kill_node() {
  local node_id="$1"
  local port pids p
  port="$(http_port_for_node "$node_id")"
  pids="$(pgrep -f "[a]yder --port ${port}" || true)"
  for p in $pids; do
    kill -9 "$p" 2>/dev/null || true
  done
}

wait_node_stopped() {
  local node_id="$1"
  local timeout_sec="${2:-5}"
  local start now
  start="$(date +%s)"

  while node_running "$node_id"; do
    now="$(date +%s)"
    if (( now - start >= timeout_sec )); then
      return 1
    fi
    sleep 1
  done

  return 0
}

cleanup_shared_ipc() {
  [[ "$CLEANUP_SHARED_IPC" == '1' ]] || return 0

  if [[ -e "$SHARED_STORAGE_SHM_PATH" ]]; then
    rm -f "$SHARED_STORAGE_SHM_PATH" || true
  fi
  if [[ -e "$METRICS_SHM_PATH" ]]; then
    rm -f "$METRICS_SHM_PATH" || true
  fi
}

start_node() {
  local node_id="$1"
  local node_dir="${CLUSTER_ROOT}/${node_id}"

  [[ -d "$node_dir" ]] || {
    log "ERROR: node directory not found: $node_dir"
    exit 1
  }

  (
    cd "$node_dir"
    # shellcheck disable=SC1091
    source "$ROOT_DIR/tests/demo/ha6_stability_env_preset.sh" "$node_id"
    nohup "$AYDER_BIN" --port "$AYDER_HTTP_PORT" --workers "$WORKERS" >"$(node_log_file "$node_id")" 2>&1 &
  )
}

wait_node_healthy() {
  local node_id="$1"
  local timeout_sec="$2"
  local start now
  start="$(date +%s)"

  while true; do
    if node_healthy "$node_id"; then
      return 0
    fi

    now="$(date +%s)"
    if (( now - start >= timeout_sec )); then
      return 1
    fi

    sleep 1
  done
}

node_log_has_shared_storage_error() {
  local node_id="$1"
  local f
  f="$(node_log_file "$node_id")"
  [[ -f "$f" ]] || return 1
  grep -Eq 'Failed to initialize shared storage|shm_open init_named: File exists' "$f"
}

start_node_with_retry() {
  local node_id="$1"
  local attempt

  for attempt in $(seq 1 "$NODE_BOOT_RETRIES"); do
    kill_node "$node_id"
    if ! wait_node_stopped "$node_id" 5; then
      log "recover: node ${node_id} still running after kill; retrying hard kill"
      kill_node "$node_id"
      wait_node_stopped "$node_id" 3 || true
    fi

    if (( attempt > 1 )); then
      sleep "$RECOVERY_BACKOFF_SEC"
    fi

    start_node "$node_id"

    if wait_node_healthy "$node_id" "$NODE_BOOT_TIMEOUT_SEC"; then
      log "recover: node ${node_id} healthy (attempt ${attempt}/${NODE_BOOT_RETRIES})"
      return 0
    fi

    log "recover: node ${node_id} failed to become healthy (attempt ${attempt}/${NODE_BOOT_RETRIES})"

    if node_log_has_shared_storage_error "$node_id"; then
      log "recover: detected shared-storage startup failure for ${node_id}; cleaning stale shm and retrying"
      if ! any_node_process_running; then
        cleanup_shared_ipc
      fi
    fi
  done

  local f
  f="$(node_log_file "$node_id")"
  if [[ -f "$f" ]]; then
    log "recover: last ${node_id} log tail follows"
    tail -n 80 "$f" || true
  fi
  return 1
}

restart_all_nodes() {
  local node
  for node in "${NODES[@]}"; do
    kill_node "$node"
  done

  for node in "${NODES[@]}"; do
    wait_node_stopped "$node" 5 || true
  done

  sleep 1
  cleanup_shared_ipc

  for node in "${NODES[@]}"; do
    start_node_with_retry "$node" || return 1
  done

  return 0
}

restart_unhealthy_nodes() {
  local node
  for node in "${NODES[@]}"; do
    if node_healthy "$node"; then
      continue
    fi
    start_node_with_retry "$node" || return 1
  done

  return 0
}

count_healthy() {
  local node healthy
  healthy=0
  for node in "${NODES[@]}"; do
    if node_healthy "$node"; then
      healthy=$((healthy + 1))
    fi
  done
  echo "$healthy"
}

main() {
  need curl
  need awk
  need pgrep

  [[ -x "$AYDER_BIN" ]] || {
    log "ERROR: ayder binary is not executable: $AYDER_BIN"
    exit 1
  }

  local healthy restart_all
  local deadline now current leader_fq last_log

  healthy="$(count_healthy)"

  restart_all=0
  case "$FORCE_RESTART_ALL" in
    1|true|yes)
      restart_all=1
      ;;
    0|false|no)
      restart_all=0
      ;;
    auto)
      if (( healthy == 0 )); then
        restart_all=1
      elif (( healthy >= 4 )) && ! leader_fresh_quorum; then
        restart_all=1
      fi
      ;;
    *)
      log "ERROR: invalid FORCE_RESTART_ALL=$FORCE_RESTART_ALL (expected auto|0|1)"
      exit 1
      ;;
  esac

  if (( restart_all == 1 )); then
    log "recover: restarting all nodes healthy=${healthy}/6"
    restart_all_nodes || {
      log 'recover: full restart failed'
      exit 1
    }
  else
    log "recover: restarting unhealthy nodes healthy=${healthy}/6"
    if ! restart_unhealthy_nodes; then
      log 'recover: partial restart failed; falling back to full restart'
      restart_all_nodes || {
        log 'recover: fallback full restart failed'
        exit 1
      }
    fi
  fi

  if [[ "$WAIT_FOR_READY" != '1' ]]; then
    log 'recover: restart actions completed (WAIT_FOR_READY=0)'
    exit 0
  fi

  deadline=$(( $(date +%s) + READINESS_WAIT_SEC ))
  last_log=$(( $(date +%s) - 5 ))
  while true; do
    current="$(count_healthy)"
    leader_fq=0
    if leader_fresh_quorum; then
      leader_fq=1
    fi

    if (( current >= 4 && leader_fq == 1 )); then
      log "recover: ready healthy=${current}/6 leader_fresh_quorum=1"
      exit 0
    fi

    now="$(date +%s)"
    if (( now - last_log >= 5 )); then
      log "recover: pending healthy=${current}/6 leader_fresh_quorum=${leader_fq}"
      last_log="$now"
    fi

    if (( now >= deadline )); then
      log "ERROR: recover timeout healthy=${current}/6 leader_fresh_quorum=${leader_fq}"
      exit 1
    fi

    sleep 1
  done
}

main "$@"


