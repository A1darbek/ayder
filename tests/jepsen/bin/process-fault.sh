#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CLUSTER_ROOT="${AYDER_JEPSEN_CLUSTER_ROOT:-$ROOT_DIR/cluster}"
AYDER_BIN="${AYDER_JEPSEN_AYDER_BIN:-$ROOT_DIR/ayder}"
NODE_ID="${AYDER_JEPSEN_FAULT_NODE:-node3}"
WORKERS="${AYDER_JEPSEN_FAULT_WORKERS:-2}"
READY_TIMEOUT_SEC="${AYDER_JEPSEN_RESTART_READY_TIMEOUT_SEC:-30}"

case "$NODE_ID" in
  node1) HTTP_PORT=7001 ;;
  node2) HTTP_PORT=8001 ;;
  node3) HTTP_PORT=9001 ;;
  node4) HTTP_PORT=10001 ;;
  node5) HTTP_PORT=11001 ;;
  node6) HTTP_PORT=12001 ;;
  *)
    printf 'unsupported AYDER_JEPSEN_FAULT_NODE: %s\n' "$NODE_ID" >&2
    exit 2
    ;;
esac

node_pids() {
  pgrep -f "[a]yder --port ${HTTP_PORT}" || true
}

case "${1:-}" in
  kill)
    pids="$(node_pids)"
    [[ -n "$pids" ]] || {
      printf 'no running %s process found on port %s\n' "$NODE_ID" "$HTTP_PORT" >&2
      exit 1
    }
    for pid in $pids; do
      kill -9 "$pid"
    done
    for _ in $(seq 1 50); do
      [[ -z "$(node_pids)" ]] && {
        printf 'killed %s on port %s (pids: %s)\n' "$NODE_ID" "$HTTP_PORT" "$pids"
        exit 0
      }
      sleep 0.1
    done
    printf '%s did not stop after SIGKILL\n' "$NODE_ID" >&2
    exit 1
    ;;
  restart)
    [[ -x "$AYDER_BIN" ]] || {
      printf 'Ayder binary is not executable: %s\n' "$AYDER_BIN" >&2
      exit 1
    }
    node_dir="$CLUSTER_ROOT/$NODE_ID"
    [[ -d "$node_dir" ]] || {
      printf 'node directory does not exist: %s\n' "$node_dir" >&2
      exit 1
    }
    if [[ -n "$(node_pids)" ]]; then
      if curl -fsS --connect-timeout 1 --max-time 1 \
          "http://127.0.0.1:${HTTP_PORT}/ready" >/dev/null 2>&1; then
        printf '%s is already running and ready; recovery is complete\n' "$NODE_ID"
        exit 0
      fi
      printf '%s is running but not ready; refusing a duplicate process\n' "$NODE_ID" >&2
      exit 1
    fi
    (
      cd "$node_dir"
      # shellcheck disable=SC1091
      source "$ROOT_DIR/tests/demo/ha6_stability_env_preset.sh" "$NODE_ID" >/dev/null
      nohup setsid "$AYDER_BIN" --port "$HTTP_PORT" --workers "$WORKERS" \
        >"/tmp/ayder-${NODE_ID}-jepsen.log" 2>&1 < /dev/null &
    )
    for _ in $(seq 1 $((READY_TIMEOUT_SEC * 10))); do
      if curl -fsS --connect-timeout 1 --max-time 1 \
          "http://127.0.0.1:${HTTP_PORT}/ready" >/dev/null 2>&1; then
        [[ -n "$(node_pids)" ]] || {
          printf '%s readiness came from no matching process\n' "$NODE_ID" >&2
          exit 1
        }
        printf 'restarted %s on port %s\n' "$NODE_ID" "$HTTP_PORT"
        exit 0
      fi
      sleep 0.1
    done
    printf '%s did not become ready within %ss\n' "$NODE_ID" "$READY_TIMEOUT_SEC" >&2
    exit 1
    ;;
  *)
    printf 'usage: %s {kill|restart}\n' "$0" >&2
    exit 2
    ;;
esac
