#!/usr/bin/env bash
# tests/demo/ha_kill9_amnesia_local.sh
# Mid-write chaos test: kill -9 + remove append.aof* + restart one node.
# Verifies snapshot catchup by comparing leader vs recovered node at probe offset.

set -Eeuo pipefail

LEADER_URL="${LEADER_URL:-http://127.0.0.1:7001}"
AUTH_BEARER="${AUTH_BEARER:-dev}"
TOPIC="${TOPIC:-ha_amnesia_demo}"
PARTITION="${PARTITION:-0}"
PRODUCE_COUNT="${PRODUCE_COUNT:-15000}"
KILL_AFTER_SEC="${KILL_AFTER_SEC:-2}"
KILL_NODE_ID="${KILL_NODE_ID:-node3}"
CLUSTER_ROOT="${CLUSTER_ROOT:-$HOME/project/ayder/cluster}"
WORKERS="${WORKERS:-4}"
WAIT_RECOVER_SEC="${WAIT_RECOVER_SEC:-45}"

# If empty, script auto-restarts node by sourcing tests/demo/ha6_stability_env_preset.sh
# and running ../../ayder --port <node_http_port> --workers <WORKERS> from node dir.
RESTART_CMD="${RESTART_CMD:-}"

HDR=()
if [[ -n "$AUTH_BEARER" ]]; then
  HDR=(-H "Authorization: Bearer ${AUTH_BEARER}")
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing: $1" >&2; exit 1; }; }
need curl
need jq
need pgrep

tmpdir="$(mktemp -d)"
producer_pid=""

cleanup() {
  if [[ -n "$producer_pid" ]] && kill -0 "$producer_pid" 2>/dev/null; then
    kill "$producer_pid" 2>/dev/null || true
    wait "$producer_pid" 2>/dev/null || true
  fi
  rm -rf "$tmpdir"
}
trap cleanup EXIT

echo "[1/8] Fetch membership and resolve target node"
membership="$(curl -sS "${HDR[@]}" "$LEADER_URL/ha/membership")"
node_http="$(jq -r --arg nid "$KILL_NODE_ID" '.membership.nodes[] | select(.node_id==$nid) | .http_addr' <<<"$membership")"
if [[ -z "$node_http" || "$node_http" == "null" ]]; then
  echo "Could not find $KILL_NODE_ID in membership" >&2
  exit 1
fi
node_http_port="$(sed -E 's#.*:([0-9]+)$#\1#' <<<"$node_http")"
node_dir="${CLUSTER_ROOT}/${KILL_NODE_ID}"
if [[ ! -d "$node_dir" ]]; then
  echo "Node directory not found: $node_dir" >&2
  exit 1
fi

mapfile -t cluster_urls < <(jq -r '.membership.nodes[].http_addr' <<<"$membership")
pick_alive_url() {
  for u in "${cluster_urls[@]}"; do
    if curl -fsS "${HDR[@]}" "$u/health" >/dev/null 2>&1; then
      echo "$u"
      return 0
    fi
  done
  return 1
}

echo "Target node: $KILL_NODE_ID ($node_http, dir=$node_dir)"

echo "[2/8] Ensure topic exists"
curl -sS -X POST "${HDR[@]}" -H 'Content-Type: application/json' \
  "$LEADER_URL/broker/topics" \
  -d "{\"name\":\"$TOPIC\",\"partitions\":8}" >/dev/null || true

echo "[3/8] Start producer"
(
  for i in $(seq 1 "$PRODUCE_COUNT"); do
    curl -sS -X POST "${HDR[@]}" \
      "$LEADER_URL/broker/topics/$TOPIC/produce?partition=$PARTITION" \
      --data-binary "{\"i\":$i}" >/dev/null || true
  done
) &
producer_pid="$!"

sleep "$KILL_AFTER_SEC"

echo "[4/8] Kill target node process and delete local AOF files"
pid="$(pgrep -f "ayder --port ${node_http_port}" | head -n1 || true)"
if [[ -z "$pid" ]]; then
  echo "No running ayder PID found for port $node_http_port" >&2
  exit 1
fi
kill -9 "$pid"

rm -f "$node_dir/append.aof" \
      "$node_dir/append.aof.sealed" \
      "$node_dir/append.aof.sealed.lock"

echo "Killed PID=$pid and removed append.aof* in $node_dir"

echo "[5/8] Restart target node"
if [[ -n "$RESTART_CMD" ]]; then
  bash -lc "$RESTART_CMD" >"$tmpdir/restart-${KILL_NODE_ID}.log" 2>&1 &
else
  bash -lc "
    cd '$node_dir'
    source '$ROOT_DIR/tests/demo/ha6_stability_env_preset.sh' '$KILL_NODE_ID'
    ../../ayder --port '$node_http_port' --workers '$WORKERS'
  " >"$tmpdir/restart-${KILL_NODE_ID}.log" 2>&1 &
fi

restart_pid="$!"
echo "Restart launched as PID=$restart_pid (log: $tmpdir/restart-${KILL_NODE_ID}.log)"

echo "[6/8] Wait recovered node health"
start_ts="$(date +%s)"
while true; do
  if curl -fsS "${HDR[@]}" "$node_http/health" >/dev/null 2>&1; then
    break
  fi
  now_ts="$(date +%s)"
  if (( now_ts - start_ts > WAIT_RECOVER_SEC )); then
    echo "Recovered node health timeout (${WAIT_RECOVER_SEC}s)" >&2
    exit 1
  fi
  sleep 0.5
done

echo "[7/8] Stop producer and compute probe offset"
if kill -0 "$producer_pid" 2>/dev/null; then
  kill "$producer_pid" 2>/dev/null || true
  wait "$producer_pid" 2>/dev/null || true
fi

api_url="$(pick_alive_url || true)"
if [[ -z "$api_url" ]]; then
  api_url="$LEADER_URL"
fi

leader_tail_json="$(curl -sS "${HDR[@]}" "$api_url/broker/consume/$TOPIC/cpcheck/$PARTITION?offset=-1&limit=1")"
leader_tail_off="$(jq -r '.messages[0].offset // -1' <<<"$leader_tail_json")"
if [[ "$leader_tail_off" == "-1" ]]; then
  echo "Leader has no readable tail for topic=$TOPIC partition=$PARTITION" >&2
  echo "$leader_tail_json" >&2
  exit 1
fi
probe_off=$(( leader_tail_off > 100 ? leader_tail_off - 100 : 0 ))

echo "Leader tail offset=$leader_tail_off, probe offset=$probe_off"

echo "[8/8] Verify recovered node catches up to probe offset"
start_ts="$(date +%s)"
while true; do
  a="$(curl -sS "${HDR[@]}" "$api_url/broker/consume/$TOPIC/cpcheck/$PARTITION?offset=$probe_off&limit=1")"
  b="$(curl -sS "${HDR[@]}" "$node_http/broker/consume/$TOPIC/cpcheck/$PARTITION?offset=$probe_off&limit=1")"
  av="$(jq -r '.messages[0].value // empty' <<<"$a")"
  bv="$(jq -r '.messages[0].value // empty' <<<"$b")"

  if [[ -n "$av" && "$av" == "$bv" ]]; then
    echo "PASS: recovered node caught up at offset $probe_off"
    echo "leader_value=$av"
    exit 0
  fi

  now_ts="$(date +%s)"
  if (( now_ts - start_ts > WAIT_RECOVER_SEC )); then
    echo "Catchup verify timeout (${WAIT_RECOVER_SEC}s)" >&2
    echo "Leader sample:   $a" >&2
    echo "Recovered sample:$b" >&2
    exit 1
  fi
  sleep 1
done

