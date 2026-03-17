#!/usr/bin/env bash
# tests/demo/ha6_stability_env_preset.sh
# Usage:
#   source tests/demo/ha6_stability_env_preset.sh node1
# Then run (from node dir):
#   ../../ayder --port "$AYDER_HTTP_PORT" --workers 4

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  echo "Source this file, do not execute it:"
  echo "  source tests/demo/ha6_stability_env_preset.sh node1"
  exit 1
fi

NODE_ID="${1:-${RF_HA_NODE_ID:-}}"
if [[ -z "$NODE_ID" ]]; then
  echo "Missing node id. Example: source tests/demo/ha6_stability_env_preset.sh node1"
  return 1 2>/dev/null || exit 1
fi

case "$NODE_ID" in
  node1) export AYDER_RAFT_PORT=7000;  export AYDER_HTTP_PORT=7001;  export RF_HA_BOOTSTRAP_LEADER=1 ;;
  node2) export AYDER_RAFT_PORT=8000;  export AYDER_HTTP_PORT=8001;  unset RF_HA_BOOTSTRAP_LEADER ;;
  node3) export AYDER_RAFT_PORT=9000;  export AYDER_HTTP_PORT=9001;  unset RF_HA_BOOTSTRAP_LEADER ;;
  node4) export AYDER_RAFT_PORT=10000; export AYDER_HTTP_PORT=10001; unset RF_HA_BOOTSTRAP_LEADER ;;
  node5) export AYDER_RAFT_PORT=11000; export AYDER_HTTP_PORT=11001; unset RF_HA_BOOTSTRAP_LEADER ;;
  node6) export AYDER_RAFT_PORT=12000; export AYDER_HTTP_PORT=12001; unset RF_HA_BOOTSTRAP_LEADER ;;
  *)
    echo "Unsupported node id: $NODE_ID (expected node1..node6)"
    return 1 2>/dev/null || exit 1
    ;;
esac

export RF_HA_ENABLED=1
export RF_HA_NODE_ID="$NODE_ID"

# Static bootstrap topology (runtime membership will take over after commits replay).
export RF_HA_NODES='node1:127.0.0.1:7000:100,node2:127.0.0.1:8000:80,node3:127.0.0.1:9000:60,node4:127.0.0.1:10000:40,node5:127.0.0.1:11000:20,node6:127.0.0.1:12000:10'

# 6 voters => majority is 4.
export RF_HA_WRITE_CONCERN=4

# Linearizable claim path: wait for majority replication before ACK.
export RF_HA_SYNC_MODE="${RF_HA_SYNC_MODE:-1}"

# Core timing.
export RF_HA_ELECTION_TIMEOUT_MS=3000
export RF_HA_HEARTBEAT_MS=50

# Throughput and bus tuning.
export RF_HA_MAX_MSG_BYTES=16777216
export RF_HA_REPLICATION_BATCH=2048
export RF_BUS_DESC=4194304
export RF_BUS_HEAP_MB=2048
export RF_HA_DEDICATED_WORKER=0

# Stability knobs from latest fixes.
export RF_HA_BOOTSTRAP_GATE_MS=9000
export RF_HA_QUORUM_LEASE_MULT_PCT=150
export RF_HA_QUORUM_RECHECK_GRACE_MS=800
export RF_HA_QUORUM_MISS_STREAK=4
export RF_HA_CFG_APPLY_LOG_THROTTLE_MS=1500

# Auth + TLS.
export RF_BEARER_TOKENS='dev@55555555555555:11111111111111111:111111111111111111111'
export RF_HTTP_DISABLE_RL="${RF_HTTP_DISABLE_RL:-1}"
export RF_HA_TLS=1
export RF_HA_TLS_CA=../certs/ca.crt
export RF_HA_TLS_CERT="../certs/${NODE_ID}.crt"
export RF_HA_TLS_KEY="../certs/${NODE_ID}.key"

echo "Loaded HA preset for $NODE_ID"
echo "  raft_port=$AYDER_RAFT_PORT http_port=$AYDER_HTTP_PORT"
echo "Start command: ../../ayder --port $AYDER_HTTP_PORT --workers 4"

