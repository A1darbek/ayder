#!/bin/bash
# start_node.sh - put in each node directory

NODE_ID=$(basename $(pwd))  # node1, node2, or node3
PORT_MAP="node1:7001 node2:8001 node3:9001"
CLUSTER_PORT_MAP="node1:7000 node2:8000 node3:9000"

PORT=$(echo $PORT_MAP | tr ' ' '\n' | grep $NODE_ID | cut -d: -f2)

export RF_HA_ENABLED=1
export RF_HA_NODE_ID=$NODE_ID
export RF_HA_NODES='node1:127.0.0.1:7000:100,node2:127.0.0.1:8000:20,node3:127.0.0.1:9000:10'
export RF_HA_WRITE_CONCERN=2
export RF_BEARER_TOKENS='dev@55555555555555:11111111111111111:111111111111111111111'

# mTLS
export RF_HA_TLS=1
export RF_HA_TLS_CA=../certs/ca.crt
export RF_HA_TLS_CERT=../certs/${NODE_ID}.crt
export RF_HA_TLS_KEY=../certs/${NODE_ID}.key

# Bootstrap only node1
[[ "$NODE_ID" == "node1" ]] && export RF_HA_BOOTSTRAP_LEADER=1

exec ../../ramforge --port $PORT --workers 12