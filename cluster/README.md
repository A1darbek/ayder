# Local Cluster Layout

This folder holds local HA node directories and TLS cert assets.

## Structure

- `certs/`: CA and node cert/key files for local TLS.
- `node1` ... `node7`: per-node working directories.
- `generate_certs.sh`: cert generation helper.

## Default Local Ports

- `node1`: raft `7000`, http `7001`
- `node2`: raft `8000`, http `8001`
- `node3`: raft `9000`, http `9001`
- `node4`: raft `10000`, http `10001`
- `node5`: raft `11000`, http `11001`
- `node6`: raft `12000`, http `12001`
- `node7`: raft `13000`, http `13001`

## Recommended Start/Recovery

Use the recovery wrapper:

```bash
bash ./tests/demo/ha6_recover_local_cluster.sh
```

It handles retries and shared-storage startup recovery logic.

## Manual Start (single node)

```bash
cd cluster/node1
source ../../tests/demo/ha6_stability_env_preset.sh node1
../../ayder --port "$AYDER_HTTP_PORT" --workers 4
```

Logs are written to `/tmp/ayder-nodeX.log` by the recovery script.