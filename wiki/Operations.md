# Operations

This page is the short runbook for local and HA work.

## Start points

- Single node quick start: [[Quickstart]]
- 6-node recovery flow: `tests/demo/ha6_recover_local_cluster.sh`
- Jepsen gold wrapper: `tests/demo/ha_broker_jepsen_gold.sh`

## Common health checks

- `/health`
- `/ready`
- `/metrics_ha`
- `/admin/sealed/status`

## Default 6-node ports

- `7001`, `8001`, `9001`, `10001`, `11001`, `12001`

The matching Raft ports are `7000`, `8000`, `9000`, `10000`, `11000`, `12000`.

## Useful environment

The repo docs and run scripts commonly use:

- `TOKEN=dev`
- `RF_HA_ENABLED=1`
- `RF_HA_SYNC_MODE=1`
- `RF_HA_WRITE_CONCERN=4` in the 6-voter demo preset
- `AYDER_JEPSEN_TOKEN`
- `AYDER_JEPSEN_PROFILE=hn-ready`

## Recovery behavior

The recovery script restarts unhealthy nodes and can fall back to a full restart. It waits for both health and a leader with fresh quorum before declaring readiness.

## Artifact locations

Look for evidence in:

- `tests/jepsen/results/` for raw Jepsen runs and matrices
- `tests/jepsen/artifacts/` for published bundles and hashes
- `artifacts/chaos_*` for chaos-suite outputs and captures

## Caution

Do not generalize a single successful run into a broad reliability statement. Use the exact run date, matrix directory, and bundle hash from [[Jepsen-Evidence]].

See also:

- [[Architecture]]
- [[Jepsen-Evidence]]
- [[FAQ]]