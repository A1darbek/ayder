# Architecture

Ayder is built around a few practical constraints:

- HTTP is the only client surface
- one binary is expected to run the service
- durability is backed by Raft in HA mode
- operational behavior should stay understandable under node failure

## Main pieces

- Broker APIs for append, consume, and offset commit
- KV APIs for a single-key linearizable register workload
- HA mode with a 3/5/7 node Raft cluster shape
- Storage and recovery paths intended to survive crash/restart cycles

## HA model

The repo docs describe these write-concern modes:

- `RF_HA_WRITE_CONCERN=1` for leader-local ack
- `RF_HA_WRITE_CONCERN=majority` for the recommended durability path
- `RF_HA_WRITE_CONCERN=all` for the strongest ack rule

For strict claim runs, the documented path also uses:

- `RF_HA_SYNC_MODE=1`
- majority write concern
- mixed fault modes such as partition-only, kill-only, and mixed

## Local cluster layout

The 6-node demo cluster uses these HTTP ports:

- node1 `7001`
- node2 `8001`
- node3 `9001`
- node4 `10001`
- node5 `11001`
- node6 `12001`

The preset lives in `tests/demo/ha6_stability_env_preset.sh`, and the recovery flow is in `tests/demo/ha6_recover_local_cluster.sh`.

## Claim discipline

Use strict wording when describing results. Architecture supports a claim path, but the claim itself comes from a specific Jepsen matrix and bundle, not from the design alone.

See also:

- [[Operations]]
- [[Jepsen-Evidence]]
- [[FAQ]]