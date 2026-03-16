# Demo and Runbook Scripts

This folder contains practical wrappers for local HA recovery checks, Jepsen campaigns, and strict-claim execution.

## Most Important Scripts

- `ha_broker_jepsen_gold.sh`: strict claim matrix wrapper for broker Jepsen runs.
- `ha6_recover_local_cluster.sh`: recover or restart local 6-node HA cluster.
- `ha6_stability_env_preset.sh`: per-node env preset (ports, HA, TLS, quorum knobs).
- `ha_cp_partition_assert.sh`: CP partition behavior assertion helper.
- `ha_kill9_amnesia_local.sh`: kill/restart durability behavior check.
- `ha_membership_dynamic.sh`: dynamic membership helper flow.

## Recommended Run Flow

1. Recover cluster baseline:

```bash
bash ./tests/demo/ha6_recover_local_cluster.sh
```

2. Run short 3-mode gate before long run:

```bash
sudo -v
sudo --preserve-env=STRICT_CLAIM,START_CLUSTER,TOKEN,AYDER_JEPSEN_WORKLOAD,AYDER_JEPSEN_PROFILE,AYDER_JEPSEN_MODES,AYDER_JEPSEN_DURATIONS,AYDER_JEPSEN_RUNS_PER_CELL,AYDER_JEPSEN_NEMESIS_STARTUP_SEC,AYDER_JEPSEN_MIXED_NEMESIS_STARTUP_SEC,AYDER_JEPSEN_PRE_READY_TIMEOUT_SEC \
env STRICT_CLAIM=1 START_CLUSTER=1 TOKEN=dev \
AYDER_JEPSEN_WORKLOAD=broker-log \
AYDER_JEPSEN_PROFILE=hn-ready \
AYDER_JEPSEN_MODES="mixed partition-only kill-only" \
AYDER_JEPSEN_DURATIONS="120 300 600" \
AYDER_JEPSEN_RUNS_PER_CELL=1 \
AYDER_JEPSEN_NEMESIS_STARTUP_SEC=10 \
AYDER_JEPSEN_MIXED_NEMESIS_STARTUP_SEC=15 \
AYDER_JEPSEN_PRE_READY_TIMEOUT_SEC=180 \
bash ./tests/demo/ha_broker_jepsen_gold.sh
```

3. Run full strict matrix:

```bash
sudo -v
sudo --preserve-env=STRICT_CLAIM,START_CLUSTER,TOKEN,AYDER_JEPSEN_WORKLOAD,AYDER_JEPSEN_PROFILE,AYDER_JEPSEN_NEMESIS_STARTUP_SEC,AYDER_JEPSEN_MIXED_NEMESIS_STARTUP_SEC,AYDER_JEPSEN_PRE_READY_TIMEOUT_SEC \
env STRICT_CLAIM=1 START_CLUSTER=1 TOKEN=dev \
AYDER_JEPSEN_WORKLOAD=broker-log \
AYDER_JEPSEN_PROFILE=hn-ready \
AYDER_JEPSEN_NEMESIS_STARTUP_SEC=10 \
AYDER_JEPSEN_MIXED_NEMESIS_STARTUP_SEC=15 \
AYDER_JEPSEN_PRE_READY_TIMEOUT_SEC=180 \
bash ./tests/demo/ha_broker_jepsen_gold.sh
```

## Troubleshooting

- `pre-run heal failed rc=1`: local chaos needs `sudo -n`; refresh credentials with `sudo -v`.
- Long `run_heartbeat ... elapsed_sec=...`: run still active; check `run_*/runner.log` and `exit_code.txt` before stopping.
- Frequent `pre-run readiness pending healthy=...`: recover cluster first, then rerun gate.

## Output Paths

- Matrix runs: `tests/jepsen/results/gold_YYYYMMDDTHHMMSSZ`
- Bundles: `tests/jepsen/artifacts/gold_<run_id>.tar.gz` + `.sha256`