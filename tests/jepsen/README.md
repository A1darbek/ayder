# Ayder Jepsen Suite

This directory contains a real Jepsen project (Jepsen + Knossos) for Ayder.

## Workloads

- `broker-log` (default): append-only event log semantics over broker APIs
- `kv-register`: single-key linearizable register over KV APIs

For public CP claims, use `broker-log`.

## Broker Workload Details

Operations:

- `:append` -> `POST /broker/topics/{topic}/produce?partition={p}`
- `:read-at` -> `GET /broker/consume/{topic}/{group}/{p}?limit=1&offset={o}&encoding=b64`

Model:

- Custom append-only log model (`next-offset` + `offset->value` map)
- `append` must return the exact next offset
- `read-at(o)` must return:
  - `nil` only if `o` has not been appended yet
  - otherwise exactly the value appended at `o`

Checker:

- `checker/linearizable` on client processes only
- `min-ok` guard to reject vacuous runs with zero successful operations
- timeline HTML report

Nemesis:

- shell-command driven (`partition`, `heal`, `kill`, `restart`)

## Prerequisites

- Java 11+
- Leiningen (`lein`)
- Ayder cluster already running (HTTP endpoints)
- `curl`, `jq`, `tar`
- `sha256sum` (or `shasum`)
- For local real partitions (no Docker): `iptables` with `owner --pid-owner` or `cgroup --path` and passwordless `sudo -n`
- WSL2 strict mixed runs: recommend at least `20 GiB` RAM and `8 GiB` swap for stable Jepsen checker memory usage

This suite runs in local Jepsen control mode (no SSH requirement), so `AYDER_JEPSEN_NODES` should be API URLs like `http://127.0.0.1:7001`.

Quick check:

```bash
./tests/jepsen/bin/check-prereqs.sh
```

No-sudo local bootstrap (installs JDK + Lein launcher under `tests/jepsen/.toolchain`):

```bash
./tests/jepsen/bin/bootstrap-toolchain.sh
```

All Jepsen run scripts auto-source `tests/jepsen/.toolchain/env.sh` if present.

## Single Run

```bash
./tests/jepsen/bin/run-local.sh
```

`run-local.sh` defaults to Docker chaos (`scripts/chaos-ha.sh`) when `ayder-node1` container is present.
If Docker chaos is unavailable, it automatically falls back to local iptables chaos (`scripts/chaos-ha-local.sh`).

Important env vars:

- `AYDER_JEPSEN_WORKLOAD=broker-log|kv-register`
- `AYDER_JEPSEN_NODES` comma-separated URLs
- `AYDER_JEPSEN_TOKEN`
- `AYDER_JEPSEN_TOPIC`
- `AYDER_JEPSEN_GROUP`
- `AYDER_JEPSEN_PARTITION`
- `AYDER_JEPSEN_TOPIC_PARTITIONS`
- `AYDER_JEPSEN_DIRECT_URL` (default empty; when set, force all ops through one endpoint)
- `AYDER_JEPSEN_TIME`
- `AYDER_JEPSEN_NEMESIS=true|false`
- `AYDER_JEPSEN_NEMESIS_INTERVAL`
- `AYDER_JEPSEN_PARTITION_CMD`
- `AYDER_JEPSEN_HEAL_CMD`
- `AYDER_JEPSEN_KILL_CMD`
- `AYDER_JEPSEN_RESTART_CMD`
- `AYDER_JEPSEN_ARTIFACT_DIR`

## Campaign

Run many independent runs:

```bash
AYDER_JEPSEN_WORKLOAD=broker-log AYDER_JEPSEN_RUNS=30 AYDER_JEPSEN_TIME=300 ./tests/jepsen/bin/campaign.sh
```

Campaign script auto-rotates topic/group per run for broker-log to avoid cross-run state contamination.

## Matrix Campaign (Recommended)

Run fault-mix x duration matrix:

```bash
AYDER_JEPSEN_WORKLOAD=broker-log ./tests/jepsen/bin/campaign-matrix.sh
```

For credible partition claims, keep `STRICT_CLAIM=1` in the gold wrapper and do not override
`AYDER_JEPSEN_PARTITION_CMD`/`AYDER_JEPSEN_HEAL_CMD` with no-op commands.

Controls:

- `AYDER_JEPSEN_RUNS_PER_CELL` (default `10`)
- `AYDER_JEPSEN_DURATIONS` (default `"120 300 600"`)
- `AYDER_JEPSEN_MODES` (default `"partition-only kill-only mixed"`)

## Artifact Layout

By default artifacts go under:

- `tests/jepsen/results/`

Single run directory includes:

- `manifest.env`
- `command.sh`
- `runner.log`
- `exit_code.txt`
- `summary.json`
- `result.edn`
- `opts.edn`
- `argv.txt`

Campaign directory includes:

- `manifest.env`
- `summary.csv`
- `run_1/`, `run_2/`, ...

Matrix directory includes:

- `manifest.env`
- `cells.csv`
- one campaign directory per mode/duration cell

## Bundle For Publishing

Bundle latest results (or a specific directory) into tar.gz + sha256:

```bash
./tests/jepsen/bin/bundle-artifacts.sh
# or
./tests/jepsen/bin/bundle-artifacts.sh tests/jepsen/results/campaign_... tests/jepsen/artifacts/my_bundle.tar.gz
```

Outputs:

- `*.tar.gz`
- `*.tar.gz.sha256`
- `*.tar.gz.manifest`

## Public "Gold Standard" Claim Criteria

1. Run at least 30 independent Jepsen runs.
2. Run multiple fault mixes (partition-only, kill-only, combined).
3. Run multiple durations (2m, 5m, 10m).
4. Require zero linearizability violations.
5. Require `min-ok` checker pass (non-vacuous run).
6. Publish raw Jepsen result directories + timelines + bundle hash.
7. Publish exact Ayder commit SHA and full Jepsen command lines.

## Nemesis Evidence

Each run `summary.json` now includes:

- `nemesis-ok` (must be `true`)
- `nemesis.error-count` (must be `0`)
- `nemesis.by-f` counters (used by gold validation to assert partition activity)

If nemesis shell commands fail (for example missing `sudo -n` for local chaos), the run exits as `FAIL`.