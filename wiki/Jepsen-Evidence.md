# Jepsen Evidence

This is the page to cite when you need the correctness story.

## Strict claim wording

Current public claim in the repo README:

- strictly linearizable under mixed faults: `45/45` pass in the latest full `hn-ready` matrix

Verified run date in the repo docs:

- `2026-03-13`

Matrix evidence:

- `tests/jepsen/results/gold_20260313T103615Z`
- `tests/jepsen/results/gold_20260313T103615Z/cells.csv`

Claim rule:

- treat the statement above as an exact-run claim
- do not reuse it for other dates, commits, or matrix settings unless you reproduce the same evidence

## How the gold run is produced

The wrapper script is `tests/demo/ha_broker_jepsen_gold.sh`.

It drives:

- `AYDER_JEPSEN_WORKLOAD=broker-log`
- `AYDER_JEPSEN_PROFILE=hn-ready`
- mixed, partition-only, and kill-only modes
- 120s, 300s, and 600s durations

The repo README shows the strict command line and the quick validation check for `cells.csv`.

## Where artifacts live

- Raw Jepsen runs: `tests/jepsen/results/`
- Published bundles: `tests/jepsen/artifacts/gold_<run_id>.tar.gz`
- Bundle hashes: `tests/jepsen/artifacts/gold_<run_id>.tar.gz.sha256`
- Bundle manifests: `tests/jepsen/artifacts/gold_<run_id>.tar.gz.manifest`

## What to look for

- all cells with `exit_code=0`
- a non-vacuous checker result
- the exact command line used for the run
- the bundle hash that matches the published archive

## Caution

The evidence supports the repo claim only as written. If your matrix, fault mix, duration, or commit differs, say so explicitly.

See also:

- [[Architecture]]
- [[Operations]]
- [[FAQ]]