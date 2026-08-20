# Ayder benchmark harness

This directory runs fixed-rate client-side latency tests and records enough
metadata to compare the same workload across ARM64 WSL2, native Linux ARM64,
AWS Graviton, Azure ARM64, and x86-64 Linux. It does not start or configure the
broker for you; record the exact server command and topology.

## Dependencies

Required: Bash, Python 3, curl, jq, sha256sum, taskset when CPU pinning is used,
and a wrk2-compatible fixed-rate client. `build_wrk2_arm64.sh` applies the
checked-in ARM64 compatibility patch. Some raw wrk2 builds also require a Lua
module search path; set `WRK2_LUA_PATH` and it will be recorded.

## Durable matrix

Start Ayder in a clean data directory, wait for `/ready`, then run:

```bash
export WRK2_BIN=/path/to/wrk2
export BASE_URL=http://127.0.0.1:1109
export TOKEN="$AYDER_BENCH_TOKEN"
export PLATFORM_LABEL='<architecture and environment>'
export CLIENT_CPUSET='<client CPUs>'
export SERVER_CPUSET='<server CPUs>'
export SERVER_PIDS='<parent and worker PIDs>'
export SERVER_COMMAND='<exact redacted server command>'
export SERVER_BINARY_SHA256="$(sha256sum ./ayder | awk '{print $1}')"
export SERVER_TOPOLOGY='<nodes, workers, CPU and client placement>'
export PAYLOADS='128 1024 16384'
export RATES='10000 25000 50000'
export WARMUP_SECONDS=10 DURATION_SECONDS=20 TRIALS=3
./bench/run_matrix.sh
```

The durable Lua workload accepts only responses containing `ok=true`,
`durable=true`, and `synced=true`. `run_fixed_rate.sh` also rejects false
readiness and `parse_wrk.py` makes socket, timeout, response, or validation
errors fail the case.

## Endpoint and crash checks

```bash
SUITE_RATE=1000 SUITE_OUTPUT_DIR=bench/results/endpoints \
  ./bench/run_endpoint_suite.sh

SERVER_BIN="$PWD/ayder" RECORDS=100 OUTPUT_DIR=bench/results/crash \
  ./bench/crash_recovery_check.sh
```

The endpoint suite separates HTTP overhead, default sealed produce (without an
explicit timeout), explicit-timeout sealed produce, consume, and query
inspection. Both produce cases require explicit local-fsync/all-worker receipts;
omitting `timeout_ms` does not select an asynchronous path. The query latency
case uses POST because the available wrk2 Lua adapter could not emit the custom
`QUERY` method reliably; route smoke covers the actual method. The crash check
is deliberately scoped to one node and one worker.

## Output and publication rules

Each fixed-rate case contains `metadata.json`, `environment.txt`, raw warmup and
trial text, per-second system TSV files, `trials.json`, `trials.csv`, and
`summary.json`. A matrix adds aggregate JSON and CSV.

Do not publish a result unless the tree/binary/tool identities are recorded,
all trials are clean, the exact durability and fsync contract is stated, and an
appropriate acknowledged-write recovery test passes. WSL2 results must always
be labeled development/regression evidence.

See [`docs/BENCHMARKS.md`](../docs/BENCHMARKS.md) for the measured Revenue
Readiness v1 results and limitations.
