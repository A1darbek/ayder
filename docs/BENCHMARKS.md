# Benchmarks

Ayder benchmark results are engineering evidence, not an SLA. The current run
is an **ARM64 WSL2 development benchmark** on ext4 (`/dev/sdd`); it is not
native production Linux or cloud-storage evidence.

## Result

The required 50,000 RPS / p99 at or below 3 ms goal was not met. After fixing
cross-worker visibility and requiring each response to prove local fsync plus
application to every local worker, median throughput was 308–382 RPS and p99
was 19.1–19.7 seconds at the requested offered rates. Every case had client
timeouts and is therefore non-clean.

Machine-readable evidence is
[rrv1-post-visibility-commitfix-wsl2.json](evidence/benchmarks/rrv1-post-visibility-commitfix-wsl2.json).
Raw trial text/JSON/CSV/system samples are retained locally under the ignored
`bench/results/rrv1-post-visibility-commitfix-wsl2-20260816/` directory.

## Standard wrk closed-loop check

At the user's request, wrk2 was not resumed. A separate standard `wrk` check
used one thread and one connection to measure the durable path without building
an offered-rate queue. Across three clean 20-second trials, median throughput
was 347.16 RPS and median p99 was 5.522 ms. Per-trial p99 was 4.812, 15.755 and
5.522 ms, so the honest result is a 5.522 ms median with one latency excursion,
not a universal 3–9 ms bound. All 20,828 responses passed the strict receipt
contract and there were no HTTP, socket, timeout or validation errors.

This is a closed-loop, coordinated-omission-uncorrected latency check. It does
not replace the fixed-rate matrix, demonstrate capacity at 50,000 RPS, or
overturn the RED performance gate. An exploratory connection sweep plateaued
near 360–370 RPS; p99 rose from 3.626 ms at one connection to 23.706 ms at four,
133.069 ms at 16, 1.610 seconds at 64, and 3.825 seconds with timeouts at 128.
That curve is consistent with queueing behind the durable completion path.

Machine-readable evidence is
[rrv1-wrk-durable-c1-ca9e210.json](evidence/benchmarks/rrv1-wrk-durable-c1-ca9e210.json).
It records an exact clean Git archive, binary and wrk hashes, CPU placement,
receipt contract, all trials, and the method boundary. Raw local output is under
the ignored `bench/results/rrv1-wrk-durable-c1-ca9e210-20260820T164250Z/`.

## Exact response contract

The workload accepts a response only when all of these are true:

```text
HTTP 2xx
ok=true
durability_scope="local"
local_fsync=true
applied_all_workers=true
quorum_committed=false
failover_safe=false
durable=true and synced=true (legacy local aliases)
```

This is a single-node local durability measurement. It deliberately does not
claim quorum or failover durability. The server used a 10 ms group-commit
interval and did not respond until the request's local fsync and all-worker
application barriers completed.

## Method

- Qualcomm Oryon, 8 aarch64 logical CPUs, 23 GiB RAM;
- WSL2 kernel `6.6.87.2-microsoft-standard-WSL2`;
- ext4 on `/dev/sdd`, GCC 15.2.0;
- one node, four forked workers, `RF_HA_DEDICATED_WORKER=0`;
- server CPUs 0–3; wrk2 client CPUs 4–7;
- bearer authentication enabled and redacted; rate limiting disabled;
- four wrk2 threads, 128 connections, coordinated-omission correction;
- 10-second warmup; three 20-second trials per case;
- payloads 128 B, 1 KiB, and 16 KiB;
- fixed offers of 10k, 25k, and 50k RPS;
- server binary SHA-256
  `de14a5e1f46785785434d059dde9c4fc3b1b152eeafe36dd101151103c298008`;
- wrk2 SHA-256
  `b868e52fbd673e1ec0a2d06248ee2c957e518c05933e3109def13e081eb335a0`.

The run was made from a dirty tree. Metadata records HEAD, tracked-patch and
untracked-manifest hashes, tool and binary hashes, redacted environment,
commands, CPU placement, raw trials, and population variance. This is
reproducible development evidence, not immutable release evidence.

## Matrix medians

| Payload | Offered | Achieved RPS | p50 | p95 | p99 | p99.9 | Clean |
|---:|---:|---:|---:|---:|---:|---:|:---:|
| 128 B | 10k | 380.26 ± 0.41 | 14.442 s | 18.694 s | 19.104 s | 19.317 s | No |
| 128 B | 25k | 381.96 ± 1.41 | 14.762 s | 19.137 s | 19.530 s | 19.661 s | No |
| 128 B | 50k | 380.47 ± 1.54 | 14.901 s | 19.284 s | 19.677 s | 19.775 s | No |
| 1 KiB | 10k | 369.01 ± 1.80 | 14.442 s | 18.711 s | 19.120 s | 19.333 s | No |
| 1 KiB | 25k | 359.67 ± 1.63 | 14.803 s | 19.153 s | 19.546 s | 19.644 s | No |
| 1 KiB | 50k | 352.39 ± 2.42 | 14.909 s | 19.300 s | 19.694 s | 19.775 s | No |
| 16 KiB | 10k | 308.76 ± 0.78 | 14.541 s | 18.825 s | 19.235 s | 19.481 s | No |
| 16 KiB | 25k | 309.37 ± 0.57 | 14.803 s | 19.186 s | 19.579 s | 19.694 s | No |
| 16 KiB | 50k | 307.53 ± 4.54 | 14.926 s | 19.300 s | 19.694 s | 19.792 s | No |

Across all measured trials, 188,877 returned successes passed the explicit
receipt validation; no received response had an invalid receipt and no 4xx/5xx
was observed. There were 3,432 client timeouts. Successful receipts do not make
an overloaded fixed-rate trial clean.

## Before/after boundary

An older 128 B run reached 9,963 RPS at a 10k offer with p99 2.781 ms, but it
predated fixes to watermark publication, partial writes, transactional
mutation behavior, and cross-worker application. Its durability claim could be
false, so it cannot be used as a speed baseline for the current contract.

The current slowdown is real and accepted in favor of correctness. Prior
profiling points to synchronous completion polling, sealed locking, and
persistence/flush coordination rather than HTTP parsing. No optimization that
weakens fsync, worker visibility, or quorum semantics is acceptable.

## Crash/restart

The fixed binary wrote 100 records with the same local-only receipt semantics,
was killed with SIGKILL, restarted, and recovered 100/100 with zero lost and
zero duplicate IDs. Fault-to-ready was 74 ms on this ext4 WSL2 run, against the
exercise's explicit 0-record/5-second pilot target. This is not power-loss or
HA-failover proof.

## Reproduce

```bash
export WRK2_BIN=/path/to/wrk2
export WRK2_LUA_PATH='/path/to/wrk2/src/?.lua;;'
export BASE_URL=http://127.0.0.1:1109
export TOKEN="$AYDER_BENCH_TOKEN"
export PLATFORM_LABEL='ARM64 WSL2 development benchmark'
export CLIENT_CPUSET=4-7 SERVER_CPUSET=0-3
export SERVER_PIDS='<parent and worker PIDs>'
export SERVER_COMMAND='<exact command with secrets redacted>'
export SERVER_BINARY_SHA256="$(sha256sum ./ayder | awk '{print $1}')"
export PAYLOADS='128 1024 16384' RATES='10000 25000 50000'
export WARMUP_SECONDS=10 DURATION_SECONDS=20 TRIALS=3
bash bench/run_matrix.sh
```

The harness is architecture-independent. The identical suite still must run
on native target-like ARM64 and amd64 storage. This WSL2 host cannot produce
that evidence, and emulation would not satisfy the requirement.
