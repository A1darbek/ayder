# Ayder Wire decision

Decision date: 2026-08-15; reconciled after the visibility fix on 2026-08-16.
Evidence platform: **ARM64 WSL2 development benchmark**. Decision: **do not
build a custom TCP/binary protocol in Revenue Readiness v1. Keep HTTP
first-class.**

## Evidence

At 1,000 offered RPS with 128 connections, the HTTP health path achieved 998.73
RPS at 1.923 ms p99. An accepted, non-waiting produce achieved 998.72 RPS at
1.932 ms p99. The same broker's locally fsync-confirmed produce achieved only
458.35 RPS at 7.074 seconds p99 and recorded timeouts. At 10,000 offered RPS,
the durable 128 B matrix case achieved 425.34 RPS at 19.022 seconds p99.

The protocol/network and route baseline is therefore roughly 2 ms at the tested
1k rate, while durable completion added seconds of queueing. Subtracting the two
is not a rigorous component percentage because the durable test is saturated,
but it is enough to reject HTTP as the first-order cause of this result.

That profile preceded the cross-worker publication fix. The strict fixed-code
rerun, which accepted only explicit local-fsync/all-worker receipts, still
achieved just 308–382 RPS with roughly 19–20 second p99 and timeouts. It therefore
supports the same prioritization without treating the superseded receipt
validator as final evidence.

## CPU attribution

The ten-second `perf` capture contained 619 user-cycle samples and no lost
samples. WSL2 symbolization was incomplete: 25.87% of samples appeared as an
unknown kernel address. Named symbols included:

| Symbol or stack | User-cycle samples |
|---|---:|
| `wait_synced_until` | 8.07% |
| `process_request` (parsing, routing, and handler included) | 3.96% |
| `sealed_sync_thread` | 2.81% |
| `read_cb` | 1.20% |
| `AOF_append_sealed` | 0.62% |

There is no defensible exact “HTTP parser percentage” in this capture: parser
work was inlined or folded into broader symbols, and the unknown-kernel bucket
is large. The best bounded statement is that the named whole
`process_request` symbol—including far more than parsing—was 3.96%, while sync
waiting, sleep/clock polling, promoter work, sealed locking, and persistence
dominated the readable call chains.

`pidstat` showed data workers around 20–21% CPU each, mostly system time, with
roughly 3.5–4.2% I/O wait. Steady `iostat` samples showed the WSL2 virtual disk
around 1,574–1,811 writes/s, 792–867 flushes/s, and 83–90% utilization. These
are development signals, not native-storage limits.

## Does HTTP materially limit saturation?

Not in the measured durable workload. HTTP and accepted-produce paths were
clean at the tested 1k rate, while local-fsync produce saturated below 500 RPS.
The measured bottleneck is synchronous event-loop polling for a sealed
completion combined with persistence/flush and cross-worker coordination.

This sprint did not run the HTTP-only case upward to its saturation point, so it
does not establish a universal maximum HTTP throughput. That missing number is
not required to decide the current priority: the durable path is two orders of
magnitude below the offered target. Focused multiworker visibility now passes;
the durable completion path remains the measured bottleneck.

## Decision

An optional binary PRODUCE/FETCH/COMMIT protocol is **not justified now**. It
would add a second untrusted parser, client libraries, version negotiation,
authentication/TLS integration, observability, fuzzing, compatibility, and
support surface without addressing the measured fsync-completion or
multiworker-consistency failures.

Priorities are:

1. retain the new cross-worker publication/apply regression gates and extend
   them under longer fault/load campaigns;
2. replace event-loop sync polling/per-request flush serialization with a
   reviewed completion and batching design that preserves acknowledgement
   semantics;
3. close Raft durability/safety and idempotency state-machine gaps;
4. rerun the same client-side matrix on native target-like storage; and
5. only then profile HTTP-only saturation and parsing/copy/allocation cost.

Revisit Ayder Wire only if a corrected durable broker reaches a CPU-bound HTTP
ceiling, HTTP parsing/copying is a substantial measured share, and a prototype
shows a material end-to-end benefit without weakening security or durability.

Evidence: [benchmark report](BENCHMARKS.md), [strict fixed-code summary](evidence/benchmarks/rrv1-post-visibility-commitfix-wsl2.json),
the [superseded profile-era summary](evidence/benchmarks/rrv1-post-fix-wsl2.json),
and locally retained raw profile files under `bench/results/rrv1-profile-final/`.
