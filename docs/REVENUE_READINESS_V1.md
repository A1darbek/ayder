# Ayder Revenue Readiness v1

Evidence date: 2026-08-16. Development platform: aarch64 WSL2 on ext4.
Measured fixed binary SHA-256:
`de14a5e1f46785785434d059dde9c4fc3b1b152eeafe36dd101151103c298008`.

This sprint removed several correctness and security blockers and produced
reproducible evidence. It did not establish production readiness. The measured
tree was dirty, the mandatory full post-change 45-run Jepsen campaign has not
run, and native target-storage benchmarks are unavailable.

**Overall: RED. READY TO MERGE: NO.**

## Release gate

| Category | Gate | Evidence and boundary |
|---|---|---|
| Correctness | YELLOW | Cross-worker and idempotency regressions pass; short kill/partition/mixed Jepsen passes with exact post-run convergence; full 45-run campaign pending. |
| Durability | RED | Partial-write/tail/fatal-state tests and a 100/100 SIGKILL recovery pass; power-loss and full HA acknowledged-ledger evidence are incomplete. |
| Performance | RED | Strict local-fsync/all-worker matrix achieved 308–382 RPS with 19–20 s p99 and timeouts; target missed. |
| Security | YELLOW | HTTP framing, auth/metrics/CORS/deadline/resource controls and HA identity have focused passing evidence; no independent penetration test or client-side TLS listener. |
| Fuzzing | YELLOW | 855 deterministic ASan/UBSan HTTP-framing executions pass; no long campaign or AOF/HA decoder fuzzer. |
| HA | RED | Durable state/protocol checks are implemented and short faults converge, but a Raft safety claim is withheld until the complete campaign and stronger crash/power tests pass. |
| Observability | GREEN for evaluation | Prometheus config and 14 source-backed rules validate; dashboard JSON validates. Thresholds still require pilot tuning. |
| Operations | YELLOW | Cold backup/restore, distinct-binary upgrade/rollback, bounded shutdown, and a scoped RPO/RTO drill pass; online backup/GC and rolling upgrade are unsupported. |
| Documentation | GREEN for evaluation | Architecture, threat model, benchmarks, observability, runbook, API, enterprise matrix, and buyer index exist and state limitations. |
| Supply chain | RED | Local Trivy/Gitleaks/Syft triage passes its defined gates and Actions are SHA-pinned; clean-commit CodeQL/CI and issued Sigstore provenance are pending. |
| Enterprise technical evaluation | YELLOW | Evidence is sufficient to inspect the system; paid real-data production approval remains blocked. |

## Correctness changes

### Cross-worker state

Topic creation now uses a process-shared pending/ready publication protocol.
Broker records are published through a shared worker bus and acknowledgements
wait for every local worker to apply them. A failed worker application returns
a failed response; replay repairs the local view after restart. Regression
evidence:

- 400/400 load-balanced responses observed the produced record;
- an injected worker-apply failure did not acknowledge success;
- restart replay made the record visible to every worker.

### Persistence and recovery

- zero flush interval now means append-fsync-always, not disabled persistence;
- full `writev` and rewrite helpers retry `EINTR`, account for partial progress,
  and reject zero-progress writes;
- replay truncates only an incomplete tail, fails closed on CRC/magic/semantic
  corruption, and prevents readiness on unrecoverable history;
- a shared fatal state stops sibling writers and waiters;
- multiworker online sealed GC is explicitly unavailable until a safe shared
  drain/lock/rename/reopen design exists;
- commit offsets use checked atomic monotonic updates.

### Raft state and protocol

A dedicated durable store now recovers current term, voted-for, commit/applied
indices, and log records. State/log records have bounds and CRC validation;
state and appended log durability use `fdatasync`; partial tails are repaired
and invalid durable history fails closed.

Protocol validation now covers stale terms, previous-log index/term matching,
candidate-log recency, per-term vote-response deduplication, and connection-
bound mTLS node identity. Followers advance commit only through the index
matched by the current AppendEntries request. Leader replication no longer
rewinds `next_index` to `match_index + 1` after each successful election.

These changes address the defects found in review. They are not, by
themselves, a formal or campaign-complete Raft safety proof.

### Response semantics

Mutation responses separate local and failover properties:

- `local_fsync`: the local persistence barrier completed;
- `applied_all_workers`: every worker in this node applied the record;
- `quorum_committed`: a voting majority durably stored/committed it;
- `failover_safe`: the checked failover property for this response;
- `durability_scope`: `local` or `quorum`.

Local single-node responses must not claim quorum/failover. HA clients and
Jepsen count a success only when every quorum field is explicitly true.

### Atomic idempotency and mutation failure

Idempotency state is serialized through process-shared striped locks and uses
payload-bound SHA-256 digests with durable `PENDING`/`COMMITTED` state. Reusing
a key for another payload returns `idempotency_conflict`. An uncertain final
state is resolved from the durable record rather than blindly appending again.
Append, HA, worker-application, and final-state failures are checked; failed
KV/broker transactions do not publish an acknowledged mutation.

The regression covers concurrent same-key retries, payload conflict, injected
COMMITTED-store failure, restart resolution, and duplicate absence.

## Security fixes

- reject Transfer-Encoding plus Content-Length in either order;
- strictly parse duplicate/malformed/overflowing Content-Length;
- require exact supported Transfer-Encoding and bounded chunk framing;
- handle fragmented chunks, extensions/trailers, reallocation, leftovers, and
  size/count limits;
- enforce header/body/idle/absolute timeouts and connection caps;
- protect metrics/admin routes with the configured auth policy;
- use constant-time bearer-token comparison and redact benchmark environment
  secrets;
- bind every HA frame's claimed node to the authenticated peer certificate;
- remove public development private keys from the tracked tree and replace
  them with ignored local fixtures.

Focused evidence: 237 pure framing checks, 27 hardened raw-socket cases, 31
live route/security smoke checks on normal/hardened/sanitized builds, GCC
analyzer passes, and clean tested ASan/UBSan paths.

## Tests and analysis run

| Gate | Result |
|---|---|
| CMake Release, GCC 15.2, warnings as errors | PASS; no C diagnostic |
| P0 durability/HA suite | PASS, including 24 protocol, 19 fault-injection, 47 durable-store checks and deterministic rewrite partial/EINTR/zero-progress writes |
| Cross-worker visibility | PASS, 400/400 plus apply-failure/restart case |
| Atomic idempotency | PASS |
| Cold backup/restore regression | PASS |
| HTTP framing unit | PASS, 237 checks |
| Hardened raw HTTP | PASS, 27/27 |
| ASan/UBSan framing build | PASS |
| Fuzz smoke | PASS, 855 executions |
| GCC analyzer | PASS for framing, HTTP, storage, AOF and tests |
| Route/security smoke | PASS, 31 checks on normal/hardened/sanitized builds; SIGTERM bounded |
| Jepsen model unit | PASS, 3 tests / 5 assertions |
| Prometheus/actionlint/dashboard/shell syntax | PASS; 14 rules |
| Trivy HIGH/CRITICAL gate | PASS; zero vulnerabilities, zero HIGH/CRITICAL misconfigurations |
| Gitleaks complete history after exact triage | PASS; zero unsuppressed findings |

## Short Jepsen evidence

The fixed binary ran one short test in each requested mode on a six-node local
cluster:

| Mode | Duration | Result | Nemesis evidence | Post-run convergence |
|---|---:|---|---|---|
| partition | 18 s | PASS / linearizable / nemesis complete | 2 applied partitions, 3 heals, 0 errors | all nodes `383:383:383`; one fresh-quorum leader |
| kill | 18 s | PASS / linearizable / nemesis complete | 2 kills, 3 restarts, 0 errors | all nodes `765:765:765`; one fresh-quorum leader |
| mixed | 26 s | PASS / linearizable / nemesis complete | 2 partitions, 3 heals, 1 kill, 2 restarts, 0 errors | all nodes `1162:1162:1162`; one fresh-quorum leader |

All six final durable HA log files were byte-identical, SHA-256
`71b1800a266fc6f5f921c54f8c3442eff7ca86a6bc1e93e04fe90d1d5ec8459d`.
The wrapper now fails if any node is unhealthy/failed, there is not exactly one
fresh-quorum leader, or last/commit/applied tuples differ.

This is short regression evidence, not the required final 45/45 campaign. Core
HA/durability semantics changed, so merge readiness remains blocked until that
identical complete campaign passes.

## Performance and crash evidence

See [BENCHMARKS.md](BENCHMARKS.md). The strict 9-case matrix missed the target:
308–382 RPS median and seconds of latency. All 188,877 returned successes had
valid explicit local-only receipts, but 3,432 client timeouts made every case
non-clean.

A separate standard-wrk closed-loop check was run instead of resuming wrk2. At
one connection it produced 347.16 median RPS and 5.522 ms median p99 across
three clean trials; per-trial p99 ranged from 4.812 to 15.755 ms. Because wrk is
closed-loop and does not correct coordinated omission, this is useful
low-concurrency latency evidence but does not replace the fixed-rate matrix or
change the RED performance gate.

The fixed single-node crash drill recovered 100/100 locally fsynced/all-worker
acknowledgements after SIGKILL, with zero loss/duplicates and 74 ms
fault-to-ready on ext4 WSL2. It met the exercise-specific target of zero records
and five seconds. It is not a universal RPO/RTO or power-loss/HA statement.

Native target-like ARM64 and amd64 storage was not available. The portable
harness is ready, but that evidence remains pending.

## Operations evidence

- Cold backup refuses a live data directory, captures a manifest, archive and
  per-file checksums, checks for changes during capture, and fsyncs outputs.
- Restore verifies manifest/archive/file checksums, rejects unsafe archive
  entries, restores through staging, preserves modes, and fsyncs files/parent.
- A distinct-binary drill upgraded from clean HEAD binary SHA
  `c21518cc0994ece2b2d750081e7a5c8aa9cf70918733029786e8feba1a2dfdc0`
  to the fixed binary and rolled back by restoring the pre-upgrade cold backup.
  It passed: upgrade ready 67 ms, backup 255 ms, restore 37 ms, rollback ready
  66 ms. Pre-upgrade data survived both paths; post-upgrade data was absent
  after rollback as designed.

No online snapshot, in-place downgrade, rolling mixed-version HA guarantee,
online GC, cross-region DR, or default SLA/RPO/RTO is claimed.

## Supply-chain evidence

Official ARM64 Trivy 0.74.0, Gitleaks 8.30.1, and Syft 1.51.0 release archives
matched their published checksums before execution. Scanner triage is in
[SCANNER_TRIAGE.md](evidence/supply-chain/SCANNER_TRIAGE.md).

The initial scan found the stale benchmark `aiohttp==3.9.5` dependency and root
runtime users; they were fixed. Final local results: zero vulnerabilities,
zero HIGH/CRITICAL misconfigurations, one MEDIUM Dockerfile style finding, and
two LOW missing-healthcheck findings on short-lived client images. Eight
historical public test keys are suppressed only by exact immutable Gitleaks
fingerprint after removal; new keys remain detectable.

SHA-pinned workflows cover ARM64/amd64 build, sanitizers, CodeQL, scanners,
SBOM and GitHub OIDC/Sigstore provenance. Definitions validate locally. They
have not run on a final clean pushed commit, so signed checksums/provenance are
not claimed as issued evidence.

## Safe public claims

- internal HTTP framing hardening with the exact focused test counts above;
- short fixed-tree kill/partition/mixed Jepsen regression passed;
- explicit local versus quorum response fields exist and are checked;
- fixed single-node 100/100 SIGKILL recovery result with its exact scope;
- Prometheus examples/rules/dashboard validate;
- reproducible ARM64 WSL2 development benchmark, including the missed target;
- cold backup/restore and distinct-binary rollback drills passed;
- prepared for enterprise technical evaluation, not certified or production-
  proven.

## Unsupported claims

Do not claim production readiness, zero data loss, crash-safe/linearizable Raft,
universal exactly-once effects, 50k durable RPS, p99 at or below 3 ms, current
45/45 Jepsen, native ARM64/amd64 performance, power-loss durability, online
backup/GC, rolling upgrades, an SLA/default RPO/RTO, penetration testing, SOC 2,
ISO, AWS approval, Azure approval, or issued release provenance.

## Ayder Wire decision

Do not build a custom binary protocol in this sprint. `/health` demonstrated
sub-2 ms p99 in the earlier endpoint split, while durable throughput was
dominated by synchronous completion/persistence coordination. The evidence
does not show HTTP parsing as the limiting factor. Optimize completion/batching
without weakening semantics, then reprofile before reconsidering a new wire
protocol.

## Required release work

1. Run the complete strict 45-run Jepsen campaign on the final candidate,
   retaining histories, post-convergence snapshots and durable-ledger results.
2. Run power-loss/storage-error and longer idempotency/commit/HA campaigns.
3. Run the identical benchmark/crash suite on native target-like ARM64 and
   amd64 storage.
4. Commit an owned scope, push it, and require all build/security/CodeQL/SBOM
   jobs to pass on that immutable commit; retain the issued attestation bundle.
5. Obtain founder/pilot sign-off on exact topology, capacity, backup cadence,
   RPO/RTO, TLS/PKI, upgrade window, monitoring and incident ownership.

## Terminal summary

```text
FILES CHANGED: broker/worker visibility, persistence, Raft, idempotency,
  HTTP security, metrics, tests, benchmarks, CI, operations, docs
TESTS RUN: focused/unit/regression/live gates pass; full 45-run Jepsen pending
SANITIZERS: ASan+UBSan tested paths pass
FUZZERS: HTTP framing smoke 855 executions; long/AOF/HA campaigns pending
STATIC ANALYSIS: GCC analyzer pass; CodeQL final-commit run pending
BENCHMARKS: strict local matrix 308-382 RPS, 19-20 s p99, all cases non-clean
JEPSEN: short kill/partition/mixed pass with exact convergence; full 45 pending
SECURITY FINDINGS FIXED: framing ambiguity, bounds/deadlines, HA peer binding,
  tracked test keys, stale aiohttp, root runtime users
RESIDUAL RISKS: full HA/power campaign, poor durable performance, client TLS,
  online GC/backup, rolling upgrade, audit logging, final signed release proof
PERFORMANCE BEFORE: older result invalid for current durability comparison
PERFORMANCE AFTER: target missed; correctness barriers retained
CUSTOM TCP DECISION: not justified by current profile
READY TO MERGE: NO
```
