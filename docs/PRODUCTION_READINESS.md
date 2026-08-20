# Production readiness

Evidence updated: 2026-08-20. This is a technical gate, not an SLA or
certification. The fixed implementation has materially stronger correctness,
but required final-candidate and target-infrastructure evidence is incomplete.

**Production pilot with real data: blocked. READY TO MERGE: NO.**

## Gate summary

| Area | Status | Evidence | Remaining blocker |
|---|---|---|---|
| Cross-worker correctness | GREEN (focused) | 400/400 visibility; failure not acknowledged; replay repairs all workers | Longer saturation/restart campaign |
| Local durability | YELLOW | Partial-write/replay/fatal-state suite and 100/100 SIGKILL reconciliation | Power-loss and storage-error campaign |
| Atomic idempotency | GREEN (focused) | Concurrent same-key, conflict, injected final-store failure, restart/no-duplicate regression | Longer HA/fault workload |
| Raft implementation | YELLOW | Durable term/vote/log/commit/applied store and term/log/vote validation tests | Full strict 45-run plus power-loss/ack ledger |
| Quorum receipts | YELLOW | Explicit checked local/quorum/failover/all-worker fields; Jepsen requires all quorum fields | Broader failover/commit workloads and complete campaign |
| Performance | RED | Fixed-rate: 308–382 RPS, 19–20 s p99, timeouts. Closed-loop c1: 347.16 RPS, 5.522 ms median p99 with one 15.755 ms trial | Correctness-preserving completion/batching design and target hardware |
| Security | YELLOW | Focused framing/raw/live/analyzer/sanitizer gates pass | Independent review, long fuzzing, client TLS/key lifecycle |
| Observability | GREEN for evaluation | Config, 14 rules, dashboard, live metric lint pass | Pilot threshold tuning and retention/integration |
| Backup/restore | YELLOW | Checksummed cold backup/restore regression and real drill pass | No online/point-in-time backup or compatibility support window |
| Upgrade/rollback | YELLOW | Distinct old/fixed binary cold-backup rollback drill pass | No rolling or mixed-version HA contract |
| Supply chain | GREEN for candidate | Clean pushed candidate passed native ARM64/amd64 CI, CodeQL, sanitizers, Trivy/Gitleaks/SPDX and verified SLSA/SBOM attestations | Release-tag policy, image/runtime inventory and staffed patch process |

## Durability contract

A successful local mutation must explicitly report:

```text
durability_scope=local
local_fsync=true
applied_all_workers=true
quorum_committed=false
failover_safe=false
```

In HA mode, a failover-safe acknowledgement additionally requires checked
`durability_scope=quorum`, `quorum_committed=true`, and
`failover_safe=true`. Legacy `durable/synced` fields are local aliases and must
not be interpreted as quorum proof.

Current focused evidence covers process crash/restart, not physical power loss,
controller-cache behavior, filesystem corruption, cross-region loss, or every
cloud-volume failure mode.

## HA boundary

Implemented:

- stable term, voted-for, commit and applied state with checksummed durable
  records;
- durable log recovery, validated ordering/terms, tail repair and fail-closed
  corruption behavior;
- candidate-log recency, one vote per term, vote-response peer deduplication,
  stale-term rejection, previous-log-term validation;
- follower commit bounded by the index matched in the current AppendEntries;
- authenticated TLS peer identity pinned for the connection;
- explicit quorum receipt and all-worker application barriers;
- exact post-Jepsen convergence gate.

Withheld claim: “crash-safe/linearizable Raft.” The short kill, partition and
mixed regressions pass, but the full post-change 45-run campaign is mandatory
and pending. No production HA approval should be based only on unit/source
contracts or a short local WSL2 run.

## Operations evidence

The fixed single-node SIGKILL drill used a 0-record RPO and 5-second RTO target:
100 records acknowledged, 100 recovered, zero duplicates, 74 ms
fault-to-ready. This is an exercise result, not a default product objective.

The distinct-binary upgrade/rollback drill passed:

- old binary SHA `c21518cc0994ece2b2d750081e7a5c8aa9cf70918733029786e8feba1a2dfdc0`;
- fixed binary SHA `de14a5e1f46785785434d059dde9c4fc3b1b152eeafe36dd101151103c298008`;
- cold backup 255 ms, upgrade ready 67 ms;
- checksum-verified restore 37 ms, rollback ready 66 ms;
- pre-upgrade record present after upgrade and rollback;
- post-upgrade record intentionally absent after restoring the older snapshot.

The supported evaluation procedure is stop, verify stopped, cold backup,
upgrade, validate, and rollback by restoring both the old binary and its known-
compatible backup. Online snapshots, rolling HA upgrades, and in-place
downgrades are not supported claims.

## Security and supply chain

- 237 pure framing checks and 27 hardened raw-socket cases pass;
- normal/hardened/sanitized route smoke passes with bounded SIGTERM;
- GCC warnings-as-errors and focused analyzer gates pass;
- ASan/UBSan tested paths and 855 fuzz-smoke executions pass;
- Trivy final local scan: zero vulnerabilities and zero HIGH/CRITICAL
  misconfigurations; one MEDIUM style and two LOW tooling-image findings
  triaged;
- Gitleaks complete history: zero unsuppressed findings after eight exact
  fingerprints for removed, public development-only keys;
- Syft source/binary SPDX documents validate and are checksummed;
- third-party Actions are pinned to reviewed commit SHAs;
- clean commit `2e334d1` passed native ARM64/amd64 CI, CodeQL, sanitizers,
  Trivy/Gitleaks/source SPDX, and issued GitHub OIDC-backed SLSA and SBOM
  attestations. Checksums and offline verification are recorded in
  [clean-commit evidence](evidence/supply-chain/CLEAN_COMMIT_2e334d1.md).

The local triage remains useful all-severity detail. The clean-candidate hosted
gates and identity-bearing bundles now exist for the exact documented commit;
they do not cover later commits, container/runtime packages or release policy.

## Pilot checklist

All checked items require candidate-specific evidence, not only documentation.

### Correctness and data

- [ ] Final immutable commit and binary digest selected.
- [ ] Complete strict 45-run Jepsen campaign passes on that candidate.
- [ ] Acknowledged-write ledger survives crash, leader loss and storage faults.
- [ ] Pilot payload/idempotency/commit concurrency workload passes.
- [ ] Capacity limit and overload behavior measured on target hardware.
- [ ] Online sealed GC remains disabled or receives a safe coordinated design.

### Security

- [ ] Client ingress terminates TLS on private networking.
- [ ] Unique bearer tokens are provisioned, rotated and redacted.
- [ ] HA nodes use pilot-owned unique mTLS identities and CA.
- [ ] Metrics/admin ingress is authenticated and network-restricted.
- [x] Candidate CodeQL/Trivy/Gitleaks/SBOM jobs pass and findings are triaged.
- [ ] Security owner and patch/incident process are assigned.

### Operations

- [ ] Persistent volume, mount options, permissions and free-space alerts set.
- [ ] Cold backup cadence/retention/encryption/access controls approved.
- [ ] Restore and distinct-binary rollback rehearsed on pilot data scale.
- [ ] Pilot RPO/RTO defined separately for node crash, volume loss and restore.
- [ ] Monitoring, paging, logs, escalation and incident owner assigned.
- [ ] Upgrade/rollback maintenance window and compatibility matrix approved.
- [ ] No rolling mixed-version HA operation unless that exact pair is tested.

### Evidence and claims

- [ ] Native target-like ARM64 or amd64 benchmark/crash suite completed.
- [x] Candidate checksum/SBOM/provenance bundles issued, downloaded and verified.
- [ ] Buyer documentation references the same candidate and configuration.
- [ ] No unsupported SLA, certification, zero-loss, exactly-once, 50k RPS, or
  universal Raft-safety claim appears in release material.

## Blocking next actions

1. Run and retain the full strict 45-run Jepsen campaign.
2. Run native target-storage ARM64 and amd64 benchmark/crash tests.
3. Improve durable completion performance without weakening receipts.
4. Push an owned clean candidate and require all CI/CodeQL/scanner/provenance
   gates to complete.
5. Execute pilot-sized restore, volume-loss and failover drills against the
   pilot's written objectives.
