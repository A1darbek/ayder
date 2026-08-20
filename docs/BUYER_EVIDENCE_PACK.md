# Buyer evidence pack

Technical evidence index for Ayder Revenue Readiness v1. This is not a
marketing page, certification, penetration-test report, SLA, or production
approval.

**Current gate: RED. Paid real-data production pilot: blocked. READY TO MERGE:
NO.** The full post-change 45-run Jepsen campaign, native target-storage tests,
and final clean-commit CI/provenance are pending.

## Executive summary

Ayder is a single-binary HTTP-native durable event log/message broker for
recovery-critical AI and API workflows. This sprint fixed cross-worker record
visibility, added durable Raft state and stricter protocol validation, separated
local fsync from checked quorum receipts, implemented atomic payload-bound
idempotency, hardened HTTP framing, and added operational/release tooling.

Focused correctness and short fault tests pass. A fixed-tree single-node
SIGKILL drill recovered 100/100 acknowledged records. A distinct-binary cold-
backup upgrade/rollback drill passed. The required performance matrix did not:
it achieved only 308–382 RPS with roughly 19–20 second p99 and client timeouts.
A standard-wrk one-connection check separately measured 5.522 ms median p99 at
347.16 RPS, with one trial at 15.755 ms; it is closed-loop latency evidence and
does not supersede the failed fixed-rate capacity test.

## Evidence index

| Question | Evidence | Boundary |
|---|---|---|
| What is the architecture? | [Architecture](ARCHITECTURE.md) | HA is experimental pending final fault proof |
| What does an acknowledgement mean? | [API reference](api-reference.md), [Production readiness](PRODUCTION_READINESS.md#durability-contract) | Local fsync and quorum/failover are separate fields |
| What performance was measured? | [Benchmarks](BENCHMARKS.md), [fixed-rate JSON](evidence/benchmarks/rrv1-post-visibility-commitfix-wsl2.json), [closed-loop JSON](evidence/benchmarks/rrv1-wrk-durable-c1-ca9e210.json) | ARM64 WSL2/ext4 development only; fixed-rate target missed |
| What correctness was tested? | [Revenue report](REVENUE_READINESS_V1.md#tests-and-analysis-run), [Jepsen suite](../tests/jepsen/README.md) | Short current-tree faults pass; full 45 pending |
| What is the security model? | [Threat model](THREAT_MODEL.md), [Security policy](../SECURITY.md) | Internal hardening, not an independent audit |
| What fuzz/sanitizer evidence exists? | [Revenue report](REVENUE_READINESS_V1.md#tests-and-analysis-run), [fuzz harness](../fuzz/) | 855 smoke executions; no long/AOF/HA campaign |
| What can operators observe? | [Observability](OBSERVABILITY.md), [Prometheus](../deploy/prometheus/), [Grafana](../deploy/grafana/) | 14 example rules validate; tune thresholds per pilot |
| How are incidents/recovery handled? | [Runbook](RUNBOOK.md) | Online backup/GC and rolling upgrades unsupported |
| What supply-chain evidence exists? | [Scanner triage](evidence/supply-chain/SCANNER_TRIAGE.md), [SBOM status](evidence/supply-chain/SBOM.md), [workflows](../.github/workflows/) | Local dirty-tree pass; final issued attestations pending |
| What enterprise gaps remain? | [Enterprise matrix](ENTERPRISE_READINESS.md) | No cloud/certification/compliance claim |

## Exact evidence highlights

- Cross-worker visibility: 400/400 load-balanced reads saw the record; an
  injected worker-apply failure did not acknowledge; restart replay repaired
  every worker.
- P0 suite: 24 Raft protocol checks, 19 fault-injection checks, 47 durable-store
  checks, partial/EINTR/zero-progress rewrite checks, replay/corruption/fatal-
  state/timeout/commit/TLS contracts all passed.
- Idempotency: concurrent same-key retries, payload conflict, uncertain final-
  state recovery and duplicate absence passed.
- Short Jepsen: partition 18 s, kill 18 s, mixed 26 s all passed linearizability
  and nemesis completion, with exact final log/commit/applied convergence.
- HTTP security: 237 pure framing checks, 27 raw-socket cases, normal/hardened/
  sanitized live smoke and GCC analyzer gates passed.
- Crash drill: 100 acknowledged, 100 recovered, zero loss/duplicates, 74 ms
  fault-to-ready against the exercise's 0-record/5-second target.
- Upgrade/rollback: distinct old/fixed binary hashes, checksummed cold backup,
  upgrade and restored rollback behavior passed.
- Supply chain: zero final Trivy vulnerabilities, zero HIGH/CRITICAL
  misconfigurations, and zero unsuppressed Gitleaks history findings; source
  and binary SPDX documents validate.

## Buyer evaluation checklist

- [ ] Candidate commit, binary SHA and configuration match every artifact.
- [ ] Full strict 45-run Jepsen and acknowledged-ledger fault campaign pass.
- [ ] Native target-like ARM64/amd64 capacity and crash evidence meets the
  pilot's load/headroom objectives.
- [ ] Pilot explicitly chooses local versus quorum acknowledgement semantics.
- [ ] Pilot-owned TLS/PKI, bearer rotation and metrics/admin network policy are
  configured.
- [ ] Clean CodeQL/build/sanitizer/scanner/SBOM CI passes; checksum and Sigstore
  provenance bundles are issued and retained.
- [ ] Backup cadence, restore validation, upgrade/rollback window and incident
  owner are accepted.
- [ ] Separate RPO/RTO objectives are written and drilled for process crash,
  volume loss and full restore.

## Safe claims

Safe with exact scope:

- “Prepared for enterprise technical evaluation.”
- “Current-tree short kill, partition and mixed Jepsen regressions passed.”
- “Local fsync and checked quorum/failover acknowledgement are separate.”
- “A 100-record local SIGKILL recovery drill recovered every acknowledged ID.”
- “The published ARM64 WSL2 development benchmark missed its target.”
- “Cold backup/restore and a distinct-binary rollback drill passed.”

Not supported:

- production-ready, enterprise-certified, AWS/Azure approved, SOC 2/ISO
  compliant, penetration tested;
- crash-safe or universally linearizable Raft;
- zero data loss, exactly-once external effects, online backup, rolling upgrade;
- 50k durable RPS, p99 at or below 3 ms, or native production performance;
- current-tree 45/45 Jepsen, a default SLA/RPO/RTO, or an issued signed release.

Ayder records durable evidence for recovery and reconciliation. It does not
automatically know whether an arbitrary external provider completed a side
effect, and it does not make arbitrary external effects exactly once.
