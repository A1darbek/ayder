# Ayder Evidence

Ayder separates broker-level correctness from application-level recovery
evidence.

> Revenue Readiness v1 status (2026-08-16): the published 45-run result below is
> historical. Persistence, acknowledgement, HA, and concurrency behavior changed
> materially on `hardening/revenue-readiness-v1`; the current-tree strict 45-run
> campaign has not run and the branch is not release-ready. Current-tree short
> kill, partition and mixed regressions passed with exact post-run convergence.
> See
> [`docs/REVENUE_READINESS_V1.md`](docs/REVENUE_READINESS_V1.md).

## 1. Broker Correctness

The Jepsen suite under `tests/jepsen/` exercises the `broker-log` workload under
mixed faults, network partitions, and process kills.

Published scoped result:

- verified March 13, 2026
- 9 matrix cells
- 5 runs per cell
- 45/45 recorded passes

Run:

```bash
AYDER_JEPSEN_PROFILE=hn-ready STRICT_CLAIM=1 \
  bash ./tests/demo/ha_broker_jepsen_gold.sh
```

The claim is bounded by the published workload, checker, fault model, build, and
configuration. It must not be described as evidence for later commits unless the
same campaign is rerun against them. The current checker additionally requires
`ok:true`, `durability_scope:"quorum"`, `local_fsync:true`,
`quorum_committed:true`, `failover_safe:true`,
`applied_all_workers:true`, a non-vacuous success floor, and exact cluster
convergence. Those strengthened checks are not retroactive proof about the
historical run.

## 2. Exactly-Once Reference Pipeline

The harness under `tests/e2e/exactly_once/` implements:

```text
Ayder produce with idempotency key
-> deterministic consume/replay
-> one Postgres transaction:
   dedupe event
   apply business effect
   advance consumer state
   append offset history
-> commit Ayder offset
-> verify invariants
```

Run:

```bash
bash ./tests/e2e/exactly_once/run_campaign.sh
```

Evidence is scoped to this consumer pattern. It does not claim universal
exactly-once delivery to arbitrary external systems.

## 3. Recovery Receipt Examples

The design-review examples use the actual Ayder produce, consume, replay, and
commit APIs and record:

- durable/sealed produce responses
- event offsets and replay source offsets
- audit-write timestamps
- commit-after-audit ordering
- idempotency suppression
- confirmed, missing, stale, or unresolved business outcomes
- operator actions and close/no-close decisions

Run:

```bash
./examples/payment-ambiguity-paypal/run_all.sh
./examples/payment-webhook-ordering/run_demo.sh
./examples/data-pipeline-recovery/run_all.sh
```

These are examples on top of Ayder's broker/replay path. Their domain tables and
business rules are application code, not broker internals.
