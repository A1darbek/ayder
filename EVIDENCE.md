# Ayder Evidence

Ayder separates broker-level correctness from application-level recovery
evidence.

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
bash ./tests/demo/ha_broker_jepsen_gold.sh
```

The claim is bounded by the published workload, checker, fault model, build, and
configuration.

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
