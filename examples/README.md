# Design-Review Examples

These examples show how applications can build recovery evidence on top of
Ayder's durable broker, replay, idempotency, and offset commit APIs.

They are deliberately separated from Ayder's core source:

- Ayder supplies the event durability and replay facts.
- The application supplies domain states, effects, and business rules.
- The receipt combines both into an operator decision.

## 1. Payment Ambiguity / PayPal Verification

Path: `payment-ambiguity-paypal/`

Demonstrates:

- frontend redirect is not provider truth
- timeout is not failure
- provider verification before retry
- database as source of truth
- Redis cache repair
- idempotent payment initiation
- YELLOW to GREEN recovery

```bash
./examples/payment-ambiguity-paypal/run_all.sh
```

## 2. Duplicate / Out-of-Order Payment Webhooks

Path: `payment-webhook-ordering/`

Demonstrates:

- `SUCCESS` and `FAILED` are terminal
- stale `PROCESSING` cannot downgrade `SUCCESS`
- duplicate provider event suppression
- one payment-success business effect
- audit-before-offset-commit evidence

```bash
./examples/payment-webhook-ordering/run_demo.sh
```

## 3. Databricks / Airflow / dbt Pipeline Recovery

Path: `data-pipeline-recovery/`

Demonstrates:

- PASS/WARN/FAIL based on business trust, not process exit alone
- page/chunk retry exhaustion
- critical versus non-critical missing data
- audit-before-offset-commit evidence
- replay of the original event to recover missing partitions
- WARN to PASS recovery

```bash
./examples/data-pipeline-recovery/run_all.sh
```
