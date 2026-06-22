# Demo 1: Payment Ambiguity / PayPal Verification

This example demonstrates deterministic broker, replay, commit, provider
verification, database, and Redis-cache evidence for an ambiguous payment flow.

Core rules:

- Frontend success redirect is not truth.
- Provider timeout is not failure.
- Do not blindly retry a charge.
- Verify PayPal using `provider_order_id`.
- Postgres is the source of truth.
- Redis is a cache and is backfilled from Postgres.

## Run

Default unresolved timeout:

```bash
./examples/payment-ambiguity-paypal/run_demo.sh
```

Select a scenario:

```bash
SCENARIO=happy_path ./examples/payment-ambiguity-paypal/run_demo.sh
SCENARIO=timeout_then_success ./examples/payment-ambiguity-paypal/run_demo.sh
SCENARIO=timeout_unknown ./examples/payment-ambiguity-paypal/run_demo.sh
SCENARIO=duplicate_initiation ./examples/payment-ambiguity-paypal/run_demo.sh
SCENARIO=redis_stale ./examples/payment-ambiguity-paypal/run_demo.sh
```

Run all five:

```bash
./examples/payment-ambiguity-paypal/run_all.sh
```

## Flow

```text
payment initiated
-> Ayder produces payment.initiated
-> worker consumes event
-> mock PayPal creates order
-> Postgres transaction writes PENDING + audit
-> provider result may be timeout/unknown
-> Ayder offset commits after audit write
-> cron/manual verification queries PayPal
-> Postgres is updated
-> Redis is backfilled from Postgres
-> receipt is generated
```

## Scenarios

1. `happy_path`: PayPal commitment `SUCCESS`, DB and Redis `SUCCESS`, GREEN.
2. `timeout_then_success`: initial YELLOW receipt, verification resolves to
   `SUCCESS`, final receipt GREEN.
3. `timeout_unknown`: three `UNKNOWN` checks, YELLOW, manual reconciliation,
   `safe_to_close=false`.
4. `duplicate_initiation`: same idempotency key produced twice, one logical
   event and one wallet credit.
5. `redis_stale`: DB says `SUCCESS`, Redis starts `PENDING`, database wins and
   Redis is backfilled.

## Ayder Evidence

The receipt records the real produce response and derives:

- `event_durable`
- `event_sealed`
- `event_synced`
- `batch_id`
- event offset
- idempotency duplicate suppression
- offset committed after the database audit write
- deterministic replay availability

Artifacts are written under:

```text
artifacts/payment_ambiguity_paypal/<run_id>/
```
