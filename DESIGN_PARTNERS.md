# Design Partner Evaluation

Ayder is the durable broker/replay layer. The examples in this repository show
how domain applications can turn broker facts into recovery receipts.

## Recommended Review

1. Run the three design-review examples:

```bash
./examples/payment-ambiguity-paypal/run_all.sh
./examples/payment-webhook-ordering/run_demo.sh
./examples/data-pipeline-recovery/run_all.sh
```

2. Run the exactly-once reference pipeline:

```bash
bash ./tests/e2e/exactly_once/run_campaign.sh
```

3. Review broker-level Jepsen tooling:

```bash
bash ./tests/demo/ha_broker_jepsen_gold.sh
```

## Evaluation Questions

- Are durable/sealed produce and committed-offset facts clear?
- Can the application explain confirmed, missing, stale, duplicate, or
  unresolved effects?
- Does the receipt make a concrete close/no-close decision?
- Are replay and repair actions scoped enough to avoid blindly repeating side
  effects?

The examples are applications on top of Ayder's broker/replay path. They are
not claims that payment, banking, webhook, or data-pipeline logic is built into
the broker.
