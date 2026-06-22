# Partial Batch Recovery Receipt Demo 1.1

This is the smallest reproducible incident harness for Ayder recovery evidence.
It uses the current Ayder broker API, Postgres, and shell scripts to show the
failure mode operators immediately understand:

- 10 events are produced to Ayder and registered as expected business work.
- Each event expects three business effects:
  - `ledger_update`
  - `order_state_updated`
  - `notification_sent`
- A batch consumer reads all 10 events.
- It fully applies the first 8 events.
- It misses event 9 entirely.
- It partially applies event 10: ledger/order effects are observed, but the
  notification effect is missing.
- It incorrectly commits the offset past the whole batch.
- Ayder can look healthy, but business damage is still present.
- The recovery receipt reconciles expected effects against observed effects and
  tells the operator what to repair.

This is not a passive validator demo.

Events flow through Ayder's event/replay/recovery path. Consumers emit
business-effect records. Ayder compares expected effects vs observed effects for
a bounded recovery window. The receipt tells the operator whether recovery is
safe to close, and what action is safe when it is not.

## Quick Run

```bash
bash ./tests/e2e/partial_batch_receipt/run_campaign.sh
```

The default run is deterministic:

```text
VERDICT: RED
event_status_counts.confirmed    8
event_status_counts.missing      1
event_status_counts.partial      1
effect_status_counts.confirmed   26
effect_status_counts.missing     4
```

The important operational section is:

```text
RECOMMENDED ACTIONS

1. evt-pbr-000009
   status: MISSING
   action: Replay/repair full event.

2. evt-pbr-000010
   status: PARTIAL
   action: Repair notification only. Do not replay full event blindly.
   warning: do not replay full event blindly
```

Artifacts are written under:

```text
artifacts/partial_batch_receipt/<run_id>/
```

Key files:

- `receipt/receipt.txt` - operator-readable recovery receipt
- `receipt/receipt.json` - machine-readable receipt
- `producer.log` - expected event registration and Ayder produce path
- `consumer.log` - partial batch processing and bad offset commit
- `recovery_receipt.log` - reconciliation/persistence step
- `summary.txt` - run parameters and artifact paths

## Modes

Default silent loss:

```bash
MODE=silent_skip bash ./tests/e2e/partial_batch_receipt/run_campaign.sh
```

Recoverable DLQ backlog:

```bash
MODE=dlq bash ./tests/e2e/partial_batch_receipt/run_campaign.sh
```

Healthy control run:

```bash
MODE=correct bash ./tests/e2e/partial_batch_receipt/run_campaign.sh
```

Run modes sequentially. If you intentionally run multiple campaigns at the same
time, give each one a different `AYDER_PORT`, `TOPIC`, and `GROUP`.

## Tunables

```bash
TOTAL_EVENTS=10 \
BATCH_SIZE=10 \
POISON_TAIL=2 \
MODE=silent_skip \
bash ./tests/e2e/partial_batch_receipt/run_campaign.sh
```

If Ayder is already running:

```bash
START_AYDER=0 \
AYDER_BASE=http://127.0.0.1:1109 \
TOKEN=dev \
bash ./tests/e2e/partial_batch_receipt/run_campaign.sh
```

By default the wrapper uses the Ayder binary's default AOF mode. To override it:

```bash
AYDER_AOF=never bash ./tests/e2e/partial_batch_receipt/run_campaign.sh
```

## What The Receipt Answers

The receipt gives incident-response evidence in business terms:

- affected event window
- expected business effects
- confirmed effects
- missing effects
- partial events
- recommended repair actions
- duplicate or over-applied effects
- retry/DLQ backlog
- checked business rules
- per-account ledger reconciliation

That is the point of the demo: not just "the broker recovered", but "these are
the business effects that did and did not happen after the incident window."
