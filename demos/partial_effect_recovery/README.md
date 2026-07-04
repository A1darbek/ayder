# Partial Effect Recovery

Adarsh demo: deterministic broker/replay evidence plus application-level
effect audit for safe recovery decisions.

This demo shows why "replay the event" is not always the safest recovery
action.

If no effects happened, full replay may be correct.

If some effects already happened, full replay can duplicate business effects.

Ayder provides the deterministic broker/replay/commit path.

The application effect audit records which expected effects were confirmed,
missing, ambiguous, or duplicated.

The receipt turns that evidence into a safe operator action.

## Boundary

Ayder is not magically inspecting business state. The application writes effect
audit evidence, and Ayder preserves the deterministic event/replay/commit path.

## Business Event

The demo produces one durable Ayder event:

```json
{
  "event_type": "order.confirmed",
  "event_id": "evt_order_001",
  "order_id": "ord_123",
  "customer_id": "cust_123",
  "amount": "42.00",
  "currency": "USD",
  "expected_effects": [
    "order_state_updated",
    "ledger_entry_created",
    "customer_notification_sent"
  ]
}
```

Expected effects:

1. `order_state_updated`
2. `ledger_entry_created`
3. `customer_notification_sent`

## Scenarios

| Scenario | Verdict | Operator answer |
|---|---:|---|
| `all_confirmed` | GREEN | Safe to close |
| `fully_missing_event` | RED | Replay whole event from Ayder |
| `partial_missing_notification` | YELLOW | Repair only the missing notification |
| `ambiguous_ledger` | YELLOW | Manual review before replay or repair |
| `duplicate_risk` | RED | Block blind replay; inspect duplicate effect first |

## Design-Partner Quick Start

From a fresh clone:

```bash
docker compose up -d --build
curl -fsS http://127.0.0.1:1109/health
./demos/partial_effect_recovery/run_all.sh
```

If a local `./ayder` binary exists, the demo uses it. If not, the demo starts a
clean one-off Ayder container from the Docker Compose image for each scenario.

Run the headline Adarsh scenario:

```bash
SCENARIO=partial_missing_notification ./demos/partial_effect_recovery/run_demo.sh
```

Run every scenario:

```bash
./demos/partial_effect_recovery/run_all.sh
```

Artifacts are written under `artifacts/partial_effect_recovery/`.
