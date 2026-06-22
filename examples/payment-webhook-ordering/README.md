# Demo 2: Duplicate / Out-of-Order Payment Webhook

This example demonstrates deterministic Ayder replay and commit evidence for
payment webhooks that arrive duplicated or out of order.

Core rules:

- Duplicate webhook effects must not apply twice.
- A stale lower-priority state must not downgrade a terminal state.
- `SUCCESS` and `FAILED` are terminal.
- `FAILED -> SUCCESS` requires a new `payment_id`.
- Duplicate `provider_event_id` values are suppressed.

## Run

```bash
./examples/payment-webhook-ordering/run_demo.sh
```

## Flow

```text
SUCCESS webhook
-> Ayder produce webhook.received
-> worker applies SUCCESS
-> decision audit written
-> offset committed

later PROCESSING webhook
-> stale transition detected
-> ignored
-> decision audit written
-> offset committed

later duplicate SUCCESS webhook
-> duplicate provider_event_id detected
-> business effect suppressed
-> decision audit written
-> offset committed
```

## Expected Receipt

```json
{
  "verdict": "GREEN",
  "payment_state": {
    "payment_id": "pay_123",
    "final_state": "SUCCESS",
    "terminal_states": ["SUCCESS", "FAILED"],
    "safe_to_close": true
  },
  "webhook_handling": [
    {"event": "SUCCESS", "decision": "APPLIED"},
    {"event": "PROCESSING", "decision": "IGNORED_STALE_TRANSITION"},
    {"event": "SUCCESS", "decision": "SUPPRESSED_DUPLICATE"}
  ]
}
```

The receipt also proves:

- three durable/sealed events were consumed in deterministic offset order
- all three offsets were committed after decision audits
- only one payment-success business effect exists
- no terminal-state downgrade or `FAILED -> SUCCESS` mutation was applied
