# Demo 3: Duplicate / Out-of-Order Webhooks

This demo shows that Ayder can prove duplicate and stale webhook attempts were
safely suppressed.

## Run

```bash
./demos/webhook_ordering/run_demo.sh
```

## Scenario

1. `webhook_001`: `SUCCESS` arrives first.
2. Payment becomes `SUCCESS`.
3. `webhook_002`: stale `PROCESSING` arrives later.
4. System does not downgrade `SUCCESS` to `PROCESSING`.
5. `webhook_003`: duplicate `SUCCESS` arrives.
6. System suppresses the duplicate business effect.

## Expected Receipt

```json
{
  "verdict": "GREEN",
  "operator_answer": "Payment state is correct. Duplicate and stale webhook attempts were safely suppressed.",
  "payment_id": "pay_123",
  "final_state": "SUCCESS"
}
```

The receipt includes two suppressed attempts:

- `webhook_002`: `stale_state_transition`, action `IGNORED`
- `webhook_003`: `duplicate_webhook`, action `SUPPRESSED`

Business rules:

- `W1` payment state-machine monotonicity
- `W2` no duplicate payment effect
- `W3` duplicate webhooks safely suppressed
