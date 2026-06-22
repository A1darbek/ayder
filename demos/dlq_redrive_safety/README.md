# Demo 4: SQS / DLQ Redrive Safety

This demo shows that a DLQ is not just a technical queue. A DLQ item can
represent unresolved business state, so redrive safety has to be checked before
replay.

## Run

```bash
./demos/dlq_redrive_safety/run_demo.sh
```

Expected receipt:

```json
{
  "verdict": "YELLOW",
  "operator_answer": "Do not blindly redrive. One DLQ item represents unresolved business state.",
  "dlq_summary": {
    "items": 1,
    "safe_to_redrive": 0,
    "manual_review_required": 1
  }
}
```

## Scenario

- `msg_008` processes successfully.
- `msg_009` fails three times with `consumer_error`.
- `msg_009` moves to DLQ.
- `msg_010` continues and processes successfully.
- The team wants to redrive.
- Ayder classifies `msg_009` as replay safety `UNKNOWN`.

## DLQ Item

```json
{
  "message_id": "msg_009",
  "business_operation": "order_payment_confirmation",
  "failure_reason": "consumer_error",
  "last_known_effect": "payment_state_unknown",
  "replay_safety": "UNKNOWN",
  "recommended_action": "manual review before redrive"
}
```

## Dispositions

- `SAFELY_REDRIVEN`
- `MANUALLY_REPAIRED`
- `COMPENSATED`
- `SUPPRESSED_DUPLICATE`
- `UNRESOLVED`
- `UNSAFE_TO_REPLAY`
