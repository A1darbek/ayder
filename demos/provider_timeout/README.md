# Demo 2: Provider Timeout / Financial Indeterminism

This demo shows that service recovery is not the same thing as financial
recovery.

A payment is complete only when the external provider commitment is confirmed,
rejected, compensated, or explicitly transferred to unresolved review. If the
provider times out and verification cannot determine the result, the payment is
financially indeterminate.

This is not a payment dashboard.

The payment attempts flow through Ayder's event/replay/recovery path. Consumers
emit business-effect records. Ayder emits a receipt showing which attempts are
confirmed, failed, suppressed, ambiguous, or financially indeterminate.

## Run

```bash
./demos/provider_timeout/run_demo.sh
```

Expected verdict:

```text
VERDICT: YELLOW
financial_determinism: FAIL
```

Operator answer:

```text
Do not close payment incident. One payment attempt remains financially indeterminate.
```

## Scenario

- `pay_attempt_001`: provider confirms `SUCCESS`
- `pay_attempt_002`: provider confirms `FAILED`
- `pay_attempt_003`: provider times out, verification confirms `SUCCESS`
- `pay_attempt_004`: provider times out, verification remains `UNKNOWN`
- `pay_attempt_005`: duplicate retry suppressed by idempotency key

## Receipt Shape

The receipt contains:

- `summary`
- `indeterminate_attempts`
- `business_rules`
- per-attempt state and effect evidence

The key indeterminate attempt is:

```json
{
  "attempt_id": "pay_attempt_004",
  "state": "AMBIGUOUS",
  "provider_commitment": "UNKNOWN",
  "customer_state": "PENDING_CONFIRMATION",
  "recommended_action": "Retry provider verification. Do not retry charge blindly."
}
```
