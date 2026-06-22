# Demo 2.1: Ledger Clearance / Bank Reconciliation

This demo shows the ledger-specific version of recovery correctness.

The point is not to model a broken ledger where a debit happened and a credit is
missing. A good ledger posts internally as one atomic journal. The harder
incident is when the ledger is internally safe, funds are held in clearance, and
external bank/provider reality is still missing or mismatched.

## Run

```bash
./demos/ledger_clearance/run_demo.sh
```

Expected receipt:

```text
verdict: YELLOW
ledger_integrity.status: PASS
clearance.state: HELD_IN_CLEARANCE
external_reconciliation.bank_statement_match: MISSING
external_reconciliation.discrepancy_status: UNEXPLAINED
```

Operator answer:

```text
Do not close reconciliation. Ledger is internally consistent, but external provider/bank discrepancy remains unresolved.
```

## Scenario

Initial balances:

- `user_available = 100`
- `clearance_account = 0`

Payment starts for `20 USD`:

- ledger journal commits atomically
- `user_available = 80`
- `clearance_account = 20`
- debits equal credits
- no partial ledger posting

External state:

- provider confirmation is missing
- bank statement match is missing
- discrepancy remains unexplained

## What The Receipt Proves

- Ledger is internally safe.
- Funds are accounted for in clearance.
- External reality is not reconciled.
- Do not close.
- Manual reconciliation required.
