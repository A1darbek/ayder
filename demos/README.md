# Additional Recovery Demos

These smaller demos explore specific recovery-receipt patterns on top of
Ayder's broker/replay path.

| Demo | Receipt focus | Run |
|---|---|---|
| `provider_timeout/` | Financially indeterminate provider timeout | `./demos/provider_timeout/run_demo.sh` |
| `ledger_clearance/` | Internally balanced ledger with unresolved external reconciliation | `./demos/ledger_clearance/run_demo.sh` |
| `webhook_ordering/` | Duplicate and stale webhook suppression | `./demos/webhook_ordering/run_demo.sh` |
| `dlq_redrive_safety/` | Business-state review before DLQ redrive | `./demos/dlq_redrive_safety/run_demo.sh` |
| `data_pipeline_audit/` | Partial source and data-quality audit | `./demos/data_pipeline_audit/run_demo.sh` |

For the polished design-review narratives, use `examples/`.
