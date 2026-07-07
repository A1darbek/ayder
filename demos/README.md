# Additional Recovery Demos

These smaller demos explore specific recovery-receipt patterns on top of
Ayder's broker/replay path.

| Demo | Receipt focus | Run |
|---|---|---|
| `provider_timeout/` | Financially indeterminate provider timeout | `./demos/provider_timeout/run_demo.sh` |
| `ledger_clearance/` | Internally balanced ledger with unresolved external reconciliation | `./demos/ledger_clearance/run_demo.sh` |
| `webhook_ordering/` | Duplicate and stale webhook suppression | `./demos/webhook_ordering/run_demo.sh` |
| `partial_effect_recovery/` | Confirmed, missing, ambiguous, and duplicated effect guidance | `./demos/partial_effect_recovery/run_demo.sh` |
| `http_query_method/` | RFC 10008 HTTP QUERY for safe broker queries with request bodies | `./demos/http_query_method/run_demo.sh` |
| `http_query_recovery_snapshot/` | Safe bounded recovery queries with ETag, 304, explain, and no broker mutation | `./demos/http_query_recovery_snapshot/run_demo.sh` |
| `dlq_redrive_safety/` | Business-state review before DLQ redrive | `./demos/dlq_redrive_safety/run_demo.sh` |
| `data_pipeline_audit/` | Partial source and data-quality audit | `./demos/data_pipeline_audit/run_demo.sh` |

For the polished design-review narratives, use `examples/`.
