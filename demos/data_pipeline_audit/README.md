# Demo 5: Data Pipeline / Reprocessing Audit Receipt

This demo shows the data-pipeline version of recovery evidence.

An API ingestion job starts, the external API fails after retries, and only a
partial dataset is loaded. The audit receipt records rows in/out, invalid
fields, failed rules, and missing data so operators know that business output
requires review.

## Run

```bash
./demos/data_pipeline_audit/run_demo.sh
```

Expected receipt:

```json
{
  "verdict": "WARN",
  "operator_answer": "Job completed with incomplete source data. Business output requires review.",
  "job": {
    "name": "daily_api_ingestion",
    "stage": "bronze_to_silver"
  },
  "retry_summary": {
    "api_attempts": 3,
    "final_error": "timeout",
    "source_status": "PARTIAL"
  },
  "data_quality": {
    "input_rows": 10000,
    "output_rows": 8420,
    "valid_rows": 8300,
    "invalid_rows": 120,
    "missing_rows": 1580
  }
}
```

Business rules:

- `D1` input/output row reconciliation: `WARN`
- `D2` required fields valid: `FAIL`
