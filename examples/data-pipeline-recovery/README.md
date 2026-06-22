# Demo 3: Databricks / Airflow / dbt Partial Pipeline Recovery

This example shows Ayder below data tools as the durable broker, replay, and
commit layer for recovery-critical data pipeline runs.

The job may handle errors technically while the business still cannot trust its
output. The receipt uses `PASS`, `WARN`, or `FAIL` based on confirmed data,
missing pages, retry exhaustion, criticality, and audit evidence.

## Run

Default partial download:

```bash
./examples/data-pipeline-recovery/run_demo.sh
```

Select a scenario:

```bash
SCENARIO=full_success ./examples/data-pipeline-recovery/run_demo.sh
SCENARIO=partial_download ./examples/data-pipeline-recovery/run_demo.sh
SCENARIO=hard_failure ./examples/data-pipeline-recovery/run_demo.sh
SCENARIO=partial_usable ./examples/data-pipeline-recovery/run_demo.sh
SCENARIO=replay_missing_partition ./examples/data-pipeline-recovery/run_demo.sh
```

Run all five:

```bash
./examples/data-pipeline-recovery/run_all.sh
```

## Flow

```text
Airflow/Databricks job scheduled
-> Ayder produces data.download.requested
-> worker consumes original event
-> API pages are downloaded with retries
-> partial failure may occur
-> audit is written
-> offset commits only after audit
-> receipt determines business trust
```

## Scenarios

1. `full_success`: 10/10 pages confirmed, `PASS`, fully trusted.
2. `partial_download`: pages 9-10 missing after retries, `WARN`, manual review.
3. `hard_failure`: critical dataset pages missing, `FAIL`, downstream blocked.
4. `partial_usable`: non-critical pages missing, `WARN`, partial continuation.
5. `replay_missing_partition`: initial `WARN`; Ayder replays offset `0`, worker
   repairs only pages 9-10, final receipt `PASS`.

## Replay Evidence

The replay scenario preserves:

```text
receipt/receipt.before.json  WARN, missing [9,10]
receipt/receipt.json         PASS, confirmed 10/10
```

The final receipt includes the original offset and the exact pages recovered.
