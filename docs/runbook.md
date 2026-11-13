# Runbook

## Prerequisites

- Databricks workspace with Unity Catalog (Free Edition works) and serverless compute
- Databricks CLI authenticated to the workspace (`databricks auth login`)
- Access to `https://hapi.fhir.org/baseR4`

## Deploy

1. Review `conf/pipeline.yaml` (catalog, schemas, volume, lookback).
2. Create or update the job:
   ```bash
   ./pipelines/deploy_job.sh
   ```
   The job reads notebooks from the `main` branch of this repository.
3. Run it once from the Jobs UI or with `databricks jobs run-now <job_id>`. The `setup` task creates schemas, the volume and control tables.
4. Unpause the schedule (daily 02:00 UTC) when ready.

To run manually, open the notebooks in order from a Git folder: `00_setup`, `01_raw`, `02_bronze`, `03_silver`, `04_gold`, `05_quality`.

## Parameters

| Parameter | Default | Use |
|---|---|---|
| `run_date` | today (UTC) | windows end before this date |
| `lookback_days` | `ingestion.lookback_days` | number of day windows |
| `run_id` | job run id | written to every log row |
| `resource` | all enabled | comma separated subset when running notebooks directly |

## Monitoring

```sql
SELECT * FROM workspace.fhir_control.pipeline_run_log ORDER BY started_at DESC;
SELECT resource, count(*) pages, sum(entry_count) entries
FROM workspace.fhir_control.api_call_log WHERE run_id = '<run_id>' GROUP BY resource;
SELECT * FROM workspace.fhir_control.ingestion_watermark;
SELECT * FROM workspace.fhir_control.dq_results WHERE run_id = '<run_id>' AND NOT passed;
```

## Common tasks

- **Backfill**: run with a larger `lookback_days`. Windows already behind the watermark are skipped, so for a full reload delete that resource's row from `ingestion_watermark` first. Re-fetched data is safe: Bronze keeps every copy and Silver ignores versions it already has.
- **Rerun a failed task**: repair the run from the Jobs UI. Raw writes the watermark only after a window is logged, so a rerun continues from the last completed window.
- **Page cap error**: raise `api.max_pages_per_window` (or `api.page_size`) and rerun.
- **New Silver column**: add it to `conf/silver_columns.yaml`. Existing Silver tables need the column added (`ALTER TABLE ... ADD COLUMN`) or a rebuild.
- **API errors**: 429 and 5xx responses are retried with exponential backoff (`api.max_retries`, `api.backoff_seconds`). Other 4xx responses fail the task immediately.

## Local tests

```bash
pip install -r requirements.txt
pytest tests
```

Requires Java 17 for the Spark tests. API tests use fixtures and do not call the live server.
