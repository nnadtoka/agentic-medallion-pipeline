# Transformation control

Transformation control is separate from `bronze.ingestion_batches`. dbt and
Prefect never change ingestion audit records.

`staging.transformation_batches` tracks the overall candidate-build, quality,
and promotion lifecycle. Its `batch_id` values use
`<job-prefix>_<POSIX-seconds>`, for example
`olist_transform_quality_1789340049`.

`staging.model_batch_control` is append-only lineage. Its composite key is
`(model_name, source_dataset_name, source_batch_id, target_batch_id)`. A source
batch is considered consumed by a model only when its associated target has
status `promoted`.

`staging.transformation_step_runs` records one row for each gate:

| Column | Purpose |
| --- | --- |
| `batch_id` | Parent transformation batch. |
| `step_name` | Stable gate name, such as `dbt_marts` or `gx_marts`. |
| `step_type` | `dbt`, `great_expectations`, or `promotion`. |
| `status` | `running`, `success`, or `failed`. |
| `started_ts` / `completed_ts` | Step execution interval. |
| `error_message` | Concise failure summary. |
| `failure_details` | JSONB identifying failed dbt resources or GX expectations. |
| `created_ts` | Control-record creation timestamp. |

Inspect the most recent execution and its steps:

```sql
select *
from staging.transformation_batches
order by created_ts desc
limit 1;

select *
from staging.transformation_step_runs
where batch_id = '<batch_id>'
order by started_ts;
```

The four dbt/GX steps must all be successful before the target can enter
`quality_passed`. Gold promotion and the successful `gold_promotion` step are
committed in the same PostgreSQL transaction.
