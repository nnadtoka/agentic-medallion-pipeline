# dbt transformations

This project transforms Olist data from `bronze` through `staging`/`intermediate` views into
candidate marts materialized in `marts_candidate`. It does not write directly to `gold`;
promotion is a separate, privileged operation.

## Model flow

`bronze sources -> stg_olist_* -> int_* -> dim_* / fct_*`

Staging and intermediate models are views. Dimensions are tables. Facts are incremental
tables using merge keys at their documented grain. Every persisted model exposes
`created_ts`; incremental facts also expose `updated_ts` and `src_batch_id`.

## Run locally

From the repository root, with PostgreSQL running and `DBT_PASSWORD` set in `.env`:

```bash
set -a
source .env
set +a
.venv/bin/dbt debug --project-dir dbt --profiles-dir dbt
.venv/bin/dbt build --project-dir dbt --profiles-dir dbt
```

Inside the pipeline engine container:

```bash
docker compose exec pipeline-engine dbt build \
  --project-dir /app/dbt --profiles-dir /app/dbt
```

Use `--full-refresh` deliberately when the incremental fact tables must be rebuilt.

## Batch-driven incremental selection

The three fact models do not compare Bronze creation timestamps with dbt
update timestamps. During an orchestrated run they query
`staging.model_batch_control` and the single active
`staging.transformation_batches` row to find source batches not previously
consumed by that model. Affected order identifiers are de-duplicated before
dbt's keyed merge.

No target-batch variable is passed to dbt. Running dbt directly when no
transformation batch is active safely processes the complete model input.

The whole-table `SET SCHEMA` promotion removes candidate facts from `marts_candidate`;
`gold.promote_marts_to_gold()` clones each of the three incremental facts back into
`marts_candidate` right after the move so the next run's `is_incremental()` finds a prior
build to merge against instead of silently rebuilding from scratch. See
`pipeline/promotion/README.md` for why only the incremental facts (not every mart) get this
treatment.
