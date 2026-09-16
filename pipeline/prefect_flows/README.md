# Prefect Flows

This package contains Prefect-specific orchestration for ingestion, dbt
transformations, and data-quality gating. Database transaction logic remains in
`pipeline/ingestion/`; flows call that logic and report state to Prefect.

Modules:

- `daily_ingestion_flow.py`: date-partitioned orders and reviews.
- `backfill_flow.py`: manual full-refresh loads for non-partitioned tables.
- `transformation_flow.py`: dbt build, staged through Great Expectations
  validation, ending in gold promotion and a catalog sync.
- `catalog_sync_flow.py`: dbt manifest → pgvector catalog sync, standalone or
  called from `transformation_flow`.
- `common.py`: shared Prefect tasks and configuration validation.
- `serve_flows.py`: registers deployments and waits for flow runs inside the
  `pipeline-engine` container.

## Serving and deployments

When `serve_flows.py` starts, it creates or updates the deployment definitions
in Prefect Server, then remains running to execute scheduled or manually
triggered flow runs. It does not redeploy before every run.

Flow code lives in the `pipeline-engine` image. Prefect Server stores deployment
metadata, parameters, schedules, and run history. After changing flow code,
rebuild and restart the engine so it serves the updated definitions:

```bash
docker compose up -d --build pipeline-engine
```

## Operations

Start the local stack and inspect service status:

```bash
docker compose up -d --build
docker compose ps
```

Verify the Prefect API and follow engine logs:

```bash
curl --fail http://127.0.0.1:4200/api/health
docker compose logs -f pipeline-engine
```

Open the local Prefect UI at <http://127.0.0.1:4200>. Use the UI to inspect
deployments, supply explicit flow parameters, trigger manual runs, and review
flow and task states.

Trigger and watch a daily ingestion run:

```bash
docker compose exec pipeline-engine prefect deployment run \
  olist-daily-ingestion/daily-ingestion \
  --param batch_date=2017-10-04 \
  --watch
```

Daily ingestion runs incremental dbt models after ingestion. Backfill runs dbt
with `--full-refresh` so removed snapshot rows cannot remain in incremental
facts. Set `run_transformations=false` only for ingestion-only diagnostics.

Each transformation creates a text target identifier using the initiating job
prefix and current POSIX timestamp seconds. The flow records these gates in
`staging.transformation_step_runs`:

1. `dbt_staging_intermediate`
2. `gx_intermediate`
3. `dbt_marts`
4. `gx_marts`
5. `gold_promotion`

The source batches used by each model are selected through database control
tables before the marts step; Prefect does not pass source or target batch IDs
to dbt through `--vars`. See `pipeline/transformation_control/README.md`.

Trigger the transformation and quality gate without ingestion:

```bash
docker compose exec pipeline-engine prefect deployment run \
  olist-transform-quality/transform-quality \
  --watch
```

Trigger a one-table backfill test. Omit `tables` to use the complete configured
backfill table list:

```bash
docker compose exec pipeline-engine prefect deployment run \
  olist-backfill/backfill \
  --param snapshot_date=2026-09-12 \
  --param 'tables=["raw_product_category_translation"]' \
  --watch
```

Stop the services without deleting their volumes:

```bash
docker compose down
```

Do not add `--volumes` unless persistent PostgreSQL and Prefect data should be
removed intentionally.
