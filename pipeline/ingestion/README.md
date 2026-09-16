# Olist Bronze Ingestion

`ingest_olist.py` loads Olist CSV data into PostgreSQL's `bronze` schema. It
supports date-based incremental ingestion for sources with a clear event date
and full-table replacement for snapshot-style sources.

The target deletion, insert, and successful control-record update run in one
transaction. If a load fails, PostgreSQL rolls back all Bronze data changes and
the pipeline records the failed execution separately for audit.

## Dataset strategies

| Bronze table | Source file | Strategy | Batch date source |
| --- | --- | --- | --- |
| `raw_orders` | `olist_orders_dataset.csv` | Incremental | `order_purchase_timestamp` |
| `raw_order_reviews` | `olist_order_reviews_dataset.csv` | Incremental | `review_creation_date` |
| `raw_order_items` | `olist_order_items_dataset.csv` | Full refresh | Snapshot date |
| `raw_customers` | `olist_customers_dataset.csv` | Full refresh | Snapshot date |
| `raw_sellers` | `olist_sellers_dataset.csv` | Full refresh | Snapshot date |
| `raw_products` | `olist_products_dataset.csv` | Full refresh | Snapshot date |
| `raw_geolocation` | `olist_geolocation_dataset.csv` | Full refresh | Snapshot date |
| `raw_order_payments` | `olist_order_payments_dataset.csv` | Full refresh | Snapshot date |
| `raw_product_category_translation` | `product_category_name_translation.csv` | Full refresh | Snapshot date |

`raw_order_items` is a full-refresh dataset because `shipping_limit_date` is a
deadline rather than a record-created date.

For orders, Bronze preserves the original `order_purchase_timestamp` and also
surfaces the pipeline-derived `order_purchase_date`. The derived date is used
for incremental selection and forced date-range replacement.

## Prerequisites

- PostgreSQL has been initialized with the SQL files in `postgres/init/`.
- The Olist CSV files are available under `data/olist/`.
- A repository-root `.env` file contains the database settings from
  `.env.example`.

The containerized pipeline engine connects as the least-privilege
`ingestion_writer` role. Its password is supplied through `INGESTION_PASSWORD`;
the engine does not receive the PostgreSQL administrator password.

When running from the host against the provided Compose database, set
`POSTGRES_HOST=127.0.0.1` and `POSTGRES_PORT=5433` in `.env`.

Start PostgreSQL from the repository root:

```bash
docker compose up -d postgres
```

Create a local virtual environment and install the ingestion dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r pipeline/ingestion/requirements.txt
```

The following examples assume the virtual environment is active. You can also
run them without activation by replacing `python` with `.venv/bin/python`.

## Incremental ingestion

Incremental tables require `--batch-date` in `YYYY-MM-DD` format. The date
column is owned by dataset configuration and is not supplied through the CLI.

```bash
python -m pipeline.ingestion.ingest_olist \
  --input-file data/olist/olist_orders_dataset.csv \
  --table raw_orders \
  --batch-date 2017-10-02
```

```bash
python -m pipeline.ingestion.ingest_olist \
  --input-file data/olist/olist_order_reviews_dataset.csv \
  --table raw_order_reviews \
  --batch-date 2017-10-02
```

## Full-refresh ingestion

Full-refresh tables load the complete CSV and atomically replace all existing
rows. `--batch-date` represents the snapshot date and is optional; when omitted
it defaults to the current UTC date.

```bash
python -m pipeline.ingestion.ingest_olist \
  --input-file data/olist/olist_customers_dataset.csv \
  --table raw_customers
```

To assign an explicit, reproducible snapshot date:

```bash
python -m pipeline.ingestion.ingest_olist \
  --input-file data/olist/olist_order_items_dataset.csv \
  --table raw_order_items \
  --batch-date 2018-10-17
```

Use the same command shape for the remaining full-refresh tables listed above.

## Forced replacement

`--force` is permitted regardless of prior batch state. It replaces the target
date for an incremental dataset or the complete table for a full-refresh
dataset, and records the execution as a `backfill`.

```bash
python -m pipeline.ingestion.ingest_olist \
  --input-file data/olist/olist_orders_dataset.csv \
  --table raw_orders \
  --batch-date 2017-10-02 \
  --force
```

Without `--force`, an already successful dataset and batch-date combination is
skipped. Failed executions do not block a new ingestion attempt.

## Ingestion audit

`bronze.ingestion_batches` keeps one append-only record for every physical
execution. Regular pipeline flow never deletes audit records. Only these
operational fields are updated while an execution is finalized:

- `completed_ts`
- `rows_read`
- `rows_loaded`
- `status`
- `error_message`

Run types are `incremental`, `full_refresh`, and `backfill`; there is no special
retry lifecycle.

Inspect recent executions:

```bash
docker compose exec postgres psql \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  -c "SELECT * FROM bronze.ingestion_batches ORDER BY created_ts DESC LIMIT 10;"
```

## Tests

Install the development dependencies and run the unit tests from the repository
root:

```bash
python -m pip install -r pipeline/ingestion/requirements-dev.txt
python -m pytest pipeline/tests
```
## Full-history incremental backfill

Load every populated order and review date as its own logical incremental
batch, reload snapshot sources once, then run the full dbt/GX/promotion gate:

```bash
docker compose exec pipeline-engine \
  python -m pipeline.ingestion.backfill_incremental --force
```

The source CSVs are parsed once per dataset and reused across daily batches.
Each date still commits independently and is recorded in
`bronze.ingestion_batches`; `--force` atomically replaces already-loaded dates.
