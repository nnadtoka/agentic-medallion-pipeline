# Catalog Sync

`sync.py` reads dbt's compiled `manifest.json`, embeds every non-empty
table/column `description:` with a local model, and upserts the result into
`catalog.dataset_embeddings` (schema defined in
`postgres/init/031_catalog_tables.sql`). It does not write `COMMENT ON` itself
-- see "Table/column comments" below for how those get set instead.

See `mcp_agent/README.md` for the design this is part of.

## Scope

Only marts models are read -- identified by `node["schema"] == "marts_candidate"`
in the manifest, since dbt builds them there and promotion (not dbt) is what
later moves them into `gold`; `"gold"` is filled in as the catalog's recorded
`schema_name` since that's where the table actually lives once promoted.
Models or columns without a `description:` in dbt YAML are skipped -- there is
nothing to embed. Only records whose description hash has changed since the
last sync are re-embedded (`catalog.description_sync_history` tracks this),
so an unchanged re-run is a fast no-op.

## Prerequisites

- PostgreSQL has been initialized with the SQL/role files in `postgres/init/`
  (`030`-`035`), which create the `catalog` schema and the `catalog_writer`
  role this module connects as.
- dbt has produced a `manifest.json` (`dbt compile` or `dbt run`) against the
  same database this syncs into.
- A repository-root `.env` file contains `CATALOG_WRITER_PASSWORD` from
  `.env.example`.

When running from the host against the provided Compose database, set
`POSTGRES_HOST=127.0.0.1` and `POSTGRES_PORT=5433` in `.env` (same as the
ingestion pipeline).

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r pipeline/catalog_sync/requirements.txt
```

Run it:

```bash
python -m pipeline.catalog_sync.sync --manifest-path dbt/target/manifest.json
```

Or let it use `$DBT_MANIFEST_PATH` / the in-container default
(`/app/dbt/target/manifest.json`, set via `pipeline-engine`'s environment in
`docker-compose.yml`) when run as the `catalog-sync` Prefect deployment.

## Table/column comments

`catalog_writer` is deliberately scoped to the `catalog` schema only (see
`postgres/init/035_catalog_writer_grants.sh`) and doesn't own the gold tables
dbt creates (`dbt_transformer` does), so this module never attempts
`COMMENT ON` -- routing it through a role that doesn't own those objects would
always fail on ownership grounds. Instead, `dbt_project.yml`'s
`marts: +persist_docs: {relation: true, columns: true}` has `dbt_transformer`
(which does own `marts_candidate` objects at build time) emit `COMMENT ON`
itself as a native part of `dbt build`. Postgres comments are keyed by object
ID, not schema, so they survive the later `ALTER TABLE ... SET SCHEMA gold`
promotion does -- confirmed live via `pg_description`/`obj_description()`
after promotion. The embeddings written to `catalog.dataset_embeddings` are
what MCP resources and tools actually read; `pg_description` is just for
plain-SQL clients poking around with `\d+` or `obj_description()`.

## Idempotency

Re-running the sync updates existing rows in place rather than duplicating
them -- upserts key on `(object_type, schema_name, table_name, column_name)`
for tables/columns. `catalog.catalog_version` bumps only on a run that
actually changed something, so the MCP server (or any other consumer) can use
it to detect real staleness rather than every sync looking like a change.

## Tests

```bash
python -m pytest pipeline/tests/catalog_sync
```

These use a fake `embed_fn` (see `sync_catalog`'s parameter) so they don't
require downloading the real embedding model.
