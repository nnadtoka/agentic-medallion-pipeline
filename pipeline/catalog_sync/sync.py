"""Sync dbt gold-layer descriptions into `catalog.dataset_embeddings`.

Runs as the least-privilege `catalog_writer` role (postgres/init/034,035),
same pattern as `ingestion_writer` in pipeline/ingestion/ingest_olist.py.

Two control mechanisms, both append-only:

- `catalog.catalog_sync_runs` (postgres/init/043) -- one row per physical
  invocation of this job, written every time it runs, including when there
  was nothing to do (status='success', comments='skipped: ...'). Mirrors
  `bronze.ingestion_batches`' create-then-update lifecycle: a 'running' row
  is inserted first, then updated to 'success'/'failed'. On failure the
  original transaction (including that insert) rolls back, so the except
  branch re-creates the row from scratch in a fresh transaction before
  marking it failed -- same reason ingest_olist.py's except block does.
  batch_id follows the same `<pipeline_name>_<dataset_name>_<...>`
  convention as ingestion, using real Unix epoch seconds of `started_ts`.

- `catalog.description_sync_history` (postgres/init/042) -- one row per
  *record* per run in which its description's sha256 actually changed.
  Before embedding anything, the sync fetches the latest known hash per
  (object_type, schema_name, table_name, column_name) key and only
  embeds/upserts/records the ones whose hash differs (an empty history
  table -- the first-ever sync -- means every record counts as changed).
  Unchanged records cost nothing: no re-embedding, no upsert, no
  catalog_version bump, no new history row.

Table/column comments are not mirrored here anymore. dbt's own
`persist_docs` (dbt_project.yml's `marts:` config) has `dbt_transformer` --
the actual owner of `marts_candidate` objects -- run `COMMENT ON` at build
time, for every model/column that has a `description:`. Postgres comments
are keyed by OID, not schema, so they survive the later
`ALTER TABLE ... SET SCHEMA gold` promotion untouched. That sidesteps the
ownership problem entirely: `catalog_writer` never needs COMMENT privileges
on gold objects, because it was never the one applying the comments that
matter to `psql`'s `\\d+`/`pg_description` -- this module's job is solely to
keep `catalog.dataset_embeddings` (what MCP resources and tools read) in
sync with those same descriptions.
"""

import hashlib
import os
from datetime import datetime, timezone

import psycopg2
from dotenv import load_dotenv
from pgvector import Vector
from pgvector.psycopg2 import register_vector
from psycopg2.extras import execute_values

from pipeline.catalog_sync.embedding import embed_texts
from pipeline.catalog_sync.manifest import load_gold_descriptions

load_dotenv()

DEFAULT_MANIFEST_PATH = "dbt/target/manifest.json"

PIPELINE_NAME = "catalog_sync"
DATASET_NAME = "gold_marts"
SOURCE = "dbt_manifest"

UPSERT_SQL = """
    INSERT INTO catalog.dataset_embeddings
        (object_type, schema_name, table_name, column_name, description, embedding, source, updated_at)
    VALUES %s
    ON CONFLICT (object_type, schema_name, table_name, (coalesce(column_name, '')))
        WHERE object_type IN ('table', 'column')
    DO UPDATE SET
        description = EXCLUDED.description,
        embedding = EXCLUDED.embedding,
        source = EXCLUDED.source,
        updated_at = EXCLUDED.updated_at
"""

LATEST_HASHES_SQL = """
    SELECT DISTINCT ON (object_type, schema_name, table_name, column_name)
        object_type, schema_name, table_name, column_name, description_hash
    FROM catalog.description_sync_history
    ORDER BY object_type, schema_name, table_name, column_name, created_ts DESC
"""

HISTORY_INSERT_SQL = """
    INSERT INTO catalog.description_sync_history
        (object_type, schema_name, table_name, column_name, description_hash, catalog_version, created_ts)
    VALUES %s
"""


def get_connection():
    """Create a PostgreSQL connection as `catalog_writer` using .env values."""

    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.getenv("CATALOG_DB_USER", "catalog_writer"),
        password=os.environ["CATALOG_DB_PASSWORD"],
    )


def _generate_batch_id(started_ts) -> str:
    """<pipeline_name>_<dataset_name>_<unix_epoch_seconds>, per bronze.ingestion_batches' convention."""

    return f"{PIPELINE_NAME}_{DATASET_NAME}_{int(started_ts.timestamp())}"


def _create_run_record(cursor, batch_id, started_ts):
    cursor.execute(
        """
        INSERT INTO catalog.catalog_sync_runs (
            pipeline_name, dataset_name, batch_id, source, started_ts, status
        )
        VALUES (%s, %s, %s, %s, %s, 'running')
        """,
        (PIPELINE_NAME, DATASET_NAME, batch_id, SOURCE, started_ts),
    )


def _finish_run_success(
    cursor, batch_id, completed_ts, records_seen, records_changed, records_unchanged, comments
):
    cursor.execute(
        """
        UPDATE catalog.catalog_sync_runs
        SET completed_ts = %s,
            records_seen = %s,
            records_changed = %s,
            records_unchanged = %s,
            status = 'success',
            comments = %s,
            error_message = NULL
        WHERE pipeline_name = %s AND batch_id = %s
        """,
        (
            completed_ts,
            records_seen,
            records_changed,
            records_unchanged,
            comments,
            PIPELINE_NAME,
            batch_id,
        ),
    )


def _finish_run_failed(cursor, batch_id, completed_ts, error_message):
    cursor.execute(
        """
        UPDATE catalog.catalog_sync_runs
        SET completed_ts = %s, status = 'failed', error_message = %s
        WHERE pipeline_name = %s AND batch_id = %s
        """,
        (completed_ts, error_message[:4000], PIPELINE_NAME, batch_id),
    )


def _hash_description(description: str) -> str:
    """sha256 of a description's text, used to detect real content changes."""

    return hashlib.sha256(description.encode("utf-8")).hexdigest()


def _record_key(record: dict) -> tuple:
    return (
        record["object_type"],
        record["schema_name"],
        record["table_name"],
        record["column_name"],
    )


def _fetch_latest_hashes(cursor) -> dict[tuple, str]:
    """Return the most recent description_hash per record key.

    Empty on the first-ever sync -- every record then has no prior hash to
    compare against and counts as changed, which is the intended behavior.
    """

    cursor.execute(LATEST_HASHES_SQL)
    return {
        (row[0], row[1], row[2], row[3]): row[4]
        for row in cursor.fetchall()
    }


def sync_catalog(manifest_path=None, embed_fn=None, logger=None):
    """Read dbt's manifest, embed and upsert only descriptions that changed.

    Writes exactly one row to `catalog.catalog_sync_runs` per call,
    regardless of outcome -- including a 'skipped' row when there was
    nothing to sync. `embed_fn` defaults to the real local embedding model
    but can be overridden (e.g. in tests) with any
    `list[str] -> list[list[float]]` callable so this can run without
    downloading a model.
    """

    manifest_path = manifest_path or os.getenv(
        "DBT_MANIFEST_PATH", DEFAULT_MANIFEST_PATH
    )
    embed_fn = embed_fn or embed_texts

    started_ts = datetime.now(timezone.utc)
    batch_id = _generate_batch_id(started_ts)

    connection = get_connection()
    register_vector(connection)
    try:
        try:
            with connection:
                with connection.cursor() as cursor:
                    _create_run_record(cursor, batch_id, started_ts)

                    records = load_gold_descriptions(manifest_path)
                    if not records:
                        _finish_run_success(
                            cursor,
                            batch_id,
                            datetime.now(timezone.utc),
                            records_seen=0,
                            records_changed=0,
                            records_unchanged=0,
                            comments="skipped: no marts descriptions found in manifest",
                        )
                        return {
                            "tables": 0,
                            "columns": 0,
                            "unchanged": 0,
                            "batch_id": batch_id,
                        }

                    for record in records:
                        record["description_hash"] = _hash_description(
                            record["description"]
                        )

                    latest_hashes = _fetch_latest_hashes(cursor)
                    changed_records = [
                        record
                        for record in records
                        if latest_hashes.get(_record_key(record))
                        != record["description_hash"]
                    ]
                    unchanged_count = len(records) - len(changed_records)

                    if not changed_records:
                        _finish_run_success(
                            cursor,
                            batch_id,
                            datetime.now(timezone.utc),
                            records_seen=len(records),
                            records_changed=0,
                            records_unchanged=unchanged_count,
                            comments="skipped: no description changes detected",
                        )
                        return {
                            "tables": 0,
                            "columns": 0,
                            "unchanged": unchanged_count,
                            "batch_id": batch_id,
                        }

                    descriptions = [
                        record["description"] for record in changed_records
                    ]
                    embeddings = embed_fn(descriptions)
                    if len(embeddings) != len(changed_records):
                        raise ValueError(
                            "embed_fn returned "
                            f"{len(embeddings)} vectors for {len(changed_records)} "
                            "changed descriptions"
                        )

                    synced_at = datetime.now(timezone.utc)
                    rows = [
                        (
                            record["object_type"],
                            record["schema_name"],
                            record["table_name"],
                            record["column_name"],
                            record["description"],
                            Vector(embedding),
                            "dbt_yml",
                            synced_at,
                        )
                        for record, embedding in zip(changed_records, embeddings)
                    ]
                    execute_values(cursor, UPSERT_SQL, rows)

                    cursor.execute(
                        """
                        UPDATE catalog.catalog_version
                        SET version = version + 1, updated_at = now()
                        WHERE id
                        RETURNING version
                        """
                    )
                    new_version = cursor.fetchone()[0]

                    history_rows = [
                        (
                            record["object_type"],
                            record["schema_name"],
                            record["table_name"],
                            record["column_name"],
                            record["description_hash"],
                            new_version,
                            synced_at,
                        )
                        for record in changed_records
                    ]
                    execute_values(cursor, HISTORY_INSERT_SQL, history_rows)

                    _finish_run_success(
                        cursor,
                        batch_id,
                        synced_at,
                        records_seen=len(records),
                        records_changed=len(changed_records),
                        records_unchanged=unchanged_count,
                        comments=(
                            f"{len(changed_records)} changed, {unchanged_count} "
                            f"unchanged (catalog_version={new_version})"
                        ),
                    )
        except Exception as exc:
            # The transaction above -- including its _create_run_record insert --
            # rolled back, so the row needs to be created again from scratch
            # before it can be marked failed. Same reason ingest_olist.py's
            # except block re-calls create_batch() before update_batch_failed().
            message = f"Catalog sync failed (batch_id={batch_id}): {exc}"
            if logger:
                logger.error(message)
            else:
                print(message)
            with connection:
                with connection.cursor() as cursor:
                    _create_run_record(cursor, batch_id, started_ts)
                    _finish_run_failed(
                        cursor, batch_id, datetime.now(timezone.utc), str(exc)
                    )
            raise
    finally:
        connection.close()

    return {
        "tables": sum(1 for r in changed_records if r["object_type"] == "table"),
        "columns": sum(1 for r in changed_records if r["object_type"] == "column"),
        "unchanged": unchanged_count,
        "batch_id": batch_id,
    }


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Sync dbt gold-layer descriptions into the pgvector catalog."
    )
    parser.add_argument(
        "--manifest-path",
        help=(
            "Path to dbt's manifest.json. Defaults to $DBT_MANIFEST_PATH, "
            f"or {DEFAULT_MANIFEST_PATH!r}."
        ),
    )
    args = parser.parse_args()

    result = sync_catalog(manifest_path=args.manifest_path)
    print(f"Batch ID:        {result['batch_id']}")
    print(f"Tables changed:  {result['tables']}")
    print(f"Columns changed: {result['columns']}")
    print(f"Unchanged:       {result['unchanged']}")


if __name__ == "__main__":
    main()
