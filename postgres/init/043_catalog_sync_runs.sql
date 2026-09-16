-- ============================================================
-- Catalog sync control / audit table
--
-- Mirrors bronze.ingestion_batches (postgres/init/021_ingestion_batches.sql):
-- one row per physical execution of the catalog-sync job, written every
-- time it runs -- including when there was nothing to do. That case is
-- still a row, with status='success' and a 'skipped: ...' comment, not a
-- missing row -- the point is a complete audit trail of every invocation,
-- not just the ones that changed something.
-- ============================================================

SET search_path TO catalog;

CREATE TABLE IF NOT EXISTS catalog_sync_runs (

    -- Logical name of the job. Constant: 'catalog_sync'.
    pipeline_name    TEXT NOT NULL,

    -- What's being synced. Constant today ('gold_marts') since one run
    -- always processes the whole manifest in one pass, unlike ingestion's
    -- per-dataset batches -- kept for structural consistency with
    -- bronze.ingestion_batches' batch_id convention.
    dataset_name     TEXT NOT NULL,

    -- Unique identifier for this physical execution.
    -- Convention: <pipeline_name>_<dataset_name>_<unix_epoch_seconds_of_started_ts>
    batch_id         TEXT NOT NULL,

    -- Origin of the descriptions. Constant: 'dbt_manifest'.
    source           TEXT NOT NULL,

    -- Timestamp when this control record was created.
    created_ts       TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Timestamp when the sync process started.
    started_ts       TIMESTAMPTZ NOT NULL,

    -- Timestamp when the sync process completed. NULL while running.
    completed_ts     TIMESTAMPTZ,

    -- Total marts table/column descriptions read from the manifest.
    records_seen      INTEGER,

    -- How many of those had a description hash different from the last
    -- recorded one (or no prior record at all) and were re-embedded.
    records_changed   INTEGER,

    -- How many matched their last recorded hash and were skipped.
    records_unchanged INTEGER,

    -- Current state of the run.
    status           TEXT NOT NULL
                     CHECK (status IN ('running', 'success', 'failed')),

    -- Free-text outcome note, e.g. "skipped: no description changes
    -- detected" or "7 changed, 83 unchanged". NULL only while running.
    comments         TEXT,

    -- Error details when the run fails. NULL otherwise.
    error_message    TEXT,

    PRIMARY KEY (pipeline_name, batch_id)
);
