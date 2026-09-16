-- ============================================================
-- Bronze ingestion control / audit table
--
-- One row represents one physical ingestion execution. 
-- This table intentionally keeps execution history:
-- - normal incremental runs
-- - retries 
-- - explicit backfills 
-- Idempotency is enforced by the ingestion application logic
-- ============================================================

SET search_path TO bronze;

CREATE TABLE IF NOT EXISTS ingestion_batches (

    -- Logical name of the pipeline responsible for ingestion.
    -- Example: 'olist_ingestion'
    pipeline_name   TEXT NOT NULL,

    -- Name of the source dataset being ingested.
    -- Example: 'orders', 'customers', 'order_items'
    dataset_name    TEXT NOT NULL,

    -- Unique identifier for this particular physical execution.
    -- Convention: <pipeline_name>_<dataset_name>_<unix_timestamp>
    batch_id        TEXT NOT NULL,

    -- Business/source date represented by this batch.
    -- For orders, this corresponds to order_purchase_timestamp.
    batch_date      DATE,

    -- Origin of the data.
    -- Example: 'olist'
    source          TEXT NOT NULL,

    -- How this batch was initiated.
    -- 'incremental' = normal processing of a new batch/date.
    -- 'full_refresh' = normal replacement of a whole snapshot.
    -- 'backfill'     = explicit reprocessing using --force.
    run_type        TEXT NOT NULL
                    CHECK (
                        run_type IN (
                            'incremental',
                            'full_refresh',
                            'backfill'
                        )
                    ),

    -- Timestamp when this control record was created.
    -- PostgreSQL sets this automatically.
    created_ts      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Timestamp when the ingestion process started.
    started_ts      TIMESTAMPTZ NOT NULL,

    -- Timestamp when the ingestion process completed.
    -- NULL while the batch is still running.
    completed_ts    TIMESTAMPTZ,

    -- Number of records read from the source file/API.
    rows_read       INTEGER,

    -- Number of records successfully written to the Bronze table.
    rows_loaded     INTEGER,

    -- Current state of the ingestion batch.
    status          TEXT NOT NULL
                    CHECK (status IN ('running', 'success', 'failed')),

    -- Error details when the batch fails.
    -- NULL for successful or currently running batches.
    error_message   TEXT,

    -- Every physical execution gets its own batch ID.
    PRIMARY KEY (pipeline_name, batch_id)

    -- Logical batches may have multiple physical execution rows.
    -- The ingestion application skips a previously successful
    -- logical batch unless --force is supplied. Audit rows are
    -- append-only apart from their operational completion fields.
);

-- ============================================================
-- Index used by the ingestion application when checking
-- whether a logical pipeline/dataset/date has already been
-- successfully processed.
--
-- Multiple executions for the same date are intentionally
-- allowed because failed attempts and backfills remain part of
-- the execution history.
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_ingestion_batches_logical_batch
    ON bronze.ingestion_batches (
        pipeline_name,
        dataset_name,
        batch_date,
        status
    );
