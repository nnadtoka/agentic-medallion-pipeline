-- ============================================================
-- Transformation execution control and source-to-target lineage
--
-- transformation_batches is operational: its lifecycle fields are updated
-- as a candidate build moves through validation and Gold promotion.
-- model_batch_control is append-only: each row states that one source
-- ingestion batch was selected as input to one model in one target batch.
-- ============================================================

SET search_path TO staging;

CREATE TABLE IF NOT EXISTS transformation_batches (
    -- Convention: <job_or_pipeline_prefix>_<unix_timestamp_seconds>
    batch_id          TEXT PRIMARY KEY,
    pipeline_name     TEXT NOT NULL,
    status            TEXT NOT NULL
                      CHECK (status IN (
                          'running',
                          'candidate_ready',
                          'quality_passed',
                          'validated',
                          'promoted',
                          'failed'
                      )),
    started_ts        TIMESTAMPTZ NOT NULL,
    completed_ts      TIMESTAMPTZ,
    promoted_ts       TIMESTAMPTZ,
    failed_step_name  TEXT,
    error_message     TEXT,
    created_ts        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- A transformation pipeline has one active database change-set at a time.
-- This also lets dbt models discover their batch without a CLI variable.
CREATE UNIQUE INDEX IF NOT EXISTS uq_transformation_batches_one_active
    ON staging.transformation_batches (pipeline_name)
    WHERE status IN ('running', 'candidate_ready', 'quality_passed');

CREATE TABLE IF NOT EXISTS transformation_step_runs (
    batch_id          TEXT NOT NULL,
    step_name         TEXT NOT NULL,
    step_type         TEXT NOT NULL
                      CHECK (step_type IN (
                          'dbt',
                          'great_expectations',
                          'promotion'
                      )),
    status            TEXT NOT NULL
                      CHECK (status IN ('running', 'success', 'failed')),
    started_ts        TIMESTAMPTZ NOT NULL,
    completed_ts      TIMESTAMPTZ,
    error_message     TEXT,
    failure_details   JSONB,
    created_ts        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (batch_id, step_name),

    FOREIGN KEY (batch_id)
        REFERENCES staging.transformation_batches (batch_id)
);

CREATE TABLE IF NOT EXISTS model_batch_control (
    model_name          TEXT NOT NULL,
    source_dataset_name TEXT NOT NULL,
    source_batch_id     TEXT NOT NULL,
    target_batch_id     TEXT NOT NULL,
    created_ts          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (
        model_name,
        source_dataset_name,
        source_batch_id,
        target_batch_id
    ),

    FOREIGN KEY (target_batch_id)
        REFERENCES staging.transformation_batches (batch_id)
);

CREATE INDEX IF NOT EXISTS idx_model_batch_control_target
    ON staging.model_batch_control (target_batch_id);

CREATE INDEX IF NOT EXISTS idx_model_batch_control_source_lookup
    ON staging.model_batch_control (
        model_name,
        source_dataset_name,
        source_batch_id
    );

CREATE INDEX IF NOT EXISTS idx_transformation_step_runs_status
    ON staging.transformation_step_runs (
        batch_id,
        status
    );

COMMENT ON TABLE staging.transformation_batches IS
    'Operational lifecycle of each dbt candidate-build and Gold-promotion batch.';

COMMENT ON TABLE staging.model_batch_control IS
    'Append-only lineage mapping source ingestion batches to model target batches.';

COMMENT ON TABLE staging.transformation_step_runs IS
    'Per-gate dbt, Great Expectations, and Gold-promotion outcomes with structured failure details.';
