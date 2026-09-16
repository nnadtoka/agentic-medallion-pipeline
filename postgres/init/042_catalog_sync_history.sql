-- ============================================================
-- catalog.description_sync_history
--
-- Append-only control table for catalog_sync. One row per record per run
-- in which its description's sha256 actually changed (including the very
-- first time it's ever seen -- an empty table means "nothing to compare
-- against yet", so everything counts as changed on the first sync).
--
-- Lets the sync detect "nothing changed since last time" and skip
-- re-embedding/re-upserting/bumping catalog_version for unchanged
-- descriptions, and gives a queryable audit trail of when each dataset's
-- description last changed and at which catalog_version.
-- ============================================================

SET search_path TO catalog, public;

CREATE TABLE IF NOT EXISTS description_sync_history (
    id                BIGSERIAL PRIMARY KEY,
    object_type       TEXT NOT NULL CHECK (object_type IN ('table', 'column', 'query_template')),
    schema_name       TEXT,
    table_name        TEXT,
    column_name       TEXT,
    template_id       TEXT,
    description_hash  TEXT NOT NULL,
    catalog_version   BIGINT NOT NULL,
    created_ts        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Supports "what's the latest hash for this key" lookups (DISTINCT ON ...
-- ORDER BY ... created_ts DESC) without a sequential scan as history grows.
CREATE INDEX IF NOT EXISTS description_sync_history_key_ts_idx
    ON description_sync_history (object_type, schema_name, table_name, column_name, created_ts DESC);
