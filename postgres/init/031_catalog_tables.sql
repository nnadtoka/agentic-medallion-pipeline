-- ============================================================
-- Catalog schema: semantic-layer metadata store
-- See mcp_agent/README.md for the design this supports.
--
-- Embedding dimension is fixed at 384 (sentence-transformers/all-MiniLM-L6-v2,
-- the toy local embedding model chosen in the plan). Changing models later
-- means dropping and recreating the `embedding` column.
-- ============================================================

-- `public` stays in the path so the `vector` type (installed into the default
-- schema by 030_catalog_schema.sql) resolves without qualifying it.
SET search_path TO catalog, public;


-- ============================================================
-- dataset_embeddings
-- One row per described object: a table, a column, or a query template.
-- object_type determines which of schema_name/table_name/column_name/
-- template_id are populated -- enforced below so the table can't drift
-- into a half-filled row.
-- ============================================================

CREATE TABLE IF NOT EXISTS dataset_embeddings (
    id            BIGSERIAL PRIMARY KEY,
    object_type   TEXT NOT NULL CHECK (object_type IN ('table', 'column', 'query_template')),
    schema_name   TEXT,
    table_name    TEXT,
    column_name   TEXT,
    template_id   TEXT,
    description   TEXT NOT NULL,
    embedding     VECTOR(384) NOT NULL,
    source        TEXT NOT NULL DEFAULT 'manual' CHECK (source IN ('dbt_yml', 'manual')),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    CHECK (
        (object_type = 'table'
            AND schema_name IS NOT NULL AND table_name IS NOT NULL
            AND column_name IS NULL AND template_id IS NULL)
        OR (object_type = 'column'
            AND schema_name IS NOT NULL AND table_name IS NOT NULL AND column_name IS NOT NULL
            AND template_id IS NULL)
        OR (object_type = 'query_template'
            AND template_id IS NOT NULL
            AND schema_name IS NULL AND table_name IS NULL AND column_name IS NULL)
    )
);

-- Note: Postgres treats NULLs as distinct in UNIQUE indexes, and column_name
-- is always NULL for 'table' rows -- so a plain (object_type, schema_name,
-- table_name, column_name) index would never catch a re-synced table-level
-- description as a duplicate. Indexing coalesce(column_name, '') instead
-- gives 'table' rows a real, comparable key. query_template rows are
-- de-duplicated via the FK below pointing at query_templates' primary key.
DROP INDEX IF EXISTS dataset_embeddings_table_column_key;
CREATE UNIQUE INDEX IF NOT EXISTS dataset_embeddings_table_column_key
    ON dataset_embeddings (object_type, schema_name, table_name, (coalesce(column_name, '')))
    WHERE object_type IN ('table', 'column');


-- ============================================================
-- query_templates
-- Pre-approved, parameterized SQL templates. sql_template uses named
-- placeholders bound at execution time; `parameters` documents each
-- placeholder's kind (value | identifier), type, and allowed range/enum --
-- the MCP server validates against this before ever building a query.
-- ============================================================

CREATE TABLE IF NOT EXISTS query_templates (
    template_id   TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    description   TEXT NOT NULL,
    sql_template  TEXT NOT NULL,
    parameters    JSONB NOT NULL DEFAULT '[]'::jsonb,
    target_schema TEXT NOT NULL DEFAULT 'gold',
    max_limit     INTEGER NOT NULL DEFAULT 100,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'dataset_embeddings_template_id_fkey'
    ) THEN
        ALTER TABLE dataset_embeddings
            ADD CONSTRAINT dataset_embeddings_template_id_fkey
            FOREIGN KEY (template_id) REFERENCES query_templates (template_id)
            ON DELETE CASCADE;
    END IF;
END
$$;


-- ============================================================
-- catalog_version
-- Singleton row bumped by the sync job after each gold promotion, so the
-- MCP server can detect staleness and fire a resources/list_changed
-- notification instead of clients polling on a timer.
-- ============================================================

CREATE TABLE IF NOT EXISTS catalog_version (
    id         BOOLEAN PRIMARY KEY DEFAULT true CHECK (id),
    version    BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO catalog_version (id, version)
VALUES (true, 0)
ON CONFLICT (id) DO NOTHING;
