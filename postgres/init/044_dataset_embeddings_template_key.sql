-- ============================================================
-- Uniqueness for query_template rows in dataset_embeddings.
--
-- 031_catalog_tables.sql's unique index only covers object_type IN
-- ('table', 'column') -- query_template rows had no equivalent, so
-- re-seeding the same template_id would insert a duplicate embedding row
-- instead of updating the existing one. This partial index gives
-- pipeline/catalog_sync/seed_templates.py an ON CONFLICT (template_id)
-- target to upsert against, the same pattern the table/column path uses.
-- ============================================================

CREATE UNIQUE INDEX IF NOT EXISTS dataset_embeddings_template_key
    ON catalog.dataset_embeddings (template_id)
    WHERE object_type = 'query_template';
