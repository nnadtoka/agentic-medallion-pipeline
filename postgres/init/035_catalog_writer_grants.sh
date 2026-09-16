#!/bin/bash
set -euo pipefail

psql \
  -v ON_ERROR_STOP=1 \
  -v database_name="$POSTGRES_DB" \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" <<-'SQL'
  GRANT CONNECT ON DATABASE :"database_name" TO catalog_writer;

  -- Writes dbt-sourced table/column descriptions and their embeddings.
  -- Does not need gold read access: descriptions come from dbt's
  -- manifest.json on disk, not from querying gold tables directly.
  GRANT USAGE ON SCHEMA catalog TO catalog_writer;
  GRANT SELECT, INSERT, UPDATE, DELETE
      ON ALL TABLES IN SCHEMA catalog
      TO catalog_writer;
  ALTER DEFAULT PRIVILEGES IN SCHEMA catalog
      GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO catalog_writer;

  -- BIGSERIAL id columns (dataset_embeddings) are backed by sequences,
  -- which are separate objects from their table -- INSERT on the table
  -- does not imply USAGE on the sequence nextval() needs.
  GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA catalog TO catalog_writer;
  ALTER DEFAULT PRIVILEGES IN SCHEMA catalog
      GRANT USAGE, SELECT ON SEQUENCES TO catalog_writer;

  -- Defensive: no bronze/staging/gold access.
  REVOKE ALL ON SCHEMA bronze FROM catalog_writer;
  REVOKE ALL ON SCHEMA staging FROM catalog_writer;
  REVOKE ALL ON SCHEMA gold FROM catalog_writer;
  REVOKE ALL ON ALL TABLES IN SCHEMA bronze FROM catalog_writer;
  REVOKE ALL ON ALL TABLES IN SCHEMA staging FROM catalog_writer;
  REVOKE ALL ON ALL TABLES IN SCHEMA gold FROM catalog_writer;
SQL
