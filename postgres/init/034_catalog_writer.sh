#!/bin/bash
set -euo pipefail

: "${CATALOG_WRITER_PASSWORD:?CATALOG_WRITER_PASSWORD must be set}"

psql \
  -v ON_ERROR_STOP=1 \
  -v catalog_writer_password="$CATALOG_WRITER_PASSWORD" \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" <<-'SQL'
  SELECT format(
    'CREATE ROLE catalog_writer LOGIN PASSWORD %L',
    :'catalog_writer_password'
  )
  WHERE NOT EXISTS (
    SELECT 1
    FROM pg_roles
    WHERE rolname = 'catalog_writer'
  ) \gexec

  ALTER ROLE catalog_writer PASSWORD :'catalog_writer_password';
  ALTER ROLE catalog_writer NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
SQL
