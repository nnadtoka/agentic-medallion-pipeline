#!/bin/bash
set -euo pipefail

: "${INGESTION_PASSWORD:?INGESTION_PASSWORD must be set}"

psql \
  -v ON_ERROR_STOP=1 \
  -v ingestion_password="$INGESTION_PASSWORD" \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" <<-'SQL'
  SELECT format(
    'CREATE ROLE ingestion_writer LOGIN PASSWORD %L',
    :'ingestion_password'
  )
  WHERE NOT EXISTS (
    SELECT 1
    FROM pg_roles
    WHERE rolname = 'ingestion_writer'
  ) \gexec

  ALTER ROLE ingestion_writer PASSWORD :'ingestion_password';
  ALTER ROLE ingestion_writer NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
SQL
