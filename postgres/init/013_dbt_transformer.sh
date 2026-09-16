#!/bin/bash
set -euo pipefail

: "${DBT_PASSWORD:?DBT_PASSWORD must be set}"

psql \
  -v ON_ERROR_STOP=1 \
  -v dbt_password="$DBT_PASSWORD" \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" <<-'SQL'
  SELECT format(
    'CREATE ROLE dbt_transformer LOGIN PASSWORD %L',
    :'dbt_password'
  )
  WHERE NOT EXISTS (
    SELECT 1
    FROM pg_roles
    WHERE rolname = 'dbt_transformer'
  ) \gexec

  ALTER ROLE dbt_transformer PASSWORD :'dbt_password';
  ALTER ROLE dbt_transformer NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
SQL
