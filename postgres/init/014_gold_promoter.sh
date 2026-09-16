#!/bin/bash
set -euo pipefail

: "${GOLD_PROMOTER_PASSWORD:?GOLD_PROMOTER_PASSWORD must be set}"

psql \
  -v ON_ERROR_STOP=1 \
  -v gold_promoter_password="$GOLD_PROMOTER_PASSWORD" \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" <<-'SQL'
  SELECT format(
    'CREATE ROLE gold_promoter LOGIN PASSWORD %L',
    :'gold_promoter_password'
  )
  WHERE NOT EXISTS (
    SELECT 1
    FROM pg_roles
    WHERE rolname = 'gold_promoter'
  ) \gexec

  ALTER ROLE gold_promoter PASSWORD :'gold_promoter_password';
  ALTER ROLE gold_promoter NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
SQL
