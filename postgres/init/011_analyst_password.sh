#!/bin/bash
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
  ALTER ROLE analyst_junior PASSWORD '${ANALYST_PASSWORD:-analyst_ro_local}';
SQL
