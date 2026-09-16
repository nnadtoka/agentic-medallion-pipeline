#!/bin/bash
set -euo pipefail

psql \
  -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" <<-'SQL'
  -- USAGE only, to resolve/call the function by name -- everything the
  -- function itself needs is covered by SECURITY DEFINER running as its
  -- owner, not by granting gold_promoter anything on the tables inside.
  GRANT USAGE ON SCHEMA gold TO gold_promoter;
  GRANT EXECUTE ON FUNCTION gold.promote_marts_to_gold(text) TO gold_promoter;
SQL
