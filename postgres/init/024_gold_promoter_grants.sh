#!/bin/bash
set -euo pipefail

psql \
  -v ON_ERROR_STOP=1 \
  -v database_name="$POSTGRES_DB" \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" <<-'SQL'
  GRANT CONNECT ON DATABASE :"database_name" TO gold_promoter;

  REVOKE ALL ON SCHEMA bronze FROM gold_promoter;
  REVOKE ALL ON SCHEMA staging FROM gold_promoter;
  REVOKE ALL ON SCHEMA gold FROM gold_promoter;
  REVOKE ALL ON SCHEMA public FROM gold_promoter;
  REVOKE ALL ON ALL TABLES IN SCHEMA bronze FROM gold_promoter;
  REVOKE ALL ON ALL TABLES IN SCHEMA staging FROM gold_promoter;
  REVOKE ALL ON ALL TABLES IN SCHEMA gold FROM gold_promoter;

  -- The promotion-function init script will grant EXECUTE on only
  -- the allowlisted SECURITY DEFINER function when it is introduced.
SQL
