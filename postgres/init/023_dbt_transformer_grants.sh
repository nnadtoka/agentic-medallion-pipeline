#!/bin/bash
set -euo pipefail

psql \
  -v ON_ERROR_STOP=1 \
  -v database_name="$POSTGRES_DB" \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" <<-'SQL'
  GRANT CONNECT ON DATABASE :"database_name" TO dbt_transformer;

  GRANT USAGE ON SCHEMA bronze TO dbt_transformer;
  GRANT SELECT ON ALL TABLES IN SCHEMA bronze TO dbt_transformer;
  ALTER DEFAULT PRIVILEGES IN SCHEMA bronze
      GRANT SELECT ON TABLES TO dbt_transformer;

  GRANT USAGE, CREATE ON SCHEMA staging TO dbt_transformer;
  GRANT USAGE, CREATE ON SCHEMA marts_candidate TO dbt_transformer;

  REVOKE ALL ON SCHEMA gold FROM dbt_transformer;
  REVOKE ALL ON SCHEMA public FROM dbt_transformer;
  REVOKE ALL ON ALL TABLES IN SCHEMA gold FROM dbt_transformer;
SQL
