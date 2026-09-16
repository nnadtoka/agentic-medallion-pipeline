#!/bin/bash
set -euo pipefail

psql \
  -v ON_ERROR_STOP=1 \
  -v database_name="$POSTGRES_DB" \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" <<-'SQL'
  GRANT CONNECT ON DATABASE :"database_name" TO ingestion_writer;
  GRANT USAGE ON SCHEMA bronze TO ingestion_writer;
  GRANT SELECT, INSERT, UPDATE, DELETE
      ON ALL TABLES IN SCHEMA bronze
      TO ingestion_writer;

  ALTER DEFAULT PRIVILEGES IN SCHEMA bronze
      GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ingestion_writer;

  REVOKE ALL ON SCHEMA staging FROM ingestion_writer;
  REVOKE ALL ON SCHEMA gold FROM ingestion_writer;
  REVOKE ALL ON SCHEMA public FROM ingestion_writer;
  REVOKE ALL ON ALL TABLES IN SCHEMA staging FROM ingestion_writer;
  REVOKE ALL ON ALL TABLES IN SCHEMA gold FROM ingestion_writer;
SQL
