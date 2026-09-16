#!/bin/bash
set -euo pipefail

psql \
  -v ON_ERROR_STOP=1 \
  -v database_name="$POSTGRES_DB" \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" <<-'SQL'
  GRANT CONNECT ON DATABASE :"database_name" TO mcp_reader;

  -- Catalog metadata: descriptions, embeddings, query templates. One service
  -- role covers both resource reads (descriptions) and the search tool
  -- (needs the embedding column) -- the resource/tool split is enforced by
  -- the MCP server's query projection (it omits `embedding` for resource
  -- reads), not by column-level grants here.
  GRANT USAGE ON SCHEMA catalog TO mcp_reader;
  GRANT SELECT ON ALL TABLES IN SCHEMA catalog TO mcp_reader;
  ALTER DEFAULT PRIVILEGES IN SCHEMA catalog GRANT SELECT ON TABLES TO mcp_reader;

  -- Gold layer: same read-only footprint as analyst_junior, for
  -- run_supported_query. Does NOT cover SET SCHEMA swaps -- the blue-green
  -- gold promotion task handles that, same caveat as analyst_junior.
  GRANT USAGE ON SCHEMA gold TO mcp_reader;
  GRANT SELECT ON ALL TABLES IN SCHEMA gold TO mcp_reader;
  ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO mcp_reader;

  -- Defensive: no bronze/staging access.
  REVOKE ALL ON SCHEMA staging FROM mcp_reader;
  REVOKE ALL ON SCHEMA bronze FROM mcp_reader;
  REVOKE ALL ON ALL TABLES IN SCHEMA staging FROM mcp_reader;
  REVOKE ALL ON ALL TABLES IN SCHEMA bronze FROM mcp_reader;
SQL
