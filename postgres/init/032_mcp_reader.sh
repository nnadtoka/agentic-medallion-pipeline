#!/bin/bash
set -euo pipefail

: "${MCP_READER_PASSWORD:?MCP_READER_PASSWORD must be set}"

psql \
  -v ON_ERROR_STOP=1 \
  -v mcp_reader_password="$MCP_READER_PASSWORD" \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" <<-'SQL'
  SELECT format(
    'CREATE ROLE mcp_reader LOGIN PASSWORD %L',
    :'mcp_reader_password'
  )
  WHERE NOT EXISTS (
    SELECT 1
    FROM pg_roles
    WHERE rolname = 'mcp_reader'
  ) \gexec

  ALTER ROLE mcp_reader PASSWORD :'mcp_reader_password';
  ALTER ROLE mcp_reader NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;

  -- Defense in depth for run_supported_query: even though template SQL is
  -- pre-approved, cap how long any statement on this role can run.
  ALTER ROLE mcp_reader SET statement_timeout = '5000';
SQL
