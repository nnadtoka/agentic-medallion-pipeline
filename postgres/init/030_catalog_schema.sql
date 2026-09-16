-- 030_catalog_schema.sql
-- Enables pgvector and creates the `catalog` schema: the semantic-layer metadata
-- store for table/column/query-template descriptions and their embeddings.
-- See mcp_agent/README.md for the design this supports.

CREATE EXTENSION IF NOT EXISTS vector;

CREATE SCHEMA IF NOT EXISTS catalog;
ALTER SCHEMA catalog OWNER TO admin_user;
