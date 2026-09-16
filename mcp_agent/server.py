"""MCP server exposing the gold semantic catalog to agents: resources
(cheap, keyed reads), `search_semantic_catalog` (embedding-based lookup),
and `run_supported_query` (governed execution of a pre-approved,
parameterized template from `catalog.query_templates`) -- see
mcp_agent/README.md for the full design.

Runs as the least-privilege `mcp_reader` role (postgres/init/032,033).
Two deployment modes, same code:

- stdio (default): spawned directly by an MCP client (e.g. Claude Code) as
  a local subprocess on the host, talking to the dockerized Postgres over
  its host-exposed port. Useful for quick local testing.
- http (`MCP_TRANSPORT=http`): how `docker/mcp-server` actually runs it --
  a persistent service in docker-compose, reached over the network instead
  of spawned per-session. See mcp_agent/README.md.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Literal

from dotenv import load_dotenv
from fastmcp import FastMCP
from pgvector import Vector
from pgvector.psycopg2 import register_vector
from psycopg2.pool import ThreadedConnectionPool

from pipeline.catalog_sync.embedding import embed_texts

from mcp_agent.query_templates import QueryTemplateError, resolve_query

# Loaded explicitly from the repo root, not the process's CWD -- an MCP
# client may launch this from anywhere. A no-op in the Docker deployment
# (no .env file in the image; real values come from the container's
# environment instead), which is why every os.environ[...] below has to
# tolerate that -- see get_connection().
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

mcp = FastMCP("gold-catalog")

_pool: ThreadedConnectionPool | None = None


def _get_pool() -> ThreadedConnectionPool:
    """Lazily create the connection pool.

    A persistent service shouldn't open/close a fresh Postgres connection
    (full TCP handshake + auth) on every single tool/resource call the way
    a one-shot CLI script reasonably would -- pooled here instead, sized
    small since this is a low-traffic, single-consumer toy deployment.
    """

    global _pool
    if _pool is None:
        _pool = ThreadedConnectionPool(
            minconn=1,
            maxconn=int(os.getenv("MCP_DB_POOL_MAX", "5")),
            host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
            port=os.getenv("POSTGRES_PORT", "5433"),
            dbname=os.environ["POSTGRES_DB"],
            user=os.getenv("MCP_READER_USER", "mcp_reader"),
            password=os.environ["MCP_READER_PASSWORD"],
        )
    return _pool


@contextmanager
def get_connection():
    """Check out a pooled read-only connection as `mcp_reader`."""

    pool = _get_pool()
    connection = pool.getconn()
    try:
        register_vector(connection)  # idempotent; connections may be reused
        yield connection
    finally:
        pool.putconn(connection)


@mcp.resource("catalog://gold")
def list_gold_datasets() -> list[dict]:
    """List every gold table with a description, cheap and cacheable."""

    with get_connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT table_name, description
            FROM catalog.dataset_embeddings
            WHERE object_type = 'table'
            ORDER BY table_name
            """
        )
        return [
            {"table": row[0], "description": row[1], "uri": f"catalog://gold/{row[0]}"}
            for row in cursor.fetchall()
        ]


@mcp.resource("catalog://gold/{table}")
def get_gold_table(table: str) -> dict:
    """Table description plus every column's description, in one read."""

    with get_connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT description
            FROM catalog.dataset_embeddings
            WHERE object_type = 'table' AND table_name = %s
            """,
            (table,),
        )
        table_row = cursor.fetchone()
        if table_row is None:
            return {"error": f"No gold table named {table!r} in the catalog."}

        cursor.execute(
            """
            SELECT column_name, description
            FROM catalog.dataset_embeddings
            WHERE object_type = 'column' AND table_name = %s
            ORDER BY column_name
            """,
            (table,),
        )
        columns = [
            {"column": row[0], "description": row[1]}
            for row in cursor.fetchall()
        ]

    return {"table": table, "description": table_row[0], "columns": columns}


@mcp.resource("catalog://query-templates")
def list_query_templates() -> list[dict]:
    """List supported query templates (seeded via pipeline/catalog_sync/seed_templates.py)."""

    with get_connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT template_id, name, description FROM catalog.query_templates ORDER BY template_id"
        )
        return [
            {
                "template_id": row[0],
                "name": row[1],
                "description": row[2],
                "uri": f"catalog://query-templates/{row[0]}",
            }
            for row in cursor.fetchall()
        ]


@mcp.resource("catalog://query-templates/{template_id}")
def get_query_template(template_id: str) -> dict:
    """One template's description and parameter spec."""

    with get_connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT name, description, parameters, max_limit
            FROM catalog.query_templates
            WHERE template_id = %s
            """,
            (template_id,),
        )
        row = cursor.fetchone()

    if row is None:
        return {"error": f"No query template named {template_id!r}."}
    return {
        "template_id": template_id,
        "name": row[0],
        "description": row[1],
        "parameters": row[2],
        "max_limit": row[3],
    }


ObjectType = Literal["table", "column", "query_template"]


@mcp.tool()
def search_semantic_catalog(
    query: str, top_k: int = 5, object_type: ObjectType | None = None
) -> list[dict]:
    """Semantically search the gold catalog for tables/columns matching a question.

    Embeds `query` with the same local model catalog_sync uses, and
    cosine-searches `catalog.dataset_embeddings`. Returns resource URIs and
    similarity scores, not full description text -- read the matched
    resource (e.g. via `catalog://gold/{table}`) for detail.
    """

    if top_k < 1 or top_k > 50:
        raise ValueError("top_k must be between 1 and 50")
    if object_type not in (None, "table", "column", "query_template"):
        raise ValueError("object_type must be one of: table, column, query_template")

    query_vector = Vector(embed_texts([query])[0])

    with get_connection() as connection, connection.cursor() as cursor:
        where_clause = "WHERE object_type = %s" if object_type else ""
        params: tuple = (object_type,) if object_type else ()
        cursor.execute(
            f"""
            SELECT object_type, schema_name, table_name, column_name, template_id,
                   description, 1 - (embedding <=> %s) AS similarity
            FROM catalog.dataset_embeddings
            {where_clause}
            ORDER BY embedding <=> %s
            LIMIT %s
            """,
            (query_vector, *params, query_vector, top_k),
        )
        rows = cursor.fetchall()

    results = []
    for object_type_, schema_name, table_name, column_name, template_id, description, similarity in rows:
        if object_type_ == "query_template":
            uri = f"catalog://query-templates/{template_id}"
        else:
            uri = f"catalog://gold/{table_name}"
        results.append({
            "object_type": object_type_,
            "table": table_name,
            "column": column_name,
            "description": description,
            "similarity": round(float(similarity), 4),
            "uri": uri,
        })
    return results


@mcp.tool()
def run_supported_query(
    template_id: str, params: dict[str, object] | None = None
) -> list[dict] | dict:
    """Execute a pre-approved, parameterized query template by ID.

    This is the fallback for "here's a known-good answer" once
    `search_semantic_catalog`/`catalog://query-templates` has identified
    which template fits the question -- not a general SQL-execution tool.
    `params` are validated against the template's own stored spec before
    anything runs: `value`-kind params are bound as ordinary SQL parameters,
    `identifier`-kind params may only select from a fixed enum baked into
    the template (never raw agent text), and any `limit` param is capped at
    the template's `max_limit` regardless of what's requested. Runs as
    `mcp_reader`, which also has a 5s `statement_timeout` set at the role
    level as defense in depth.
    """

    params = params or {}
    with get_connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT sql_template, parameters, max_limit
            FROM catalog.query_templates
            WHERE template_id = %s
            """,
            (template_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return {"error": f"No supported query template named {template_id!r}."}
        sql_template, parameter_specs, max_limit = row

        try:
            sql, bind_params = resolve_query(sql_template, parameter_specs, params, max_limit)
        except QueryTemplateError as exc:
            return {"error": str(exc)}

        cursor.execute(sql, bind_params)
        columns = [column.name for column in cursor.description]
        rows = cursor.fetchall()

    return [dict(zip(columns, row)) for row in rows]


if __name__ == "__main__":
    transport = os.getenv("MCP_TRANSPORT", "stdio")
    if transport == "stdio":
        mcp.run()
    else:
        mcp.run(
            transport=transport,
            host=os.getenv("MCP_HTTP_HOST", "0.0.0.0"),
            port=int(os.getenv("MCP_HTTP_PORT", "8000")),
        )
