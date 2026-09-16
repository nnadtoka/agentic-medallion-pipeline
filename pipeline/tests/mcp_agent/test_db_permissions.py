"""Verify the `mcp_reader` role's actual privilege boundary is enforced by
Postgres -- not just declared in `postgres/init/032_mcp_reader.sh`/
`033_mcp_reader_grants.sh` and never checked again.

This is the one property everything else in this project's MCP design
depends on: `run_supported_query` is "safe" specifically because it always
executes as this least-privilege, read-only role. Every other guardrail
(parameter validation, identifier-enum resolution) still assumes this
boundary holds. Nothing anywhere in this repo's test suite checked that
before this file -- confirmed by grep before writing it.

Connects directly to Postgres as `mcp_reader`, the same credentials
`mcp_agent/server.py`'s `get_connection()` uses -- this is deliberately
not going through the MCP protocol/mcp-server at all, since the property
under test is a Postgres-level guarantee, not an MCP-level one. Skips
cleanly if Postgres isn't reachable (same reasoning as the live MCP tests
elsewhere in this directory, just a plain DB connection instead of an MCP
client).
"""

import os

import psycopg2
import pytest
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture(scope="module")
def mcp_reader_connection():
    try:
        connection = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5433"),
            dbname=os.environ["POSTGRES_DB"],
            user=os.getenv("MCP_READER_USER", "mcp_reader"),
            password=os.environ["MCP_READER_PASSWORD"],
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"Postgres not reachable as mcp_reader ({exc}) -- run `docker compose up -d postgres`")

    # Autocommit: each check below is an independent probe, several of
    # which are *expected* to fail -- without autocommit, one failed
    # statement would abort the whole transaction and poison every
    # subsequent query on this connection.
    connection.autocommit = True
    yield connection
    connection.close()


def _assert_permission_denied(cursor, sql):
    with pytest.raises(psycopg2.errors.InsufficientPrivilege):
        cursor.execute(sql)


def test_mcp_reader_can_select_from_gold(mcp_reader_connection):
    with mcp_reader_connection.cursor() as cursor:
        cursor.execute("select count(*) from gold.dim_customer")
        assert cursor.fetchone()[0] > 0


def test_mcp_reader_can_select_from_catalog(mcp_reader_connection):
    with mcp_reader_connection.cursor() as cursor:
        cursor.execute("select count(*) from catalog.dataset_embeddings")
        assert cursor.fetchone()[0] > 0


def test_mcp_reader_cannot_write_to_gold(mcp_reader_connection):
    with mcp_reader_connection.cursor() as cursor:
        _assert_permission_denied(cursor, "insert into gold.dim_customer (customer_id) values ('x')")
        _assert_permission_denied(cursor, "update gold.dim_customer set customer_city = 'x'")
        _assert_permission_denied(cursor, "delete from gold.dim_customer")


def test_mcp_reader_cannot_write_to_catalog(mcp_reader_connection):
    """mcp_reader is read-only everywhere, including the catalog schema it
    reads from -- catalog_writer, a separate role, owns writes there."""

    with mcp_reader_connection.cursor() as cursor:
        _assert_permission_denied(
            cursor,
            "insert into catalog.query_templates (template_id, name, description, sql_template) "
            "values ('x', 'x', 'x', 'x')",
        )


@pytest.mark.parametrize(
    "schema,table",
    [("bronze", "raw_customers"), ("staging", "stg_olist_customers")],
)
def test_mcp_reader_cannot_access_bronze_or_staging(mcp_reader_connection, schema, table):
    with mcp_reader_connection.cursor() as cursor:
        _assert_permission_denied(cursor, f"select count(*) from {schema}.{table}")


def test_mcp_reader_has_a_statement_timeout_set(mcp_reader_connection):
    """Defense in depth for run_supported_query, per postgres/init/032's own
    comment: even a pre-approved template shouldn't be able to run forever."""

    with mcp_reader_connection.cursor() as cursor:
        cursor.execute("show statement_timeout")
        assert cursor.fetchone()[0] == "5s"
