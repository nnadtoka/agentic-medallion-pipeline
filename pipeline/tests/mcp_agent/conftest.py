"""Shared helper for mcp_agent's live-integration tests
(test_run_supported_query.py, test_db_permissions.py) -- both need to call
a real MCP tool against the actually-running `mcp-server` and skip cleanly
(not fail) if it isn't up.

test_search_evals.py predates this and keeps its own inline copy of the
same skip-on-unreachable logic -- not refactored to share this, to avoid
touching an already-passing, already-documented file for a pure-DRY reason.
"""

import asyncio

import httpx
import pytest
from fastmcp import Client

MCP_URL = "http://127.0.0.1:8000/mcp"


async def _call(name, params):
    async with Client(MCP_URL) as client:
        response = await client.call_tool(name, params)
        return response.data


def call_mcp_tool(name, params):
    """Call an MCP tool synchronously; skip the test if mcp-server isn't reachable."""

    try:
        return asyncio.run(_call(name, params))
    except (httpx.ConnectError, ConnectionError, OSError) as exc:
        pytest.skip(
            f"mcp-server not reachable at {MCP_URL} ({exc}) -- "
            "run `docker compose up -d mcp-server` to run this test"
        )
    except RuntimeError as exc:
        # fastmcp wraps a failed connection attempt in its own RuntimeError
        # rather than letting httpx.ConnectError propagate -- only treat
        # *that* specific case as "server's not up", so an actual bug
        # inside a tool call still fails loudly instead of being silently
        # skipped. Same reasoning as test_search_evals.py's fixture.
        if "failed to connect" not in str(exc).lower():
            raise
        pytest.skip(
            f"mcp-server not reachable at {MCP_URL} ({exc}) -- "
            "run `docker compose up -d mcp-server` to run this test"
        )
