"""Behavior eval suite for `search_semantic_catalog`, run against the real,
live `mcp-server` over HTTP -- see mcp_agent/evals/cases.yaml for the case
data and the similarity_floor's justification.

This is deliberately not a hermetic unit test: it's the "does the actual
deployed behavior still make sense" gate `mcp_agent/README.md` calls
for, illustrating the same before-you-release-it idea as Great
Expectations gating candidate marts before gold promotion -- just for the
MCP/search layer instead of the data layer, and run manually rather than
Prefect-orchestrated (there's no CI/deployment pipeline for mcp-server in
this repo to hook it into).

Run this explicitly, with `docker compose up -d mcp-server` and templates
already seeded, whenever mcp_agent/templates/*.yaml or
search_semantic_catalog's logic changes -- before rebuilding/redeploying
the image:

    pytest pipeline/tests/mcp_agent/test_search_evals.py -v

Skips cleanly (not a failure) if the server isn't reachable at all, so the
default `pytest pipeline/tests -q` run stays green without live infra.
"""

import asyncio
from pathlib import Path

import httpx
import pytest
import yaml
from fastmcp import Client

CASES_PATH = Path(__file__).resolve().parents[3] / "mcp_agent" / "evals" / "cases.yaml"
MCP_URL = "http://127.0.0.1:8000/mcp"

_spec = yaml.safe_load(CASES_PATH.read_text())
SIMILARITY_FLOOR = _spec["similarity_floor"]
CASES = _spec["cases"]


async def _run_all_cases():
    """One Client connection, one search_semantic_catalog call per case."""

    results = {}
    async with Client(MCP_URL) as client:
        for case in CASES:
            params = {"query": case["query"], "top_k": 3}
            if case.get("object_type"):
                params["object_type"] = case["object_type"]
            response = await client.call_tool("search_semantic_catalog", params)
            results[case["query"]] = response.data
    return results


@pytest.fixture(scope="module")
def eval_results():
    try:
        return asyncio.run(_run_all_cases())
    except (httpx.ConnectError, ConnectionError, OSError) as exc:
        pytest.skip(
            f"mcp-server not reachable at {MCP_URL} ({exc}) -- "
            "run `docker compose up -d mcp-server` to run this eval"
        )
    except RuntimeError as exc:
        # fastmcp wraps a failed connection attempt in its own RuntimeError
        # rather than letting httpx.ConnectError propagate -- only treat
        # *that* specific case as "server's not up", so an actual bug
        # inside a tool call (a different RuntimeError) still fails loudly
        # instead of being silently skipped.
        if "failed to connect" not in str(exc).lower():
            raise
        pytest.skip(
            f"mcp-server not reachable at {MCP_URL} ({exc}) -- "
            "run `docker compose up -d mcp-server` to run this eval"
        )


@pytest.mark.eval
@pytest.mark.parametrize("case", CASES, ids=[c["query"][:50] for c in CASES])
def test_search_eval_case(case, eval_results):
    hits = eval_results[case["query"]]
    top_hit = hits[0] if hits else None

    if case.get("expect") == "no_match":
        assert top_hit is None or top_hit["similarity"] < SIMILARITY_FLOOR, (
            f"Expected no confident match for {case['query']!r}, got {top_hit!r} "
            f"(floor={SIMILARITY_FLOOR})"
        )
        return

    assert top_hit is not None, f"No results at all for {case['query']!r}"
    assert top_hit["similarity"] >= SIMILARITY_FLOOR, (
        f"Top hit for {case['query']!r} scored below floor {SIMILARITY_FLOOR}: {top_hit!r}"
    )
    if "expect_uri" in case:
        assert top_hit["uri"] == case["expect_uri"], (
            f"Expected top match {case['expect_uri']!r} for {case['query']!r}, got {top_hit!r}"
        )
    if "expect_column" in case:
        assert top_hit["column"] == case["expect_column"], (
            f"Expected top column {case['expect_column']!r} for {case['query']!r}, got {top_hit!r}"
        )
