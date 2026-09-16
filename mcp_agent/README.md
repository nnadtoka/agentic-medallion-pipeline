# Gold catalog MCP server

A hardened, read-only MCP server over the gold layer: resources for browsing the semantic
catalog, `search_semantic_catalog` for finding the right table/column/query template for a
plain-language question, and `run_supported_query` for executing one of a fixed set of
pre-approved, parameterized query templates — never raw SQL.

## Design rationale

**Why not raw SQL, or a generic filter tool.** Three options were considered for the query
surface: (A) raw SQL behind a read-only role — stops writes, not wrong or runaway `SELECT`s;
(B) raw SQL through an AST validator (`SELECT`-only, gold-only, forced `LIMIT`) — more effort,
still doesn't stop a *syntactically valid but semantically wrong* query (wrong join key,
silently wrong number), the exact failure mode a semantic layer exists to prevent; (C) a
structured filter the server turns into SQL — injection becomes structurally impossible, every
query is reproducible. Adopted, but shaped into **named templates** rather than one generic
filter object, since a business question maps more naturally onto "which canned query is this"
than onto "which metric/dimension/filter combination is this."

Each template's parameters come in two kinds, because they need different guardrails:
**value** parameters (a date, an ID, a threshold) always bind as ordinary SQL parameters, safe
regardless of content; **identifier** parameters (which dimension to group by, which metric to
aggregate) can only be chosen from a fixed enum declared on the template — the server maps the
chosen key to a pre-validated SQL fragment internally, so the agent's own text never becomes a
raw identifier in the query. If search finds no template above a confidence threshold, the
agent says "no supported query for that yet" rather than falling back to raw SQL — that dead
end is the actual safety boundary of the whole design.

**Resources vs. tools.** Static/keyed reads (descriptions, the template list) are MCP
*resources* — cheap, cacheable, never touch the `embedding` column. Anything requiring
computation (embedding a query, executing a template) is a *tool*. This matches the emerging
MCP convention of resources-as-GET, tools-as-POST-with-real-cost.

**Embedding model.** Local, `all-MiniLM-L6-v2` (384 dims), run through `fastembed` (ONNX
Runtime) rather than `sentence-transformers`/PyTorch — no external API dependency, reproducible
offline in Docker, and `pip install torch` on Linux defaults to a CUDA-bundled multi-GB build
even for CPU-only use; `fastembed` runs the same weights through ONNX for ~275MB total.

**Sync-job identity.** The catalog sync job writes as its own role, `catalog_writer`, scoped to
the `catalog` schema only — separate from `mcp_reader`, which only ever needs `SELECT`. Giving
the MCP server write privileges it never uses would widen its blast radius for no benefit.

## Deployment: a real docker-compose service, not a spawned subprocess

Runs as the least-privilege `mcp_reader` role. `mcp-server` is a fourth
service in `docker-compose.yml`, alongside `postgres`/`prefect-server`/
`pipeline-engine` — persistent, connects to Postgres over the internal
`postgres:5432` address, reached from the host at `127.0.0.1:8000` (same
loopback-only discipline every other service here uses). Claude Code
connects to it project-scoped via `.mcp.json` at the repo root, over HTTP
(`streamable-http` transport — the current MCP spec's transport; the older
dedicated SSE transport was deprecated 2025-03 and hit end-of-life
2026-04-01, so this deliberately isn't used even though `fastmcp` still
supports it).

Why a persistent service instead of a client-spawned subprocess: no
per-session embedding-model reload cost, one warm process instead of one
per session, and no dependency on a specific `.venv` path existing on
whoever's machine runs the client — anyone with this repo just needs
`docker compose up`.

```bash
docker compose up -d mcp-server
```

Then open (or reload) a Claude Code session in this repo — `.mcp.json` is
picked up automatically. `/mcp` inside that session confirms connection.

The code also still supports stdio (`MCP_TRANSPORT` unset, the default) for
quick local testing without Docker:
```bash
.venv/bin/python -m mcp_agent.server
```

## Hardening applied (`docker/mcp-server/Dockerfile`, `docker-compose.yml`)

- Multi-stage build — build tools never reach the final image.
- Minimal base (`python:3.12-slim`), not the `prefecthq/prefect` image
  `pipeline-engine` uses — this service never touches Prefect.
- Runs as a non-root user (`mcpuser`), confirmed via `docker exec ... whoami`.
- `read_only: true` root filesystem + a `tmpfs` mount for `/tmp` only —
  confirmed live: writes to `/app` fail (`Read-only file system`), writes to
  `/tmp` succeed.
- `cap_drop: [ALL]` + `no-new-privileges:true`.
- Connection pooling (`psycopg2.pool.ThreadedConnectionPool`) instead of a
  fresh connect/close per call.
- `fastembed`'s model cache pinned outside `/tmp` via `FASTEMBED_CACHE_DIR`
  (`pipeline/catalog_sync/embedding.py`) — its *default* cache path is
  under `/tmp`, which the `tmpfs` mount above would otherwise silently wipe
  at every container start, forcing a re-download (or a hard failure with
  no network) instead of using the model baked in at build time.
- `healthcheck` + `depends_on: postgres: condition: service_healthy` +
  `restart: unless-stopped`, same pattern as the other three services.

## What's exposed

**Resources** (cheap, keyed reads — never touch the `embedding` column):
- `catalog://gold` — every gold table with its description.
- `catalog://gold/{table}` — one table's description plus every column's.
- `catalog://query-templates` — list of supported templates.
- `catalog://query-templates/{template_id}` — one template's spec.

**Tools**:
- `search_semantic_catalog(query, top_k=5, object_type=None)` — embeds
  `query` with the same local `fastembed` model `catalog_sync` uses, cosine-
  searches `catalog.dataset_embeddings`, returns resource URIs + similarity
  scores (not full description text — read the matched resource for detail).
- `run_supported_query(template_id, params)` — validates `params` against
  the template's stored spec (`mcp_agent/query_templates.py`), then executes
  it as `mcp_reader`. `value`-kind params bind as ordinary SQL parameters;
  `identifier`-kind params may only select from a fixed enum baked into the
  template row (never raw agent text); any `limit` param is capped at the
  template's `max_limit` regardless of what's requested.

**The 11 seeded templates** (source files: `mcp_agent/templates/*.yaml`), grouped by shape:

| Shape | Templates |
|---|---|
| Aggregate ranking by a guarded dimension | `top_n_by_metric`, `top_categories_by_metric` |
| Metric trended over time | `metric_over_time` |
| Entity lookup (one ID → one row) | `customer_order_history_lookup`, `customer_360_lookup`, `seller_performance_lookup` |
| Top-N entity ranking, no dimension slot | `top_customers_by_lifetime_value`, `top_sellers_by_metric` |
| Recency threshold + value ranking | `quiet_key_accounts`, `quiet_key_sellers` |
| Bucketed distribution | `seller_revenue_concentration` |

The seller-side templates and `customer_360_lookup` were added later, deliberately mirroring
shapes already proven on the customer side rather than inventing new ones, once
`agg_seller_performance`/`customer_360_current` existed in gold. `top_n_by_metric`'s
`dimension` enum only has `customer_state` so far — the other candidates
(`product_category`/`seller_state`/`payment_type`) join through tables with multiple rows per
order, which would double-count order-level metrics unless those expressions are reworked to
de-duplicate first.

A recurring pattern across the recency-based templates: "days since last order" is measured
against the dataset's own most-recent order date, not real wall-clock `now()` — this is a
frozen historical dataset, so comparing against actual today would make every entity look
permanently dormant regardless of real behavior.

## Try it yourself

With `docker compose up -d mcp-server` running and a Python environment that has `fastmcp`
(`.venv` in this repo already does), this script exercises both the "it works" path and the
"it correctly refuses" path — including a completely off-topic, non-data question, to check
the guardrail doesn't hallucinate a match just because *something* was asked:

```python
import asyncio
from fastmcp import Client

async def main():
    async with Client("http://127.0.0.1:8000/mcp") as client:
        # 1. A real business question -- should surface the right template
        #    with a strong similarity score.
        r = await client.call_tool(
            "search_semantic_catalog",
            {"query": "which states have the highest GMV", "top_k": 3},
        )
        print("on-topic search:", r.data[0])

        # 2. Run it for a real answer.
        r = await client.call_tool(
            "run_supported_query",
            {"template_id": "top_n_by_metric",
             "params": {"dimension": "customer_state", "metric": "gmv", "limit": 3}},
        )
        print("run_supported_query:", r.data)

        # 3. Something with zero connection to the gold catalog. Does the
        #    tool hallucinate a plausible-looking match, or correctly find
        #    nothing worth acting on?
        r = await client.call_tool(
            "search_semantic_catalog",
            {"query": "write a recursive traversal of a linked list", "top_k": 3},
        )
        print("off-topic search (expect low scores, no real match):", r.data)

        # 4. What if an agent skips search and just tries to smuggle that
        #    same string in directly as a template_id?
        r = await client.call_tool(
            "run_supported_query",
            {"template_id": "write a recursive traversal of a linked list", "params": {}},
        )
        print("misuse attempt:", r.data)

asyncio.run(main())
```

Expected shape of the output: step 1's top match is a `query_template` with similarity well
above 0.4; step 2 returns real ranked rows; step 3's best score is under 0.2 (weak,
coincidental word overlap, not a real match — nothing here should be presented to a user as
an answer); step 4 returns a clean `{"error": "No supported query template named '...'"}`
rather than executing anything. The tool has no code-generation/execution capability at
all — it only ever talks to Postgres through one of the seeded templates — so the
interesting part isn't that it "can't write code", it's that it doesn't quietly do something
*plausible-but-wrong* instead of just saying no.

## Eval suite: the pre-release gate for search/template/execution changes

For this toy, "promotion" is represented by these automated tests, not a real CI/CD
pipeline — in a production deployment, the equivalent would be building an immutable
container image, deploying it to staging, running these same gates there, and promoting
that exact tested image to production, the same "test the thing that actually ships"
principle scaled up.

Three files, deliberately covering different risk, not just more of the same unit coverage:

- **`test_search_evals.py`** (retrieval) — cases in `mcp_agent/evals/cases.yaml`: one
  positive match per seeded template, several table/column-search positives, and off-topic
  no-match negatives (e.g. "write a recursive traversal of a linked list"). Each asserts the
  top hit clears a `similarity_floor` of 0.30 for positives (empirically grounded: real
  matches scored 0.45–0.76 checked live, off-topic questions scored well under 0.2) and
  stays under it for negatives.
- **`test_run_supported_query.py`** (execution) — runs every template against real gold data
  through the live tool and checks the *results* are actually correct (GMV ranked descending,
  a real repeat customer's history aggregating to `order_count: 2`, independently-written
  templates agreeing with each other at a shared threshold), plus that an unknown
  `template_id`, an out-of-enum param, and a missing required param are all rejected cleanly
  *at the real tool boundary* — not just by `resolve_query()` in isolation
  (`test_query_templates.py` already covers that).
- **`test_db_permissions.py`** (access control) — connects directly as `mcp_reader` and
  verifies the privilege boundary the entire design depends on is actually enforced by
  Postgres: it can `SELECT` from `gold`/`catalog`, cannot write to either, cannot touch
  `bronze`/`staging` at all, and has its `statement_timeout` set.

Run all three whenever `mcp_agent/templates/*.yaml`, `search_semantic_catalog`, or
`run_supported_query`'s logic changes, before rebuilding/redeploying the image:

```bash
docker compose up -d postgres mcp-server
pytest pipeline/tests/mcp_agent/ -v
```

Each skips cleanly (not a failure) if the infra it needs isn't reachable, so the default
`pytest pipeline/tests -q` run stays green without live infra.

## Explicitly out of scope for this toy

- dbt Semantic Layer / MetricFlow (would need dbt Cloud/Fusion; disproportionate for this
  stack).
- Multi-database support.
- Any write tools.
- Auth beyond the fixed `mcp_reader` Postgres role.
- A raw-SQL escape hatch alongside the templates — offering both defeats the point of
  building guardrailed templates, since agents (and debugging humans) will always reach
  for the more flexible option.
- Reranking beyond plain cosine similarity in `search_semantic_catalog`.
- Caching of resource reads, or a `resources/list_changed` notification off
  `catalog.catalog_version` — every read hits Postgres fresh, which is fine at this scale.
- Alerting on pipeline/catalog-sync failures — no alerting channel exists in this toy setup,
  so failures are only discoverable by looking, not pushed to anyone.
- TLS/auth beyond loopback-only binding — acceptable for a single-user local deployment, not
  for anything beyond that.
