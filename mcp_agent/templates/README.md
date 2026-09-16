# Query templates

Source-of-truth files for `catalog.query_templates` — one YAML file per template.
`pipeline/catalog_sync/seed_templates.py` reads every file here and upserts it into
`catalog.query_templates`, embedding each `description` into `catalog.dataset_embeddings`
the same way `catalog_sync` embeds table/column descriptions, so `search_semantic_catalog`
can surface templates too. Run it (inside `pipeline-engine`, or from the host `.venv`)
whenever a template file here is added or changed:

```bash
docker compose exec pipeline-engine python -m pipeline.catalog_sync.seed_templates
```

Folder convention chosen to match two things: the closest real-world MCP precedent
(dedicated `templates/`/`prompts/` directories with one YAML config per template in
existing MCP prompt-template servers) and this project's own framework's stated best
practice (dbt's Semantic Layer docs recommend a dedicated folder over co-location once
there's more than a couple of these). See `mcp_agent/README.md`'s "Design rationale"
section for the full reasoning ("Why not raw SQL", "Why templates, not one generic
filter tool").

## File schema

One YAML file per template, named `<template_id>.yaml`. Two real, seeded examples —
`value`-kind only (`customer_order_history_lookup.yaml`):

```yaml
template_id: customer_order_history_lookup
name: Customer order history lookup
description: >
  Full activity picture for one customer across all of their orders --
  order count, first/latest order dates, lifetime value, average order
  value, and days since their last order.
sql_template: |
  select
      c.customer_unique_id,
      count(distinct o.order_id) as order_count,
      min(o.order_purchase_timestamp) as first_order_ts,
      max(o.order_purchase_timestamp) as latest_order_ts,
      coalesce(sum(o.order_item_value), 0) as lifetime_value
  from gold.dim_customer c
  left join gold.fct_orders o on o.customer_id = c.customer_id
  where c.customer_unique_id = %(customer_unique_id)s
  group by c.customer_unique_id
target_schema: gold
max_limit: 1
parameters:
  - name: customer_unique_id
    kind: value          # value | identifier -- see below
    type: string
    required: true
```

and one `identifier`-kind slot (from `top_n_by_metric.yaml`, trimmed):

```yaml
sql_template: |
  select {dimension} as dimension_value, {metric} as metric_value
  from gold.fct_orders o
  join gold.dim_customer c on c.customer_id = o.customer_id
  group by {dimension}
  order by metric_value desc
  limit %(limit)s
parameters:
  - name: dimension
    kind: identifier
    type: string
    required: true
    enum:
      customer_state: c.customer_state   # agent picks the key; the tool substitutes the value
```

- **`description`** is what gets embedded — write it the way a business user would phrase
  the underlying question, not just a restatement of the table/column names (matches how
  `catalog_sync` treats table/column descriptions).
- **`sql_template`** uses named placeholders (`%(name)s`, psycopg2 style) for `value`-kind
  parameters, bound as literal query parameters at execution time — safe regardless of
  content. `identifier`-kind parameters instead use `{python_format}`-style placeholders
  (e.g. `{dimension}`), resolved via `str.format()` in `mcp_agent/query_templates.py`
  *before* the SQL reaches psycopg2 — the agent only ever supplies the `enum` *key*
  (e.g. `"customer_state"`); the fragment actually spliced into the query is the mapped
  *value*, which lives in this file, not in anything the agent sent. That's the actual
  safety property this whole design exists for — see
  `pipeline/tests/mcp_agent/test_query_templates.py`'s injection-attempt test.
- **`parameters`**, one entry per placeholder — needs `name`, `kind`, `type`, `required`,
  and (for `value` params with bounds, or `identifier` params) `enum`/`min`/`max`
  constraints, all validated before the query ever runs.
- **`target_schema`** is always `gold` for now (the only schema templates are allowed to
  touch).
- **`max_limit`** caps returned rows (via a `limit` value-param, if the template declares
  one) regardless of what the agent asks for.

## Templates

All 11 are built and seeded. See `mcp_agent/README.md`'s "What's exposed" section for the
full list grouped by shape (aggregate ranking, trended-over-time, entity lookup, top-N
ranking, recency-threshold ranking, bucketed distribution). Two things worth noting here that
apply across several files:

- `customer_order_history_lookup.yaml`, `customer_360_lookup.yaml`, and
  `seller_performance_lookup.yaml` are entity lookups — one required ID parameter, one row
  back. The customer-side ones are keyed on `customer_unique_id`, not `customer_id` — see the
  grain note in each file.
- `top_n_by_metric.yaml`'s `dimension` identifier slot currently only supports
  `customer_state` — the only fan-out-safe option so far. `product_category`/`seller_state`/
  `payment_type` all join through tables with multiple rows per order
  (`fct_order_items`/`fct_order_payments`), which would double-count order-level metrics
  unless those expressions are reworked to de-duplicate first — see the file's comment.
