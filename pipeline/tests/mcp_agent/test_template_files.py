"""Smoke tests for the actual shipped template YAML files under
mcp_agent/templates/ -- catches mismatches between a template's declared
`parameters` and what its `sql_template` actually references (e.g. a
renamed placeholder), which the hand-rolled specs in
test_query_templates.py can't see since those are deliberately synthetic.

No DB connection: this only exercises resolve_query()'s pure logic against
real file content, same as sync_module tests elsewhere in this suite.
"""

from pathlib import Path

import pytest

from mcp_agent.query_templates import resolve_query
from pipeline.catalog_sync.seed_templates import _load_templates

TEMPLATES_DIR = Path(__file__).resolve().parents[3] / "mcp_agent" / "templates"

VALID_PARAMS = {
    "customer_order_history_lookup": {"customer_unique_id": "abc123"},
    "top_n_by_metric": {"dimension": "customer_state", "metric": "gmv"},
    "metric_over_time": {"date_grain": "month", "metric": "gmv"},
    "top_customers_by_lifetime_value": {"limit": 5},
    "quiet_key_accounts": {"min_days_since_last_order": 10},
    "seller_performance_lookup": {"seller_id": "abc123"},
    "top_sellers_by_metric": {"metric": "gmv", "sort_direction": "desc"},
    "quiet_key_sellers": {"min_days_since_last_order": 10},
    "customer_360_lookup": {"customer_unique_id": "abc123"},
    "top_categories_by_metric": {"metric": "item_value"},
    "seller_revenue_concentration": {},
}


@pytest.fixture(scope="module")
def templates():
    return {t["template_id"]: t for t in _load_templates(TEMPLATES_DIR)}


def test_every_designed_template_has_a_yaml_file(templates):
    assert set(templates) == set(VALID_PARAMS)


@pytest.mark.parametrize("template_id", sorted(VALID_PARAMS))
def test_template_resolves_with_valid_params_and_binds_every_placeholder(
    template_id, templates
):
    template = templates[template_id]
    sql, bound = resolve_query(
        template["sql_template"], template["parameters"], VALID_PARAMS[template_id],
        template["max_limit"],
    )

    # No leftover {identifier} placeholder should survive resolution --
    # that would mean a declared parameter's `name` doesn't match what the
    # sql_template actually references.
    assert "{" not in sql and "}" not in sql

    # Every declared parameter produced a bound value (even if None for an
    # optional one) -- an identifier-kind param contributes to `sql`
    # instead, so only value-kind names are expected in `bound`.
    value_param_names = {
        p["name"] for p in template["parameters"] if p["kind"] == "value"
    }
    assert value_param_names <= bound.keys()
