"""Execution-correctness tests for `run_supported_query`, against real
gold data through the live `mcp-server`.

`test_search_evals.py` covers *retrieval* (does search find the right
template for a question). `test_query_templates.py` covers the pure
*validation logic* (`resolve_query()`) in isolation, with no DB or MCP
protocol involved. Neither actually executes a template against Postgres
and checks the results are correct, and neither exercises rejection
through the real `run_supported_query` tool (as opposed to calling
`resolve_query()` directly) -- this file closes both gaps.

Skips cleanly if `mcp-server` isn't reachable -- see conftest.py.
"""

from pipeline.tests.mcp_agent.conftest import call_mcp_tool

# Verified live earlier in this project's history: a real customer with
# exactly 2 orders under one customer_unique_id (across what would be two
# different, order-specific customer_id rows) -- the case the grain fix in
# customer_order_history_lookup.yaml exists for.
REPEAT_CUSTOMER_UNIQUE_ID = "027803eb28cc01fbdf5da72b109fabee"

# Verified live: the top seller by GMV in gold.agg_seller_performance.
TOP_SELLER_ID = "4869f7a5dfa277a7dca6462dcf3b52b2"


def _run(template_id, params):
    return call_mcp_tool("run_supported_query", {"template_id": template_id, "params": params})


# --- Execution correctness: each template returns real, sane data ---


def test_top_n_by_metric_returns_correctly_ranked_real_data():
    rows = _run("top_n_by_metric", {"dimension": "customer_state", "metric": "gmv", "limit": 5})

    assert 1 <= len(rows) <= 5
    values = [float(row["metric_value"]) for row in rows]
    assert values == sorted(values, reverse=True), "expected GMV ranked highest-first"
    assert all(row["dimension_value"] for row in rows), "every row should have a state"


def test_metric_over_time_returns_chronological_periods():
    rows = _run("metric_over_time", {"date_grain": "month", "metric": "gmv"})

    assert len(rows) >= 1
    periods = [row["period"] for row in rows]
    assert periods == sorted(periods), "expected periods oldest-first"


def test_customer_order_history_lookup_aggregates_repeat_customer_correctly():
    rows = _run("customer_order_history_lookup", {"customer_unique_id": REPEAT_CUSTOMER_UNIQUE_ID})

    assert len(rows) == 1
    row = rows[0]
    assert row["order_count"] == 2, (
        "grain fix: must aggregate by customer_unique_id (the stable person), "
        "not customer_id (order-specific) -- see the YAML's grain note"
    )
    assert float(row["lifetime_value"]) > 0


def test_top_customers_by_lifetime_value_returns_descending_ranking():
    rows = _run("top_customers_by_lifetime_value", {"limit": 5})

    assert len(rows) == 5
    values = [float(row["lifetime_value"]) for row in rows]
    assert values == sorted(values, reverse=True)


def test_quiet_key_accounts_respects_recency_threshold():
    threshold = 10
    rows = _run("quiet_key_accounts", {"min_days_since_last_order": threshold, "limit": 5})

    assert len(rows) >= 1
    assert all(row["days_since_last_order"] >= threshold for row in rows)
    values = [float(row["lifetime_value"]) for row in rows]
    assert values == sorted(values, reverse=True)


def test_top_customers_and_quiet_accounts_agree_when_everyone_qualifies():
    """Cross-consistency between two independently-written templates: with
    min_days_since_last_order=0 every customer qualifies for
    quiet_key_accounts, so its #1 row (ranked by lifetime_value, same as
    top_customers_by_lifetime_value) must be the exact same customer with
    the exact same figure -- same underlying fct_orders data, no reason
    for two different templates to disagree."""

    top = _run("top_customers_by_lifetime_value", {"limit": 1})[0]
    quiet_top = _run("quiet_key_accounts", {"min_days_since_last_order": 0, "limit": 1})[0]

    assert quiet_top["customer_unique_id"] == top["customer_unique_id"]
    assert quiet_top["lifetime_value"] == top["lifetime_value"]


# --- Seller-side templates (agg_seller_performance) ---


def test_seller_performance_lookup_returns_real_seller_data():
    rows = _run("seller_performance_lookup", {"seller_id": TOP_SELLER_ID})

    assert len(rows) == 1
    row = rows[0]
    assert row["seller_id"] == TOP_SELLER_ID
    assert float(row["gmv"]) > 0
    assert row["order_count"] > 0
    assert row["single_seller_order_count"] <= row["order_count"] + row["canceled_order_count"]


def test_top_sellers_by_metric_desc_returns_correctly_ranked_data():
    rows = _run("top_sellers_by_metric", {"metric": "gmv", "sort_direction": "desc", "limit": 5})

    assert len(rows) == 5
    assert rows[0]["seller_id"] == TOP_SELLER_ID, "top seller by GMV should rank first"
    values = [float(row["metric_value"]) for row in rows]
    assert values == sorted(values, reverse=True)


def test_top_sellers_by_metric_asc_finds_poorest_reviews_not_best():
    """Sanity-checks the sort_direction toggle actually reverses ranking --
    without it, "which sellers have the poorest reviews" would be
    unanswerable (top_n_by_metric.yaml has no equivalent toggle)."""

    worst = _run(
        "top_sellers_by_metric",
        {"metric": "avg_review_score", "sort_direction": "asc", "limit": 5},
    )
    best = _run(
        "top_sellers_by_metric",
        {"metric": "avg_review_score", "sort_direction": "desc", "limit": 5},
    )

    worst_scores = [float(row["metric_value"]) for row in worst]
    best_scores = [float(row["metric_value"]) for row in best]
    assert worst_scores == sorted(worst_scores)
    assert best_scores == sorted(best_scores, reverse=True)
    assert max(worst_scores) <= min(best_scores)


def test_quiet_key_sellers_respects_recency_threshold():
    threshold = 10
    rows = _run("quiet_key_sellers", {"min_days_since_last_order": threshold, "limit": 5})

    assert len(rows) >= 1
    assert all(row["days_since_last_order"] >= threshold for row in rows)
    values = [float(row["gmv"]) for row in rows]
    assert values == sorted(values, reverse=True)


def test_top_sellers_and_quiet_key_sellers_agree_when_everyone_qualifies():
    """Same cross-consistency check as the customer-side templates: with
    min_days_since_last_order=0 every seller with a real order qualifies,
    so quiet_key_sellers' #1 row (ranked by GMV, same as
    top_sellers_by_metric) must be the same seller with the same GMV."""

    top = _run("top_sellers_by_metric", {"metric": "gmv", "sort_direction": "desc", "limit": 1})[0]
    quiet_top = _run("quiet_key_sellers", {"min_days_since_last_order": 0, "limit": 1})[0]

    assert quiet_top["seller_id"] == top["seller_id"]
    assert quiet_top["gmv"] == top["gmv"]


def test_top_sellers_by_metric_rejects_unknown_metric_at_the_tool_boundary():
    result = _run("top_sellers_by_metric", {"metric": "dance_moves", "sort_direction": "desc"})

    assert isinstance(result, dict) and "error" in result
    assert "metric" in result["error"]


def test_top_sellers_by_metric_requires_sort_direction_at_the_tool_boundary():
    result = _run("top_sellers_by_metric", {"metric": "gmv"})

    assert isinstance(result, dict) and "error" in result
    assert "sort_direction" in result["error"]


# --- customer_360_current-backed and category/concentration templates ---


def test_customer_360_lookup_returns_repeat_customer_correctly():
    rows = _run("customer_360_lookup", {"customer_unique_id": REPEAT_CUSTOMER_UNIQUE_ID})

    assert len(rows) == 1
    row = rows[0]
    assert row["order_count"] == 2
    assert row["repeat_customer"] is True
    assert row["as_of_date"] is not None


def test_customer_360_lookup_supports_an_explicit_historical_as_of_date():
    latest = _run("customer_360_lookup", {"customer_unique_id": REPEAT_CUSTOMER_UNIQUE_ID})[0]
    historical = _run(
        "customer_360_lookup",
        {"customer_unique_id": REPEAT_CUSTOMER_UNIQUE_ID, "as_of_date": "2017-11-06"},
    )[0]

    assert historical["as_of_date"] != latest["as_of_date"] or historical["as_of_date"] == "2017-11-06"


def test_top_categories_by_metric_returns_correctly_ranked_data():
    rows = _run("top_categories_by_metric", {"metric": "item_value", "limit": 5})

    assert len(rows) == 5
    values = [float(row["metric_value"]) for row in rows]
    assert values == sorted(values, reverse=True)
    assert all(row["category_name"] for row in rows)


def test_seller_revenue_concentration_buckets_sum_to_roughly_all_gmv():
    rows = _run("seller_revenue_concentration", {"bucket_count": 10})

    assert len(rows) == 10
    percentages = [float(row["pct_of_total_gmv"]) for row in rows]
    assert abs(sum(percentages) - 100.0) < 0.5, "bucket percentages should sum to ~100%"
    # Top decile should hold a disproportionate share -- real concentration,
    # not a coincidence of rounding.
    assert percentages[0] > percentages[-1]


# --- Rejection at the real tool boundary (not just resolve_query() in isolation) ---


def test_unknown_template_id_returns_clean_error_not_a_crash():
    result = _run("does_not_exist", {})

    assert isinstance(result, dict) and "error" in result
    assert "does_not_exist" in result["error"]


def test_out_of_enum_dimension_is_rejected_at_the_tool_boundary():
    result = _run("top_n_by_metric", {"dimension": "product_category", "metric": "gmv"})

    assert isinstance(result, dict) and "error" in result
    assert "dimension" in result["error"]


def test_missing_required_param_is_rejected_at_the_tool_boundary():
    result = _run("customer_order_history_lookup", {})

    assert isinstance(result, dict) and "error" in result
    assert "customer_unique_id" in result["error"]
