import pytest

from mcp_agent.query_templates import QueryTemplateError, resolve_query

LOOKUP_SPEC = [
    {"name": "customer_unique_id", "kind": "value", "type": "string", "required": True},
]
LOOKUP_SQL = "select * from gold.dim_customer where customer_unique_id = %(customer_unique_id)s"

TOP_N_SPEC = [
    {
        "name": "dimension",
        "kind": "identifier",
        "type": "string",
        "required": True,
        "enum": {"customer_state": "c.customer_state"},
    },
    {
        "name": "metric",
        "kind": "identifier",
        "type": "string",
        "required": True,
        "enum": {"order_count": "count(*)", "gmv": "sum(o.order_item_value)"},
    },
    {"name": "date_from", "kind": "value", "type": "date", "required": False},
    {"name": "date_to", "kind": "value", "type": "date", "required": False},
    {
        "name": "limit",
        "kind": "value",
        "type": "integer",
        "required": False,
        "default": 10,
        "min": 1,
    },
]
TOP_N_SQL = (
    "select {dimension} as dimension_value, {metric} as metric_value "
    "from gold.fct_orders o join gold.dim_customer c on c.customer_id = o.customer_id "
    "where (%(date_from)s::date is null or o.order_purchase_timestamp >= %(date_from)s::date) "
    "group by {dimension} order by metric_value desc limit %(limit)s"
)


def test_value_param_binds_through_untouched():
    sql, params = resolve_query(
        LOOKUP_SQL, LOOKUP_SPEC, {"customer_unique_id": "abc123"}, max_limit=1
    )
    assert sql == LOOKUP_SQL
    assert params == {"customer_unique_id": "abc123"}


def test_missing_required_value_param_raises():
    with pytest.raises(QueryTemplateError, match="customer_unique_id"):
        resolve_query(LOOKUP_SQL, LOOKUP_SPEC, {}, max_limit=1)


def test_identifier_param_resolves_to_its_enum_fragment_not_raw_text():
    sql, params = resolve_query(
        TOP_N_SQL,
        TOP_N_SPEC,
        {"dimension": "customer_state", "metric": "gmv"},
        max_limit=100,
    )
    assert "c.customer_state as dimension_value" in sql
    assert "sum(o.order_item_value) as metric_value" in sql
    assert "group by c.customer_state" in sql
    # The raw enum keys never appear as SQL text themselves.
    assert "{dimension}" not in sql and "{metric}" not in sql


def test_identifier_param_rejects_value_outside_its_enum():
    with pytest.raises(QueryTemplateError, match="dimension"):
        resolve_query(
            TOP_N_SQL,
            TOP_N_SPEC,
            {"dimension": "product_category", "metric": "gmv"},
            max_limit=100,
        )


def test_identifier_param_cannot_be_used_to_inject_arbitrary_sql():
    with pytest.raises(QueryTemplateError):
        resolve_query(
            TOP_N_SQL,
            TOP_N_SPEC,
            {"dimension": "1); drop table gold.fct_orders; --", "metric": "gmv"},
            max_limit=100,
        )


def test_missing_required_identifier_param_raises():
    with pytest.raises(QueryTemplateError, match="metric"):
        resolve_query(TOP_N_SQL, TOP_N_SPEC, {"dimension": "customer_state"}, max_limit=100)


def test_limit_defaults_when_absent():
    _, params = resolve_query(
        TOP_N_SQL,
        TOP_N_SPEC,
        {"dimension": "customer_state", "metric": "gmv"},
        max_limit=100,
    )
    assert params["limit"] == 10


def test_limit_is_capped_at_template_max_limit_even_if_caller_asks_for_more():
    _, params = resolve_query(
        TOP_N_SQL,
        TOP_N_SPEC,
        {"dimension": "customer_state", "metric": "gmv", "limit": 99999},
        max_limit=100,
    )
    assert params["limit"] == 100


def test_optional_date_params_default_to_none():
    _, params = resolve_query(
        TOP_N_SQL,
        TOP_N_SPEC,
        {"dimension": "customer_state", "metric": "gmv"},
        max_limit=100,
    )
    assert params["date_from"] is None
    assert params["date_to"] is None


def test_date_param_must_be_iso_format():
    with pytest.raises(QueryTemplateError, match="date_from"):
        resolve_query(
            TOP_N_SQL,
            TOP_N_SPEC,
            {"dimension": "customer_state", "metric": "gmv", "date_from": "not-a-date"},
            max_limit=100,
        )


def test_integer_param_below_min_raises():
    with pytest.raises(QueryTemplateError, match="limit"):
        resolve_query(
            TOP_N_SQL,
            TOP_N_SPEC,
            {"dimension": "customer_state", "metric": "gmv", "limit": 0},
            max_limit=100,
        )


def test_integer_param_accepts_numeric_string():
    _, params = resolve_query(
        TOP_N_SQL,
        TOP_N_SPEC,
        {"dimension": "customer_state", "metric": "gmv", "limit": "25"},
        max_limit=100,
    )
    assert params["limit"] == 25


GRAIN_SPEC = [
    {
        "name": "date_grain",
        "kind": "value",
        "type": "string",
        "required": True,
        "enum": ["month", "quarter"],
    },
]
GRAIN_SQL = "select date_trunc(%(date_grain)s, o.order_purchase_timestamp) from gold.fct_orders o"


def test_value_param_with_list_enum_binds_normally_not_via_format():
    """date_trunc()'s grain argument is an ordinary value, not an identifier --
    it should go through psycopg2 binding with an enum check, not str.format()."""

    sql, params = resolve_query(GRAIN_SQL, GRAIN_SPEC, {"date_grain": "month"}, max_limit=100)
    assert sql == GRAIN_SQL  # untouched -- no {placeholder} substitution happened
    assert params == {"date_grain": "month"}


def test_value_param_with_list_enum_rejects_value_outside_enum():
    with pytest.raises(QueryTemplateError, match="date_grain"):
        resolve_query(GRAIN_SQL, GRAIN_SPEC, {"date_grain": "week"}, max_limit=100)
