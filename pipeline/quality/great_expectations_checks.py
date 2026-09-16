"""Great Expectations quality gates for candidate models in PostgreSQL.

Two gates, run at two different points in the DAG (see
pipeline/prefect_flows/transformation_flow.py):

- `validate_candidate_intermediate` runs after `dbt build --select staging
  intermediate`, before marts are built at all.
- `validate_candidate_marts` runs after `dbt build --select marts`, before
  gold promotion.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote_plus


class QualityGateError(RuntimeError):
    """Raised when one or more Great Expectations checks fail."""

    def __init__(self, failures: list[dict[str, Any]]):
        self.failures = failures
        names = [
            f"{failure['table_name']}.{failure['expectation_type']}"
            for failure in failures
        ]
        super().__init__("Great Expectations checks failed: " + ", ".join(names))


@dataclass(frozen=True)
class TableValidationSummary:
    """Validation totals for one table."""

    table_name: str
    checks_run: int
    checks_passed: int


def _json_safe(value: Any) -> Any:
    """Normalize Great Expectations result values for JSONB persistence."""

    return json.loads(json.dumps(value, default=str))


def _expectation_configuration(expectation: object) -> dict[str, Any]:
    """Return JSON-safe expectation settings such as columns and limits."""

    configuration = getattr(expectation, "configuration", None)
    if configuration is not None and hasattr(configuration, "to_json_dict"):
        return _json_safe(configuration.to_json_dict())
    if hasattr(expectation, "to_json_dict"):
        return _json_safe(expectation.to_json_dict())
    return {}


def postgres_connection_string() -> str:
    """Build an escaped SQLAlchemy connection string from dbt environment variables."""

    user = quote_plus(os.getenv("DBT_USER", "dbt_transformer"))
    password = quote_plus(os.environ["DBT_PASSWORD"])
    host = os.getenv("DBT_HOST", "127.0.0.1")
    port = int(os.getenv("DBT_PORT", "5433"))
    database = quote_plus(os.getenv("POSTGRES_DB", "analytics_db"))
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"


def intermediate_expectations() -> dict[str, list[object]]:
    """Return the expectations applied to each candidate intermediate model.

    Covers all intermediate models -- including the models that exist so
    dim_product/dim_seller/dim_date/fct_order_items/fct_order_payments have a
    testable upstream layer instead of computing their join/aggregation logic
    directly in the marts SQL (see pipeline/promotion/README.md for why that
    mattered).
    """

    import great_expectations as gx

    not_empty = gx.expectations.ExpectTableRowCountToBeBetween(min_value=1)
    return {
        "int_customer_order_history": [
            not_empty,
            gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"),
            gx.expectations.ExpectColumnValuesToBeUnique(column="customer_id"),
        ],
        "int_unique_customer_order_history": [
            not_empty,
            gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_unique_id"),
            gx.expectations.ExpectColumnValuesToBeUnique(column="customer_unique_id"),
            gx.expectations.ExpectColumnValuesToBeBetween(column="order_count", min_value=1),
        ],
        "int_customer_category_activity": [
            not_empty,
            gx.expectations.ExpectCompoundColumnsToBeUnique(
                column_list=["customer_unique_id", "category_name", "activity_date"]
            ),
            gx.expectations.ExpectColumnValuesToBeBetween(column="item_count", min_value=1),
            gx.expectations.ExpectColumnValuesToBeBetween(column="item_value", min_value=0),
            gx.expectations.ExpectColumnValuesToBeBetween(column="freight_value", min_value=0),
        ],
        "int_order_item_totals": [
            not_empty,
            gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id"),
            gx.expectations.ExpectColumnValuesToBeUnique(column="order_id"),
            gx.expectations.ExpectColumnValuesToBeBetween(column="item_count", min_value=0),
        ],
        "int_order_payment_totals": [
            not_empty,
            gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id"),
            gx.expectations.ExpectColumnValuesToBeUnique(column="order_id"),
            gx.expectations.ExpectColumnValuesToBeBetween(column="payment_value", min_value=0),
        ],
        "int_order_review_summary": [
            not_empty,
            gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id"),
            gx.expectations.ExpectColumnValuesToBeUnique(column="order_id"),
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="average_review_score", min_value=1, max_value=5
            ),
        ],
        "int_products_enriched": [
            not_empty,
            gx.expectations.ExpectColumnValuesToNotBeNull(column="product_id"),
            gx.expectations.ExpectColumnValuesToBeUnique(column="product_id"),
        ],
        "int_sellers": [
            not_empty,
            gx.expectations.ExpectColumnValuesToNotBeNull(column="seller_id"),
            gx.expectations.ExpectColumnValuesToBeUnique(column="seller_id"),
        ],
        "int_date_spine": [
            not_empty,
            gx.expectations.ExpectColumnValuesToNotBeNull(column="date_key"),
            gx.expectations.ExpectColumnValuesToBeUnique(column="date_key"),
            gx.expectations.ExpectColumnValuesToBeUnique(column="date_day"),
        ],
        "int_order_items_enriched": [
            not_empty,
            gx.expectations.ExpectCompoundColumnsToBeUnique(
                column_list=["order_id", "order_item_id"]
            ),
            gx.expectations.ExpectColumnValuesToBeBetween(column="price", min_value=0),
            gx.expectations.ExpectColumnValuesToBeBetween(column="freight_value", min_value=0),
        ],
        "int_order_payments_enriched": [
            not_empty,
            gx.expectations.ExpectCompoundColumnsToBeUnique(
                column_list=["order_id", "payment_sequential"]
            ),
            gx.expectations.ExpectColumnValuesToBeBetween(column="payment_value", min_value=0),
        ],
    }


def mart_expectations() -> dict[str, list[object]]:
    """Return the expectations applied to each candidate mart."""

    import great_expectations as gx

    not_empty = gx.expectations.ExpectTableRowCountToBeBetween(min_value=1)
    return {
        "dim_customer": [
            not_empty,
            gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"),
            gx.expectations.ExpectColumnValuesToBeUnique(column="customer_id"),
        ],
        "dim_product": [
            not_empty,
            gx.expectations.ExpectColumnValuesToNotBeNull(column="product_id"),
            gx.expectations.ExpectColumnValuesToBeUnique(column="product_id"),
        ],
        "dim_seller": [
            not_empty,
            gx.expectations.ExpectColumnValuesToNotBeNull(column="seller_id"),
            gx.expectations.ExpectColumnValuesToBeUnique(column="seller_id"),
        ],
        "dim_date": [
            not_empty,
            gx.expectations.ExpectColumnValuesToNotBeNull(column="date_key"),
            gx.expectations.ExpectColumnValuesToBeUnique(column="date_key"),
            gx.expectations.ExpectColumnValuesToBeUnique(column="date_day"),
        ],
        "dim_location": [
            not_empty,
            gx.expectations.ExpectColumnValuesToNotBeNull(column="location_id"),
            gx.expectations.ExpectColumnValuesToBeUnique(column="location_id"),
        ],
        "dim_unique_customer_profile": [
            not_empty,
            gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_profile_id"),
            gx.expectations.ExpectColumnValuesToBeUnique(column="customer_profile_id"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_unique_id"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="version_start_ts"),
        ],
        "agg_seller_performance": [
            not_empty,
            gx.expectations.ExpectColumnValuesToNotBeNull(column="seller_id"),
            gx.expectations.ExpectColumnValuesToBeUnique(column="seller_id"),
            gx.expectations.ExpectColumnValuesToBeBetween(column="order_count", min_value=0),
            gx.expectations.ExpectColumnValuesToBeBetween(column="gmv", min_value=0),
        ],
        "fct_orders": [
            not_empty,
            gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id"),
            gx.expectations.ExpectColumnValuesToBeUnique(column="order_id"),
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="order_status",
                value_set=[
                    "approved",
                    "canceled",
                    "created",
                    "delivered",
                    "invoiced",
                    "processing",
                    "shipped",
                    "unavailable",
                ],
            ),
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="average_review_score",
                min_value=1,
                max_value=5,
            ),
        ],
        "fct_order_items": [
            not_empty,
            gx.expectations.ExpectCompoundColumnsToBeUnique(
                column_list=["order_id", "order_item_id"]
            ),
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="price", min_value=0
            ),
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="freight_value", min_value=0
            ),
        ],
        "fct_order_payments": [
            not_empty,
            gx.expectations.ExpectCompoundColumnsToBeUnique(
                column_list=["order_id", "payment_sequential"]
            ),
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="payment_value", min_value=0
            ),
        ],
        "fct_customer_category_activity_daily": [
            not_empty,
            gx.expectations.ExpectCompoundColumnsToBeUnique(
                column_list=["activity_date", "customer_unique_id", "category_name"]
            ),
            gx.expectations.ExpectColumnValuesToBeBetween(column="item_count", min_value=1),
            gx.expectations.ExpectColumnValuesToBeBetween(column="item_value", min_value=0),
        ],
        "agg_customer_lifetime_value": [
            not_empty,
            gx.expectations.ExpectCompoundColumnsToBeUnique(
                column_list=["as_of_date", "customer_unique_id"]
            ),
            gx.expectations.ExpectColumnValuesToBeBetween(column="order_count", min_value=0),
            gx.expectations.ExpectColumnValuesToBeBetween(column="gross_order_value", min_value=0),
        ],
        "agg_customer_interests": [
            not_empty,
            gx.expectations.ExpectCompoundColumnsToBeUnique(
                column_list=["as_of_date", "customer_unique_id"]
            ),
            gx.expectations.ExpectColumnValuesToBeBetween(column="item_count_7d", min_value=0),
            gx.expectations.ExpectColumnValuesToBeBetween(column="item_count_30d", min_value=0),
        ],
        "customer_360_current": [
            not_empty,
            gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_unique_id"),
            gx.expectations.ExpectColumnValuesToBeUnique(column="customer_unique_id"),
            gx.expectations.ExpectColumnValuesToBeBetween(column="order_count", min_value=0),
        ],
    }


def _validate_expectations(
    data_source_name: str,
    expectations_by_table: dict[str, list[object]],
    schema_name: str,
) -> list[TableValidationSummary]:
    """Shared runner: validate every table's expectations, raise once at the end.

    A failure on one table does not stop the remaining tables from also
    being validated and reported -- the raised error names every failure,
    not just the first.
    """

    import great_expectations as gx

    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_postgres(
        name=data_source_name,
        connection_string=postgres_connection_string(),
    )

    summaries: list[TableValidationSummary] = []
    failures: list[dict[str, Any]] = []
    for table_name, expectations in expectations_by_table.items():
        asset = data_source.add_table_asset(
            name=table_name,
            table_name=table_name,
            schema_name=schema_name,
        )
        batch_definition = asset.add_batch_definition_whole_table(
            name="whole_table"
        )
        batch = batch_definition.get_batch()

        passed = 0
        for expectation in expectations:
            result = batch.validate(expectation)
            if result.success:
                passed += 1
            else:
                failures.append(
                    {
                        "table_name": table_name,
                        "expectation_type": expectation.__class__.__name__,
                        "configuration": _expectation_configuration(expectation),
                        "observed_result": _json_safe(
                            getattr(result, "result", {}) or {}
                        ),
                    }
                )
        summaries.append(
            TableValidationSummary(
                table_name=table_name,
                checks_run=len(expectations),
                checks_passed=passed,
            )
        )

    if failures:
        raise QualityGateError(failures)
    return summaries


def validate_candidate_intermediate(
    schema_name: str = "staging",
) -> list[TableValidationSummary]:
    """Validate all candidate intermediate models and raise on any failure."""

    return _validate_expectations(
        "candidate_intermediate", intermediate_expectations(), schema_name
    )


def validate_candidate_marts(
    schema_name: str = "marts_candidate",
) -> list[TableValidationSummary]:
    """Validate all candidate marts and raise when any expectation fails."""

    return _validate_expectations(
        "candidate_marts", mart_expectations(), schema_name
    )


def main() -> None:
    """Run both quality gates from the command line."""

    print("Intermediate:")
    for summary in validate_candidate_intermediate():
        print(
            f"  {summary.table_name}: "
            f"{summary.checks_passed}/{summary.checks_run} checks passed"
        )

    print("Marts:")
    for summary in validate_candidate_marts():
        print(
            f"  {summary.table_name}: "
            f"{summary.checks_passed}/{summary.checks_run} checks passed"
        )


if __name__ == "__main__":
    main()
