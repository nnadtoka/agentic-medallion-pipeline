import pytest

from pipeline.configs.olist_flows import BACKFILL_TABLES, DAILY_INGESTION_TABLES
from pipeline.prefect_flows.common import validate_flow_tables


def test_flow_table_groups_are_complete_and_disjoint():
    assert len(DAILY_INGESTION_TABLES) == 2
    assert len(BACKFILL_TABLES) == 7
    assert set(DAILY_INGESTION_TABLES).isdisjoint(BACKFILL_TABLES)


def test_explicit_subset_is_validated():
    assert validate_flow_tables(
        ("raw_orders",), DAILY_INGESTION_TABLES
    ) == ("raw_orders",)

    with pytest.raises(ValueError, match="not configured"):
        validate_flow_tables(("raw_customers",), DAILY_INGESTION_TABLES)


def test_empty_and_duplicate_table_selections_are_rejected():
    with pytest.raises(ValueError, match="At least one"):
        validate_flow_tables((), DAILY_INGESTION_TABLES)

    with pytest.raises(ValueError, match="Duplicate"):
        validate_flow_tables(
            ("raw_orders", "raw_orders"), DAILY_INGESTION_TABLES
        )
