from datetime import date

import pandas as pd
import pytest

from pipeline.configs.olist_datasets import ALLOWED_DATASETS
from pipeline.ingestion.ingest_olist import (
    resolve_batch_date,
    select_rows_for_load,
)
from pipeline.ingestion.backfill_incremental import populated_dates


def test_incremental_orders_select_date_and_derive_purchase_date():
    source = pd.DataFrame(
        {
            "order_id": ["one", "two", "invalid"],
            "order_purchase_timestamp": [
                "2017-10-02 08:15:00",
                "2017-10-03 09:30:00",
                "not-a-date",
            ],
        }
    )

    selected = select_rows_for_load(
        source,
        ALLOWED_DATASETS["raw_orders"],
        date(2017, 10, 2),
    )

    assert selected["order_id"].tolist() == ["one"]
    assert selected["order_purchase_date"].tolist() == [date(2017, 10, 2)]
    assert "order_purchase_date" not in source.columns


def test_full_refresh_keeps_all_rows_and_source_date_fields():
    source = pd.DataFrame(
        {
            "order_id": ["one", "two"],
            "shipping_limit_date": [
                "2017-10-02 08:15:00",
                "2017-10-03 09:30:00",
            ],
        }
    )

    selected = select_rows_for_load(
        source,
        ALLOWED_DATASETS["raw_order_items"],
        date(2026, 9, 12),
    )

    pd.testing.assert_frame_equal(selected, source)
    assert selected is not source


def test_incremental_batch_date_is_required():
    with pytest.raises(ValueError, match="--batch-date is required"):
        resolve_batch_date("incremental", None, "raw_orders")


def test_full_refresh_accepts_an_explicit_snapshot_date():
    assert resolve_batch_date(
        "full_refresh",
        "2026-09-12",
        "raw_customers",
    ) == date(2026, 9, 12)


def test_populated_dates_returns_sorted_distinct_dates():
    source = pd.DataFrame(
        {
            "order_purchase_timestamp": [
                "2017-07-02 08:00:00",
                "2017-07-01 09:00:00",
                "2017-07-02 10:00:00",
            ]
        }
    )

    assert populated_dates(source, "raw_orders") == [
        date(2017, 7, 1),
        date(2017, 7, 2),
    ]


def test_populated_dates_rejects_invalid_source_dates():
    source = pd.DataFrame({"review_creation_date": ["2017-07-01", None]})

    with pytest.raises(ValueError, match="1 null or invalid"):
        populated_dates(source, "raw_order_reviews")
