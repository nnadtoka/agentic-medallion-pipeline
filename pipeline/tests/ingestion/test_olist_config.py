from pipeline.configs.olist_datasets import ALLOWED_DATASETS


def test_all_olist_datasets_have_an_ingestion_strategy():
    assert len(ALLOWED_DATASETS) == 9
    assert {
        config["load_strategy"]
        for config in ALLOWED_DATASETS.values()
    } == {"incremental", "full_refresh"}


def test_only_event_dated_sources_are_incremental():
    incremental_tables = {
        table_name
        for table_name, config in ALLOWED_DATASETS.items()
        if config["load_strategy"] == "incremental"
    }

    assert incremental_tables == {"raw_orders", "raw_order_reviews"}
    assert ALLOWED_DATASETS["raw_order_items"]["load_strategy"] == "full_refresh"
