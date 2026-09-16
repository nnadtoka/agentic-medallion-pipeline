import json

import pytest

from pipeline.catalog_sync.manifest import load_gold_descriptions


def _write_manifest(tmp_path, nodes):
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps({"nodes": nodes}))
    return manifest_path


def test_missing_manifest_raises(tmp_path):
    with pytest.raises(FileNotFoundError, match="dbt manifest not found"):
        load_gold_descriptions(tmp_path / "does_not_exist.json")


def test_only_marts_models_are_included(tmp_path):
    # Marts have their own dedicated candidate schema (dbt_project.yml:
    # marts: +schema: marts_candidate); promotion into `gold` happens
    # out-of-band afterward, so the manifest never says schema="gold".
    manifest_path = _write_manifest(
        tmp_path,
        {
            "model.project.dim_customer": {
                "resource_type": "model",
                "schema": "marts_candidate",
                "name": "dim_customer",
                "description": "One row per customer.",
                "columns": {},
            },
            "model.project.stg_olist_customers": {
                "resource_type": "model",
                "schema": "staging",
                "name": "stg_olist_customers",
                "description": "Should be ignored.",
                "columns": {},
            },
            "model.project.int_customer_order_history": {
                "resource_type": "model",
                "schema": "staging",
                "name": "int_customer_order_history",
                "description": "Should be ignored.",
                "columns": {},
            },
            "source.project.bronze.raw_customers": {
                "resource_type": "source",
                "schema": "bronze",
                "name": "raw_customers",
                "description": "Should be ignored.",
            },
        },
    )

    records = load_gold_descriptions(manifest_path)

    assert len(records) == 1
    assert records[0] == {
        "object_type": "table",
        "schema_name": "gold",
        "table_name": "dim_customer",
        "column_name": None,
        "description": "One row per customer.",
    }


def test_empty_descriptions_are_skipped(tmp_path):
    manifest_path = _write_manifest(
        tmp_path,
        {
            "model.project.dim_product": {
                "resource_type": "model",
                "schema": "marts_candidate",
                "name": "dim_product",
                "description": "",
                "columns": {
                    "product_id": {"description": ""},
                    "created_ts": {"description": "  "},
                },
            },
        },
    )

    assert load_gold_descriptions(manifest_path) == []


def test_table_and_column_descriptions_both_captured(tmp_path):
    manifest_path = _write_manifest(
        tmp_path,
        {
            "model.project.dim_seller": {
                "resource_type": "model",
                "schema": "marts_candidate",
                "name": "dim_seller",
                "description": "One row per seller.",
                "columns": {
                    "seller_id": {"description": "Natural key for the seller."},
                    "created_ts": {"description": ""},
                },
            },
        },
    )

    records = load_gold_descriptions(manifest_path)

    assert records == [
        {
            "object_type": "table",
            "schema_name": "gold",
            "table_name": "dim_seller",
            "column_name": None,
            "description": "One row per seller.",
        },
        {
            "object_type": "column",
            "schema_name": "gold",
            "table_name": "dim_seller",
            "column_name": "seller_id",
            "description": "Natural key for the seller.",
        },
    ]
