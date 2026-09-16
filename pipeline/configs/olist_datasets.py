"""Olist source-to-Bronze dataset configuration."""


# This allowlist controls both supported datasets and safe SQL identifiers.
ALLOWED_DATASETS = {
    "raw_orders": {
        "dataset_name": "orders",
        "source_file": "olist_orders_dataset.csv",
        "load_strategy": "incremental",
        "source_date_column": "order_purchase_timestamp",
        "partition_column": "order_purchase_date",
    },
    "raw_order_items": {
        "dataset_name": "order_items",
        "source_file": "olist_order_items_dataset.csv",
        "load_strategy": "full_refresh",
    },
    "raw_order_reviews": {
        "dataset_name": "order_reviews",
        "source_file": "olist_order_reviews_dataset.csv",
        "load_strategy": "incremental",
        "source_date_column": "review_creation_date",
        "partition_column": "review_creation_date",
    },
    "raw_customers": {
        "dataset_name": "customers",
        "source_file": "olist_customers_dataset.csv",
        "load_strategy": "full_refresh",
    },
    "raw_sellers": {
        "dataset_name": "sellers",
        "source_file": "olist_sellers_dataset.csv",
        "load_strategy": "full_refresh",
    },
    "raw_products": {
        "dataset_name": "products",
        "source_file": "olist_products_dataset.csv",
        "load_strategy": "full_refresh",
    },
    "raw_geolocation": {
        "dataset_name": "geolocation",
        "source_file": "olist_geolocation_dataset.csv",
        "load_strategy": "full_refresh",
    },
    "raw_order_payments": {
        "dataset_name": "order_payments",
        "source_file": "olist_order_payments_dataset.csv",
        "load_strategy": "full_refresh",
    },
    "raw_product_category_translation": {
        "dataset_name": "product_category_translation",
        "source_file": "product_category_name_translation.csv",
        "load_strategy": "full_refresh",
    },
}
