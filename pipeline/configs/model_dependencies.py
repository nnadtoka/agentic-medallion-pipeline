"""Source-batch dependencies for incremental dbt marts."""


SOURCE_PIPELINE_NAME = "olist_ingestion"
TRANSFORMATION_PIPELINE_NAME = "olist_transform_quality"


# Dataset names match bronze.ingestion_batches.dataset_name. Dimensions are
# included so their promoted source lineage is tracked alongside the facts.
MODEL_SOURCES = {
    "dim_customer": ("customers", "orders"),
    "dim_product": ("products", "product_category_translation"),
    "dim_seller": ("sellers",),
    "dim_date": ("orders",),
    "fct_orders": (
        "orders",
        "order_items",
        "order_payments",
        "order_reviews",
    ),
    "fct_order_items": (
        "order_items",
        "orders",
    ),
    "fct_order_payments": (
        "order_payments",
        "orders",
    ),
}

INCREMENTAL_FACTS = (
    "fct_orders",
    "fct_order_items",
    "fct_order_payments",
)
