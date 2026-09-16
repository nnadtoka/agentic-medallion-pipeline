"""Table membership for Olist Prefect flows."""

DAILY_INGESTION_TABLES = ("raw_orders", "raw_order_reviews")

BACKFILL_TABLES = (
    "raw_order_items",
    "raw_customers",
    "raw_sellers",
    "raw_products",
    "raw_geolocation",
    "raw_order_payments",
    "raw_product_category_translation",
)
