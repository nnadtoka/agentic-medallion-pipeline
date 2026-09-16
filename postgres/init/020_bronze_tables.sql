-- ============================================================
-- Bronze layer: Olist raw source tables
-- ============================================================

SET search_path TO bronze;


-- ============================================================
-- Orders
-- Source: olist_orders_dataset.csv
-- ============================================================

CREATE TABLE IF NOT EXISTS raw_orders (
    order_id                       TEXT,
    customer_id                    TEXT,
    order_status                   TEXT,
    order_purchase_timestamp       TIMESTAMP,
    order_purchase_date            DATE,
    order_approved_at              TIMESTAMP,
    order_delivered_carrier_date   TIMESTAMP,
    order_delivered_customer_date  TIMESTAMP,
    order_estimated_delivery_date  TIMESTAMP,

    created_ts                     TIMESTAMPTZ NOT NULL,
    batch_id                       TEXT NOT NULL,
    source                         TEXT NOT NULL
);


-- ============================================================
-- Order items
-- Source: olist_order_items_dataset.csv
-- ============================================================

CREATE TABLE IF NOT EXISTS raw_order_items (
    order_id            TEXT,
    order_item_id       INTEGER,
    product_id          TEXT,
    seller_id           TEXT,
    shipping_limit_date TIMESTAMP,
    price               NUMERIC(12, 2),
    freight_value       NUMERIC(12, 2),

    created_ts          TIMESTAMPTZ NOT NULL,
    batch_id             TEXT NOT NULL,
    source               TEXT NOT NULL
);


-- ============================================================
-- Order payments
-- Source: olist_order_payments_dataset.csv
-- ============================================================

CREATE TABLE IF NOT EXISTS raw_order_payments (
    order_id             TEXT,
    payment_sequential    INTEGER,
    payment_type         TEXT,
    payment_installments INTEGER,
    payment_value        NUMERIC(12, 2),

    created_ts           TIMESTAMPTZ NOT NULL,
    batch_id             TEXT NOT NULL,
    source               TEXT NOT NULL
);


-- ============================================================
-- Order reviews
-- Source: olist_order_reviews_dataset.csv
-- ============================================================

CREATE TABLE IF NOT EXISTS raw_order_reviews (
    review_id              TEXT,
    order_id               TEXT,
    review_score           INTEGER,
    review_comment_title   TEXT,
    review_comment_message TEXT,
    review_creation_date   TIMESTAMP,
    review_answer_timestamp TIMESTAMP,

    created_ts             TIMESTAMPTZ NOT NULL,
    batch_id               TEXT NOT NULL,
    source                 TEXT NOT NULL
);


-- ============================================================
-- Customers
-- Source: olist_customers_dataset.csv
-- ============================================================

CREATE TABLE IF NOT EXISTS raw_customers (
    customer_id              TEXT,
    customer_unique_id       TEXT,
    customer_zip_code_prefix INTEGER,
    customer_city            TEXT,
    customer_state           TEXT,

    created_ts               TIMESTAMPTZ NOT NULL,
    batch_id                 TEXT NOT NULL,
    source                   TEXT NOT NULL
);


-- ============================================================
-- Products
-- Source: olist_products_dataset.csv
--
-- Preserve Olist's original "lenght" spelling in Bronze.
-- These will be renamed in dbt staging.
-- ============================================================

CREATE TABLE IF NOT EXISTS raw_products (
    product_id                    TEXT,
    product_category_name         TEXT,
    product_name_lenght           INTEGER,
    product_description_lenght    INTEGER,
    product_photos_qty            INTEGER,
    product_weight_g              INTEGER,
    product_length_cm             INTEGER,
    product_height_cm             INTEGER,
    product_width_cm              INTEGER,

    created_ts                    TIMESTAMPTZ NOT NULL,
    batch_id                      TEXT NOT NULL,
    source                        TEXT NOT NULL
);


-- ============================================================
-- Sellers
-- Source: olist_sellers_dataset.csv
-- ============================================================

CREATE TABLE IF NOT EXISTS raw_sellers (
    seller_id              TEXT,
    seller_zip_code_prefix INTEGER,
    seller_city            TEXT,
    seller_state           TEXT,

    created_ts             TIMESTAMPTZ NOT NULL,
    batch_id               TEXT NOT NULL,
    source                 TEXT NOT NULL
);


-- ============================================================
-- Geolocation
-- Source: olist_geolocation_dataset.csv
-- ============================================================

CREATE TABLE IF NOT EXISTS raw_geolocation (
    geolocation_zip_code_prefix INTEGER,
    geolocation_lat              NUMERIC,
    geolocation_lng              NUMERIC,
    geolocation_city             TEXT,
    geolocation_state            TEXT,

    created_ts                   TIMESTAMPTZ NOT NULL,
    batch_id                     TEXT NOT NULL,
    source                       TEXT NOT NULL
);


-- ============================================================
-- Product category translation
-- Source: product_category_name_translation.csv
-- ============================================================

CREATE TABLE IF NOT EXISTS raw_product_category_translation (
    product_category_name         TEXT,
    product_category_name_english TEXT,

    created_ts                    TIMESTAMPTZ NOT NULL,
    batch_id                      TEXT NOT NULL,
    source                        TEXT NOT NULL
);
