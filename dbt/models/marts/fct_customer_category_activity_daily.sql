{#
    DROP + plain CREATE below, not CREATE ... IF NOT EXISTS -- see
    dim_location.sql's comment for why: on this materialized='table'
    model, "IF NOT EXISTS" silently no-ops against the new live table every
    rebuild (the name is already claimed by the outgoing table's
    renamed-to-backup copy), and dbt's cleanup then deletes that copy via
    CASCADE, leaving no index at all. All DROPs below are schema-qualified
    explicitly, not left to resolve via the connection's search_path --
    see dim_location.sql.
#}
{{ config(
    post_hook=[
        'drop index if exists "{{ this.schema }}".idx_customer_category_daily_grain',
        "create unique index idx_customer_category_daily_grain on {{ this }} (activity_date, customer_unique_id, category_name)",
        'drop index if exists "{{ this.schema }}".idx_customer_category_daily_category',
        "create index idx_customer_category_daily_category on {{ this }} (activity_date, category_name)",
        'drop index if exists "{{ this.schema }}".idx_customer_category_daily_customer',
        "create index idx_customer_category_daily_customer on {{ this }} (customer_unique_id, activity_date)"
    ]
) }}

select
    customer_unique_id,
    category_name,
    activity_date,
    item_count,
    item_value,
    freight_value,
    total_value,
    first_purchase_ts,
    latest_purchase_ts,
    created_ts,
    '{{ run_started_at }}'::timestamptz as updated_ts
from {{ ref('int_customer_category_activity') }}
