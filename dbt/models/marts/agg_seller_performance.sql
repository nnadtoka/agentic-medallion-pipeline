{#
    DROP + plain CREATE below, not CREATE ... IF NOT EXISTS -- see
    dim_location.sql's comment for why: on this materialized='table' model,
    "IF NOT EXISTS" would silently no-op against the new live table every
    rebuild (the name is already claimed by the outgoing table's
    renamed-to-backup copy), and dbt's cleanup then deletes that copy via
    CASCADE, leaving no index at all. The DROP is schema-qualified
    explicitly, not left to resolve via the connection's search_path.
#}
{{ config(
    post_hook=[
        'drop index if exists "{{ this.schema }}".idx_seller_performance_id',
        "create unique index idx_seller_performance_id on {{ this }} (seller_id)"
    ]
) }}

-- Every metric except canceled_order_count excludes canceled orders, the
-- same fix applied to agg_customer_lifetime_value.sql's order_metrics CTE:
-- mixing canceled-inclusive and canceled-exclusive numbers on one row
-- produces "has GMV but bought nothing" contradictions. avg_review_score
-- and single_seller_order_count additionally restrict to orders where this
-- seller is the *only* seller (o.distinct_seller_count = 1) -- reviews are
-- attached to the order, not to a specific seller, so a multi-seller
-- order's review can't be honestly attributed to any one of them.
-- reviewed_single_seller_order_count is the exact denominator
-- avg_review_score uses. Reviews must first be reduced to seller-order grain:
-- averaging them after the item join would give orders with more items more
-- weight than orders with fewer items.
with aggregated as (
    select
        i.seller_id,
        count(distinct o.order_id) filter (where o.order_status <> 'canceled')::bigint as order_count,
        count(distinct o.order_id) filter (where o.order_status = 'canceled')::bigint
            as canceled_order_count,
        count(*) filter (where o.order_status <> 'canceled')::bigint as item_count,
        -- sum(...) filter (...), unlike count(...) filter (...), returns
        -- NULL rather than 0 when zero rows match -- a seller whose every
        -- order was canceled has no non-canceled rows to sum, so this needs
        -- coalescing explicitly, the same reason dim_customer.sql/
        -- agg_customer_lifetime_value.sql coalesce their LEFT JOIN'd sums.
        coalesce(sum(i.price) filter (where o.order_status <> 'canceled'), 0) as gmv,
        coalesce(sum(i.freight_value) filter (where o.order_status <> 'canceled'), 0)
            as freight_total,
        count(distinct i.product_id) filter (where o.order_status <> 'canceled')::bigint
            as distinct_product_count,
        count(distinct coalesce(p.product_category_name_english, p.product_category_name, 'unknown'))
            filter (where o.order_status <> 'canceled')::bigint as distinct_category_count,
        count(distinct c.customer_unique_id) filter (where o.order_status <> 'canceled')::bigint
            as distinct_customer_count,
        count(distinct c.customer_state) filter (where o.order_status <> 'canceled')::bigint
            as distinct_customer_state_count,
        count(distinct o.order_id)
            filter (where o.distinct_seller_count = 1 and o.order_status <> 'canceled')::bigint
            as single_seller_order_count,
        min(o.order_purchase_timestamp) filter (where o.order_status <> 'canceled') as first_order_ts,
        max(o.order_purchase_timestamp) filter (where o.order_status <> 'canceled') as latest_order_ts,
        max(o.updated_ts) as source_updated_ts
    from {{ ref('fct_order_items') }} i
    join {{ ref('fct_orders') }} o using (order_id)
    join {{ ref('dim_customer') }} c using (customer_id)
    left join {{ ref('dim_product') }} p using (product_id)
    group by i.seller_id
), seller_orders as (
    select distinct
        i.seller_id,
        o.order_id,
        o.average_review_score
    from {{ ref('fct_order_items') }} i
    join {{ ref('fct_orders') }} o using (order_id)
    where o.distinct_seller_count = 1
      and o.order_status <> 'canceled'
), review_metrics as (
    select
        seller_id,
        count(average_review_score)::bigint as reviewed_single_seller_order_count,
        avg(average_review_score) as avg_review_score
    from seller_orders
    group by seller_id
)
select
    seller_id,
    order_count,
    canceled_order_count,
    item_count,
    gmv,
    freight_total,
    freight_total / nullif(gmv, 0) as freight_ratio,
    distinct_product_count,
    distinct_category_count,
    distinct_customer_count,
    distinct_customer_state_count,
    single_seller_order_count,
    coalesce(r.reviewed_single_seller_order_count, 0)::bigint
        as reviewed_single_seller_order_count,
    r.avg_review_score,
    first_order_ts,
    latest_order_ts,
    source_updated_ts,
    '{{ run_started_at }}'::timestamptz as updated_ts
from aggregated a
left join review_metrics r using (seller_id)
