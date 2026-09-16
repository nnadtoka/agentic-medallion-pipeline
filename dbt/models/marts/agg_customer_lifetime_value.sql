{#
    DROP + plain CREATE below, not CREATE ... IF NOT EXISTS -- see
    dim_location.sql's comment for why: on this materialized='table'
    model, "IF NOT EXISTS" silently no-ops against the new live table every
    rebuild (the name is already claimed by the outgoing table's
    renamed-to-backup copy), and dbt's cleanup then deletes that copy via
    CASCADE, leaving no index at all. The DROP is schema-qualified
    explicitly, not left to resolve via the connection's search_path --
    see dim_location.sql.
#}
{{ config(
    post_hook=[
        'drop index if exists "{{ this.schema }}".idx_customer_lifetime_snapshot_grain',
        "create unique index idx_customer_lifetime_snapshot_grain on {{ this }} (as_of_date, customer_unique_id)"
    ]
) }}

-- depends_on: {{ ref('fct_orders') }}
{% set as_of_date_override = var('as_of_date', none) %}

with parameters as (
    -- Defaults to the most recent order date actually in the data, not
    -- real wall-clock time (run_started_at) -- this is a static historical
    -- dataset (every order currently ingested is from Oct-Nov 2017), so
    -- trusting wall-clock "today" would make every customer's
    -- days_since_last_order read ~3,200+ regardless of real behavior, and
    -- agg_customer_interests' 7d/30d rolling windows (which must agree with
    -- this model's as_of_date -- see that model and customer_360_current,
    -- which joins the two on it) would always land on dates with zero
    -- matching activity. Pass `--vars 'as_of_date: YYYY-MM-DD'` to backfill
    -- a specific historical partition instead of this default.
    {% if as_of_date_override %}
    select '{{ as_of_date_override }}'::date as as_of_date
    {% else %}
    select coalesce(max(order_purchase_timestamp)::date, current_date) as as_of_date
    from {{ ref('fct_orders') }}
    {% endif %}
), customers as (
    select distinct customer_unique_id
    from {{ ref('stg_olist_customers') }}
    where customer_unique_id is not null
), order_metrics as (
    -- canceled_order_count needs to see canceled orders to count them, so
    -- the base row set below stays unfiltered by status -- every other
    -- aggregate instead FILTERs canceled orders out individually, the same
    -- way delivered_order_count/canceled_order_count already do, so an
    -- order that never completed doesn't count toward order_count,
    -- first/latest_order_ts, or any dollar figure. Matches item_metrics'
    -- existing `and o.order_status <> 'canceled'` exclusion below -- before
    -- this fix the two CTEs disagreed, so a customer whose only order was
    -- canceled showed nonzero lifetime_value but zero distinct_product_count.
    select
        c.customer_unique_id,
        min(o.order_purchase_timestamp) filter (where o.order_status <> 'canceled') as first_order_ts,
        max(o.order_purchase_timestamp) filter (where o.order_status <> 'canceled') as latest_order_ts,
        count(distinct o.order_id) filter (where o.order_status <> 'canceled')::bigint as order_count,
        count(distinct o.order_id) filter (where o.order_status = 'delivered')::bigint as delivered_order_count,
        count(distinct o.order_id) filter (where o.order_status = 'canceled')::bigint as canceled_order_count,
        sum(o.item_value) filter (where o.order_status <> 'canceled') as item_value,
        sum(o.freight_value) filter (where o.order_status <> 'canceled') as freight_value,
        sum(o.order_item_value) filter (where o.order_status <> 'canceled') as gross_order_value,
        sum(o.payment_value) filter (where o.order_status <> 'canceled') as payment_value,
        avg(o.average_review_score)
            filter (where o.average_review_score is not null and o.order_status <> 'canceled')
            as average_review_score,
        count(distinct date_trunc('month', o.order_purchase_timestamp))
            filter (where o.order_status <> 'canceled')::bigint as active_month_count,
        max(o.updated_ts) as source_updated_ts
    from {{ ref('fct_orders') }} o
    join {{ ref('dim_customer') }} c using (customer_id)
    cross join parameters p
    where o.order_purchase_timestamp < p.as_of_date + interval '1 day'
    group by 1
), item_metrics as (
    select
        c.customer_unique_id,
        count(distinct i.product_id)::bigint as distinct_product_count,
        count(distinct coalesce(p.product_category_name_english, p.product_category_name, 'unknown'))::bigint
            as distinct_category_count,
        count(distinct i.seller_id)::bigint as distinct_merchant_count
    from {{ ref('fct_order_items') }} i
    join {{ ref('fct_orders') }} o using (order_id)
    join {{ ref('dim_customer') }} c using (customer_id)
    left join {{ ref('dim_product') }} p using (product_id)
    cross join parameters x
    where o.order_purchase_timestamp < x.as_of_date + interval '1 day'
      and o.order_status <> 'canceled'
    group by 1
), current_profile as (
    select distinct on (p.customer_unique_id)
        p.customer_unique_id,
        p.customer_profile_id,
        p.location_id
    from {{ ref('dim_unique_customer_profile') }} p
    cross join parameters x
    where p.version_start_ts < x.as_of_date + interval '1 day'
    order by p.customer_unique_id, p.version_start_ts desc
)
select
    p.as_of_date,
    c.customer_unique_id,
    cp.customer_profile_id,
    cp.location_id,
    o.first_order_ts,
    o.latest_order_ts,
    coalesce(o.order_count, 0) as order_count,
    coalesce(o.delivered_order_count, 0) as delivered_order_count,
    coalesce(o.canceled_order_count, 0) as canceled_order_count,
    coalesce(o.item_value, 0) as item_value,
    coalesce(o.freight_value, 0) as freight_value,
    coalesce(o.gross_order_value, 0) as gross_order_value,
    coalesce(o.payment_value, 0) as payment_value,
    case when coalesce(o.order_count, 0) > 0
         then round(o.gross_order_value / o.order_count, 2) else 0 end as average_order_value,
    o.average_review_score,
    case when o.latest_order_ts is not null then p.as_of_date - o.latest_order_ts::date end
        as days_since_last_order,
    case when o.first_order_ts is not null then p.as_of_date - o.first_order_ts::date end
        as customer_tenure_days,
    coalesce(o.active_month_count, 0) as active_month_count,
    coalesce(i.distinct_product_count, 0) as distinct_product_count,
    coalesce(i.distinct_category_count, 0) as distinct_category_count,
    coalesce(i.distinct_merchant_count, 0) as distinct_merchant_count,
    coalesce(o.order_count, 0) > 1 as repeat_customer,
    '{{ run_started_at }}'::timestamptz as snapshot_created_ts,
    o.source_updated_ts
from customers c
cross join parameters p
left join order_metrics o using (customer_unique_id)
left join item_metrics i using (customer_unique_id)
left join current_profile cp using (customer_unique_id)
