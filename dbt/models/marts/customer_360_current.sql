{{ config(materialized='view') }}

with latest_snapshot as (
    select max(as_of_date) as as_of_date
    from {{ ref('agg_customer_lifetime_value') }}
)
select
    lv.as_of_date,
    lv.customer_unique_id,
    lv.customer_profile_id,
    lv.location_id,
    l.zip_code_prefix,
    l.city,
    l.state,
    lv.first_order_ts,
    lv.latest_order_ts,
    lv.order_count,
    lv.delivered_order_count,
    lv.canceled_order_count,
    lv.item_value,
    lv.freight_value,
    lv.gross_order_value,
    lv.payment_value,
    lv.average_order_value,
    lv.average_review_score,
    lv.days_since_last_order,
    lv.customer_tenure_days,
    lv.active_month_count,
    lv.distinct_product_count,
    lv.distinct_category_count,
    lv.distinct_merchant_count,
    lv.repeat_customer,
    interests.top_category_7d,
    interests.top_category_30d,
    interests.item_count_7d,
    interests.item_value_7d,
    interests.item_count_30d,
    interests.item_value_30d,
    interests.categories,
    interests.activity_by_day_7d,
    lv.snapshot_created_ts as lifetime_snapshot_created_ts,
    interests.snapshot_created_ts as interests_snapshot_created_ts
from {{ ref('agg_customer_lifetime_value') }} lv
join latest_snapshot s using (as_of_date)
left join {{ ref('dim_location') }} l using (location_id)
left join {{ ref('agg_customer_interests') }} interests
  on interests.as_of_date = lv.as_of_date
 and interests.customer_unique_id = lv.customer_unique_id
