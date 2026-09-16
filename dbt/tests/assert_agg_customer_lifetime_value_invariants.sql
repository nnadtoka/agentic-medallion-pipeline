-- order_count now excludes canceled orders (see agg_customer_lifetime_value.sql's
-- order_metrics CTE), so delivered_order_count (a subset of non-canceled orders)
-- must never exceed it, but canceled_order_count is independent of it -- a
-- customer can have canceled orders on top of any number of non-canceled ones.
select customer_unique_id
from {{ ref('agg_customer_lifetime_value') }}
where delivered_order_count > order_count
   or distinct_category_count > distinct_product_count
   or repeat_customer <> (order_count > 1)
   or (order_count = 0 and (first_order_ts is not null or latest_order_ts is not null))
   or (order_count > 0 and (first_order_ts is null or latest_order_ts is null))
   or days_since_last_order < 0
   or customer_tenure_days < 0
