-- Semantic constraints not_null/unique/non_negative tests can't express:
-- single_seller_order_count is a subset of order_count by construction, a
-- product belongs to exactly one category so distinct_category_count can
-- never exceed distinct_product_count, first/latest_order_ts must be
-- present exactly when order_count > 0, and a review score is on a 1-5
-- scale.
select seller_id
from {{ ref('agg_seller_performance') }}
where single_seller_order_count > order_count
   or distinct_category_count > distinct_product_count
   or (order_count = 0 and (first_order_ts is not null or latest_order_ts is not null))
   or (order_count > 0 and (first_order_ts is null or latest_order_ts is null))
   or (avg_review_score is not null and (avg_review_score < 1 or avg_review_score > 5))
