select
    order_id,
    count(*)::bigint as review_count,
    avg(review_score)::numeric(10, 2) as average_review_score,
    min(review_score) as minimum_review_score,
    max(review_score) as maximum_review_score,
    min(review_creation_date) as first_review_date,
    max(created_ts) as created_ts,
    case when count(distinct src_batch_id) = 1 then min(src_batch_id) end as src_batch_id
from {{ ref('stg_olist_order_reviews') }}
group by order_id
