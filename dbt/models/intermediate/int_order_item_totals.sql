select
    order_id,
    count(*)::bigint as item_count,
    count(distinct product_id)::bigint as distinct_product_count,
    count(distinct seller_id)::bigint as distinct_seller_count,
    sum(price) as item_value,
    sum(freight_value) as freight_value,
    sum(price + freight_value) as order_item_value,
    max(created_ts) as created_ts,
    case when count(distinct src_batch_id) = 1 then min(src_batch_id) end as src_batch_id
from {{ ref('stg_olist_order_items') }}
group by order_id
