select
    o.customer_id,
    c.customer_unique_id,
    min(o.order_purchase_timestamp) as first_order_ts,
    max(o.order_purchase_timestamp) as latest_order_ts,
    count(*)::bigint as order_count,
    max(o.created_ts) as created_ts,
    case
        when count(distinct o.src_batch_id) = 1 then min(o.src_batch_id)
    end as src_batch_id
from {{ ref('stg_olist_orders') }} o
left join {{ ref('stg_olist_customers') }} c using (customer_id)
group by o.customer_id, c.customer_unique_id
