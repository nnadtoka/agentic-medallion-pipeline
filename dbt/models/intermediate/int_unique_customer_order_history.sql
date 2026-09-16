select
    customer_unique_id,
    count(distinct customer_id)::bigint as customer_id_count,
    min(first_order_ts) as first_order_ts,
    max(latest_order_ts) as latest_order_ts,
    sum(order_count)::bigint as order_count,
    max(created_ts) as created_ts,
    case
        when count(*) = count(src_batch_id)
         and count(distinct src_batch_id) = 1
            then min(src_batch_id)
    end as src_batch_id
from {{ ref('int_customer_order_history') }}
where customer_unique_id is not null
group by customer_unique_id
