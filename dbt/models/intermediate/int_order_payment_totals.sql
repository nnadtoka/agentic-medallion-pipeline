select
    order_id,
    count(*)::bigint as payment_count,
    sum(payment_installments)::bigint as payment_installments,
    sum(payment_value) as payment_value,
    max(created_ts) as created_ts,
    case when count(distinct src_batch_id) = 1 then min(src_batch_id) end as src_batch_id
from {{ ref('stg_olist_order_payments') }}
group by order_id
