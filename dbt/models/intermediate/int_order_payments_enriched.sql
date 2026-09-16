select
    p.order_id,
    p.payment_sequential,
    to_char(o.order_purchase_date, 'YYYYMMDD')::integer as purchase_date_key,
    p.payment_type,
    p.payment_installments,
    p.payment_value,
    p.created_ts,
    p.src_batch_id
from {{ ref('stg_olist_order_payments') }} p
left join {{ ref('stg_olist_orders') }} o using (order_id)
