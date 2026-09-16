select
    c.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    h.first_order_ts,
    h.latest_order_ts,
    coalesce(h.order_count, 0) as order_count,
    greatest(c.created_ts, h.created_ts) as created_ts,
    c.src_batch_id
from {{ ref('stg_olist_customers') }} c
left join {{ ref('int_customer_order_history') }} h using (customer_id)
