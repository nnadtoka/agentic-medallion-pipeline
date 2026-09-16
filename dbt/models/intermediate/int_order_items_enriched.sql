select
    i.order_id,
    i.order_item_id,
    i.product_id,
    i.seller_id,
    to_char(o.order_purchase_date, 'YYYYMMDD')::integer as purchase_date_key,
    i.shipping_limit_date,
    i.price,
    i.freight_value,
    i.price + i.freight_value as total_item_value,
    i.created_ts,
    i.src_batch_id
from {{ ref('stg_olist_order_items') }} i
left join {{ ref('stg_olist_orders') }} o using (order_id)
