select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    created_ts,
    batch_id as src_batch_id,
    source as source_system
from {{ source('bronze', 'raw_order_items') }}
