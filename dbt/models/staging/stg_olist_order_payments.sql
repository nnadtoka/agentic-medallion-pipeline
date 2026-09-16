select
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    created_ts,
    batch_id as src_batch_id,
    source as source_system
from {{ source('bronze', 'raw_order_payments') }}
