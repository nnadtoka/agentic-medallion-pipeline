select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    created_ts,
    batch_id as src_batch_id,
    source as source_system
from {{ source('bronze', 'raw_customers') }}
