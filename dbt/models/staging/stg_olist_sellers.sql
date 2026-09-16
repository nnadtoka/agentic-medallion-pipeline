select
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    created_ts,
    batch_id as src_batch_id,
    source as source_system
from {{ source('bronze', 'raw_sellers') }}
