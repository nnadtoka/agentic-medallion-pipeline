select
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    created_ts,
    src_batch_id
from {{ ref('int_sellers') }}
