select
    product_category_name,
    product_category_name_english,
    created_ts,
    batch_id as src_batch_id,
    source as source_system
from {{ source('bronze', 'raw_product_category_translation') }}
