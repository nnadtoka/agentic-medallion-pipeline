select
    product_id,
    product_category_name,
    product_name_lenght as product_name_length,
    product_description_lenght as product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    created_ts,
    batch_id as src_batch_id,
    source as source_system
from {{ source('bronze', 'raw_products') }}
