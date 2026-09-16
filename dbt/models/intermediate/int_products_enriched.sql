select
    p.product_id,
    p.product_category_name,
    t.product_category_name_english,
    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    greatest(p.created_ts, t.created_ts) as created_ts,
    p.src_batch_id
from {{ ref('stg_olist_products') }} p
left join {{ ref('stg_olist_category_translation') }} t
    using (product_category_name)
