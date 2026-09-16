{{ config(
    materialized='incremental',
    unique_key=['order_id', 'order_item_id'],
    incremental_strategy='merge',
    merge_update_columns=[
        'product_id', 'seller_id', 'purchase_date_key', 'shipping_limit_date',
        'price', 'freight_value', 'total_item_value', 'updated_ts', 'src_batch_id'
    ]
) }}

-- depends_on: {{ ref('stg_olist_order_items') }}
-- depends_on: {{ ref('stg_olist_orders') }}
-- depends_on: {{ source('transformation_control', 'transformation_batches') }}
-- depends_on: {{ source('transformation_control', 'model_batch_control') }}

{% if is_incremental() %}
with active_batch as (
    select batch_id
    from {{ source('transformation_control', 'transformation_batches') }}
    where pipeline_name = 'olist_transform_quality'
      and status = 'running'
),
affected_orders as (
    select i.order_id
    from {{ ref('stg_olist_order_items') }} i
    inner join {{ source('transformation_control', 'model_batch_control') }} b
       on b.source_batch_id = i.src_batch_id
       and b.model_name = 'fct_order_items'
       and b.source_dataset_name = 'order_items'
    inner join active_batch t on b.target_batch_id = t.batch_id

    union

    select o.order_id
    from {{ ref('stg_olist_orders') }} o
    inner join {{ source('transformation_control', 'model_batch_control') }} b
       on b.source_batch_id = o.src_batch_id
       and b.model_name = 'fct_order_items'
       and b.source_dataset_name = 'orders'
    inner join active_batch t on b.target_batch_id = t.batch_id
)
{% endif %}

select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    purchase_date_key,
    shipping_limit_date,
    price,
    freight_value,
    total_item_value,
    created_ts,
    '{{ run_started_at }}'::timestamptz as updated_ts,
    src_batch_id
from {{ ref('int_order_items_enriched') }}
{% if is_incremental() %}
where not exists (select 1 from active_batch)
   or order_id in (select order_id from affected_orders)
{% endif %}
