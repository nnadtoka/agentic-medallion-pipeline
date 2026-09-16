{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    merge_update_columns=[
        'customer_id', 'purchase_date_key', 'order_status', 'order_purchase_timestamp',
        'order_approved_at', 'order_delivered_carrier_date',
        'order_delivered_customer_date', 'order_estimated_delivery_date',
        'item_count', 'distinct_product_count', 'distinct_seller_count',
        'item_value', 'freight_value', 'order_item_value', 'payment_count',
        'payment_installments', 'payment_value', 'review_count',
        'average_review_score', 'updated_ts', 'src_batch_id'
    ]
) }}

-- depends_on: {{ ref('stg_olist_order_items') }}
-- depends_on: {{ ref('stg_olist_order_payments') }}
-- depends_on: {{ ref('stg_olist_order_reviews') }}
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
    select o.order_id
    from {{ ref('stg_olist_orders') }} o
    inner join {{ source('transformation_control', 'model_batch_control') }} b
        on b.source_batch_id = o.src_batch_id
       and b.model_name = 'fct_orders'
       and b.source_dataset_name = 'orders'
    inner join active_batch t on b.target_batch_id = t.batch_id

    union

    select i.order_id
    from {{ ref('stg_olist_order_items') }} i
    inner join {{ source('transformation_control', 'model_batch_control') }} b
       on b.source_batch_id = i.src_batch_id
       and b.model_name = 'fct_orders'
       and b.source_dataset_name = 'order_items'
    inner join active_batch t on b.target_batch_id = t.batch_id

    union

    select p.order_id
    from {{ ref('stg_olist_order_payments') }} p
    inner join {{ source('transformation_control', 'model_batch_control') }} b
       on b.source_batch_id = p.src_batch_id
       and b.model_name = 'fct_orders'
       and b.source_dataset_name = 'order_payments'
    inner join active_batch t on b.target_batch_id = t.batch_id

    union

    select r.order_id
    from {{ ref('stg_olist_order_reviews') }} r
    inner join {{ source('transformation_control', 'model_batch_control') }} b
       on b.source_batch_id = r.src_batch_id
       and b.model_name = 'fct_orders'
       and b.source_dataset_name = 'order_reviews'
    inner join active_batch t on b.target_batch_id = t.batch_id
)
{% endif %}

select
    o.order_id,
    o.customer_id,
    to_char(o.order_purchase_date, 'YYYYMMDD')::integer as purchase_date_key,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    coalesce(i.item_count, 0) as item_count,
    coalesce(i.distinct_product_count, 0) as distinct_product_count,
    coalesce(i.distinct_seller_count, 0) as distinct_seller_count,
    coalesce(i.item_value, 0) as item_value,
    coalesce(i.freight_value, 0) as freight_value,
    coalesce(i.order_item_value, 0) as order_item_value,
    coalesce(p.payment_count, 0) as payment_count,
    coalesce(p.payment_installments, 0) as payment_installments,
    coalesce(p.payment_value, 0) as payment_value,
    coalesce(r.review_count, 0) as review_count,
    r.average_review_score,
    '{{ run_started_at }}'::timestamptz as created_ts,
    '{{ run_started_at }}'::timestamptz as updated_ts,
    o.src_batch_id
from {{ ref('stg_olist_orders') }} o
left join {{ ref('int_order_item_totals') }} i using (order_id)
left join {{ ref('int_order_payment_totals') }} p using (order_id)
left join {{ ref('int_order_review_summary') }} r using (order_id)
{% if is_incremental() %}
where not exists (select 1 from active_batch)
   or o.order_id in (select order_id from affected_orders)
{% endif %}
