{{ config(
    materialized='incremental',
    unique_key=['order_id', 'payment_sequential'],
    incremental_strategy='merge',
    merge_update_columns=[
        'purchase_date_key', 'payment_type', 'payment_installments',
        'payment_value', 'updated_ts', 'src_batch_id'
    ]
) }}

-- depends_on: {{ ref('stg_olist_order_payments') }}
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
    select p.order_id
    from {{ ref('stg_olist_order_payments') }} p
    inner join {{ source('transformation_control', 'model_batch_control') }} b
       on b.source_batch_id = p.src_batch_id
       and b.model_name = 'fct_order_payments'
       and b.source_dataset_name = 'order_payments'
    inner join active_batch t on b.target_batch_id = t.batch_id

    union

    select o.order_id
    from {{ ref('stg_olist_orders') }} o
    inner join {{ source('transformation_control', 'model_batch_control') }} b
       on b.source_batch_id = o.src_batch_id
       and b.model_name = 'fct_order_payments'
       and b.source_dataset_name = 'orders'
    inner join active_batch t on b.target_batch_id = t.batch_id
)
{% endif %}

select
    order_id,
    payment_sequential,
    purchase_date_key,
    payment_type,
    payment_installments,
    payment_value,
    created_ts,
    '{{ run_started_at }}'::timestamptz as updated_ts,
    src_batch_id
from {{ ref('int_order_payments_enriched') }}
{% if is_incremental() %}
where not exists (select 1 from active_batch)
   or order_id in (select order_id from affected_orders)
{% endif %}
