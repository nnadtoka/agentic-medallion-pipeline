select
    c.customer_unique_id,
    coalesce(p.product_category_name_english, p.product_category_name, 'unknown') as category_name,
    o.order_purchase_timestamp::date as activity_date,
    count(*)::bigint as item_count,
    sum(i.price) as item_value,
    sum(i.freight_value) as freight_value,
    sum(i.total_item_value) as total_value,
    min(o.order_purchase_timestamp) as first_purchase_ts,
    max(o.order_purchase_timestamp) as latest_purchase_ts,
    max(greatest(i.created_ts, o.created_ts, c.created_ts, p.created_ts)) as created_ts
from {{ ref('int_order_items_enriched') }} i
join {{ ref('stg_olist_orders') }} o using (order_id)
join {{ ref('stg_olist_customers') }} c using (customer_id)
left join {{ ref('int_products_enriched') }} p using (product_id)
where o.order_status <> 'canceled'
  and c.customer_unique_id is not null
group by 1, 2, 3
