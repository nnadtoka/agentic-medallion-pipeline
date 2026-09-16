-- Rolling order-specific customer IDs up to customer_unique_id must not lose
-- or duplicate any order associated with a known stable customer identity.
with source_total as (
    select coalesce(sum(order_count), 0)::bigint as order_count
    from {{ ref('int_customer_order_history') }}
    where customer_unique_id is not null
),
rolled_total as (
    select coalesce(sum(order_count), 0)::bigint as order_count
    from {{ ref('int_unique_customer_order_history') }}
)
select source_total.order_count as source_order_count,
       rolled_total.order_count as rolled_order_count
from source_total
cross join rolled_total
where source_total.order_count <> rolled_total.order_count
