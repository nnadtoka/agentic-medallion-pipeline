-- Every row represents at least one order-specific customer and one order,
-- and the rolled-up activity window must run forward in time. These are
-- semantic constraints that not_null/unique/non_negative tests cannot express.
select customer_unique_id
from {{ ref('int_unique_customer_order_history') }}
where customer_id_count <= 0
   or order_count <= 0
   or customer_id_count > order_count
   or first_order_ts > latest_order_ts
