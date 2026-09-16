select order_id
from {{ ref('fct_orders') }}
where order_item_value <> item_value + freight_value
