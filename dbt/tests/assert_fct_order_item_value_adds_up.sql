select order_id, order_item_id
from {{ ref('fct_order_items') }}
where total_item_value <> price + freight_value
