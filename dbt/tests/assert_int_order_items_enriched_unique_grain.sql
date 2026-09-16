select order_id, order_item_id
from {{ ref('int_order_items_enriched') }}
group by order_id, order_item_id
having count(*) > 1
