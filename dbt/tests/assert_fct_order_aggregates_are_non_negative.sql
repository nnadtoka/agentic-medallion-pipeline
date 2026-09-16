select order_id
from {{ ref('fct_orders') }}
where item_count < 0
   or distinct_product_count < 0
   or distinct_seller_count < 0
   or item_value < 0
   or freight_value < 0
   or order_item_value < 0
   or payment_count < 0
   or payment_installments < 0
   or payment_value < 0
   or review_count < 0
