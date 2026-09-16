select order_id, payment_sequential
from {{ ref('int_order_payments_enriched') }}
group by order_id, payment_sequential
having count(*) > 1
