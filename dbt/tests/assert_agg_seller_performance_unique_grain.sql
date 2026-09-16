select seller_id
from {{ ref('agg_seller_performance') }}
group by 1
having count(*) > 1
