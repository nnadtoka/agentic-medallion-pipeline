select as_of_date, customer_unique_id
from {{ ref('agg_customer_interests') }}
group by 1, 2
having count(*) > 1
