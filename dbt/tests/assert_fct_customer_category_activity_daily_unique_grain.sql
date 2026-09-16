select activity_date, customer_unique_id, category_name
from {{ ref('fct_customer_category_activity_daily') }}
group by 1, 2, 3
having count(*) > 1
