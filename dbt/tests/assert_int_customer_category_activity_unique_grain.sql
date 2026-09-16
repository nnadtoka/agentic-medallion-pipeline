select customer_unique_id, category_name, activity_date
from {{ ref('int_customer_category_activity') }}
group by 1, 2, 3
having count(*) > 1
