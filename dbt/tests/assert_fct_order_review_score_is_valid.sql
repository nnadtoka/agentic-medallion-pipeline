select order_id
from {{ ref('fct_orders') }}
where average_review_score is not null
  and average_review_score not between 1 and 5
