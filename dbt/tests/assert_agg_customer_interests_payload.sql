select customer_unique_id
from {{ ref('agg_customer_interests') }}
where jsonb_typeof(categories) <> 'object'
   or jsonb_typeof(activity_by_day_7d) <> 'object'
   or (select count(*) from jsonb_object_keys(activity_by_day_7d)) <> 7
   or item_count_7d > item_count_30d
   or item_value_7d > item_value_30d
   or (distinct_category_count = 0 and categories <> '{}'::jsonb)
