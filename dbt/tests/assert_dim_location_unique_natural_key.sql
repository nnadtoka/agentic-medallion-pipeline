select zip_code_prefix, city, state
from {{ ref('dim_location') }}
group by 1, 2, 3
having count(*) > 1
