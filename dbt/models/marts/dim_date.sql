select
    date_key,
    date_day,
    year_number,
    quarter_number,
    month_number,
    month_name,
    iso_day_of_week,
    day_name,
    iso_week_number,
    is_weekend,
    created_ts
from {{ ref('int_date_spine') }}
