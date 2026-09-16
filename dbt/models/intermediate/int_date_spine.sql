with bounds as (
    select
        min(order_purchase_date) as minimum_date,
        max(greatest(
            order_purchase_date,
            order_delivered_customer_date::date,
            order_estimated_delivery_date::date
        )) as maximum_date
    from {{ ref('stg_olist_orders') }}
),
date_spine as (
    select generate_series(minimum_date, maximum_date, interval '1 day')::date as date_day
    from bounds
    where minimum_date is not null and maximum_date is not null
)
select
    to_char(date_day, 'YYYYMMDD')::integer as date_key,
    date_day,
    extract(year from date_day)::smallint as year_number,
    extract(quarter from date_day)::smallint as quarter_number,
    extract(month from date_day)::smallint as month_number,
    trim(to_char(date_day, 'Month')) as month_name,
    extract(isodow from date_day)::smallint as iso_day_of_week,
    trim(to_char(date_day, 'Day')) as day_name,
    extract(week from date_day)::smallint as iso_week_number,
    (extract(isodow from date_day) in (6, 7)) as is_weekend,
    '{{ run_started_at }}'::timestamptz as created_ts
from date_spine
