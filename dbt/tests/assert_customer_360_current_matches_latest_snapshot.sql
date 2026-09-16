with expected as (
    select count(*)::bigint as row_count
    from {{ ref('agg_customer_lifetime_value') }}
    where as_of_date = (select max(as_of_date) from {{ ref('agg_customer_lifetime_value') }})
), actual as (
    select count(*)::bigint as row_count
    from {{ ref('customer_360_current') }}
)
select expected.row_count as expected_rows, actual.row_count as actual_rows
from expected cross join actual
where expected.row_count <> actual.row_count
