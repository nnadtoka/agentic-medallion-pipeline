with invalid_boundaries as (
    select customer_profile_id
    from {{ ref('dim_unique_customer_profile') }}
    where version_end_ts is not null and version_end_ts <= version_start_ts
), invalid_current_flags as (
    select customer_profile_id
    from {{ ref('dim_unique_customer_profile') }}
    where is_current <> (version_end_ts is null)
), invalid_current_counts as (
    select min(customer_profile_id::text)::uuid as customer_profile_id
    from {{ ref('dim_unique_customer_profile') }}
    group by customer_unique_id
    having count(*) filter (where is_current) <> 1
), overlapping_versions as (
    select a.customer_profile_id
    from {{ ref('dim_unique_customer_profile') }} a
    join {{ ref('dim_unique_customer_profile') }} b
      on a.customer_unique_id = b.customer_unique_id
     and a.customer_profile_id < b.customer_profile_id
     and a.version_start_ts < coalesce(b.version_end_ts, 'infinity'::timestamp)
     and b.version_start_ts < coalesce(a.version_end_ts, 'infinity'::timestamp)
)
select * from invalid_boundaries
union all select * from invalid_current_flags
union all select * from invalid_current_counts
union all select * from overlapping_versions
