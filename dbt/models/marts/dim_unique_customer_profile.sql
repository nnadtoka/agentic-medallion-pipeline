{#
    DROP + plain CREATE below, not CREATE ... IF NOT EXISTS -- see
    dim_location.sql's comment for why: this is a materialized='table'
    model, so the outgoing table's rename-to-backup step leaves these index
    names claimed by the backup table, making "IF NOT EXISTS" silently
    no-op on the new live table every rebuild, right before dbt's cleanup
    DROPs the backup (and those index names with it) via CASCADE. Both the
    DROP and CREATE below are schema-qualified explicitly, not left to
    resolve via the connection's search_path -- see dim_location.sql.
#}
{{ config(
    post_hook=[
        'drop index if exists "{{ this.schema }}".idx_unique_customer_profile_id',
        "create unique index idx_unique_customer_profile_id on {{ this }} (customer_profile_id)",
        'drop index if exists "{{ this.schema }}".idx_unique_customer_profile_period',
        "create index idx_unique_customer_profile_period on {{ this }} (customer_unique_id, version_start_ts, version_end_ts)",
        'drop index if exists "{{ this.schema }}".idx_unique_customer_profile_current',
        "create unique index idx_unique_customer_profile_current on {{ this }} (customer_unique_id) where is_current"
    ]
) }}

with source_observations as (
    select
        c.customer_unique_id,
        l.location_id,
        coalesce(o.order_purchase_timestamp, c.created_ts) as observed_ts,
        c.customer_id,
        c.created_ts
    from {{ ref('stg_olist_customers') }} c
    left join {{ ref('stg_olist_orders') }} o using (customer_id)
    join {{ ref('dim_location') }} l
      on l.zip_code_prefix is not distinct from c.customer_zip_code_prefix::text
     and lower(l.city) is not distinct from lower(nullif(trim(c.customer_city), ''))
     and l.state is not distinct from upper(nullif(trim(c.customer_state), ''))
), deduplicated as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by customer_unique_id, observed_ts
                order by (customer_id is not null) desc, customer_id desc
            ) as observation_rank
        from source_observations
        where customer_unique_id is not null
    ) ranked
    where observation_rank = 1
), changes as (
    select
        *,
        case
            when location_id is distinct from lag(location_id) over (
                partition by customer_unique_id order by observed_ts, customer_id
            ) then 1 else 0
        end as starts_new_version
    from deduplicated
), islands as (
    select
        *,
        sum(starts_new_version) over (
            partition by customer_unique_id order by observed_ts, customer_id
            rows unbounded preceding
        ) as version_number
    from changes
), versions as (
    select
        customer_unique_id,
        version_number,
        location_id,
        min(observed_ts) as version_start_ts,
        max(observed_ts) as last_seen_ts,
        min(created_ts) as first_seen_ts,
        max(created_ts) as created_ts
    from islands
    group by 1, 2, 3
), bounded as (
    select
        *,
        lead(version_start_ts) over (
            partition by customer_unique_id order by version_start_ts, version_number
        ) as version_end_ts
    from versions
)
select
    md5(customer_unique_id || '|' || version_start_ts::text || '|' || location_id::text)::uuid
        as customer_profile_id,
    customer_unique_id,
    location_id,
    version_start_ts,
    version_end_ts,
    version_end_ts is null as is_current,
    first_seen_ts,
    last_seen_ts,
    created_ts,
    '{{ run_started_at }}'::timestamptz as updated_ts
from bounded
