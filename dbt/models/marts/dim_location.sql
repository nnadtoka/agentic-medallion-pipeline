{#
    DROP + plain CREATE below, not CREATE ... IF NOT EXISTS: this is a
    materialized='table' model, so every run does dbt's usual
    create-tmp/rename-old-to-backup/rename-tmp-to-final swap. Renaming a
    table doesn't rename its indexes, so the outgoing backup table keeps
    physically owning indexes named idx_dim_location_id/
    idx_dim_location_natural_key -- and since Postgres index names are
    unique per schema, not per table, "IF NOT EXISTS" then silently no-ops
    against the *new* live table (the name's already taken by the backup's
    copy). dbt's own subsequent `DROP TABLE ... _dbt_backup CASCADE`
    cleanup then deletes those indexes along with the backup table, leaving
    the live table with none at all. Confirmed live via `log_statement=
    'ddl'`: the whole sequence runs error-free on one connection, the index
    creation is genuinely a silent no-op, not a race. DROP INDEX IF EXISTS
    first guarantees the name is free regardless of which table currently
    holds it, so the CREATE right after it always lands on the correct,
    current table. Both the DROP and CREATE below are schema-qualified
    explicitly ("{{ this.schema }}".idx_...) rather than relying on the
    connection's search_path to resolve an unqualified name -- an
    unqualified `DROP INDEX IF EXISTS` depends on search_path containing
    this schema, which isn't guaranteed, and got this exact fix wrong the
    first time (it silently found nothing, so the CREATE right after it
    still collided).
#}
{{ config(
    post_hook=[
        'drop index if exists "{{ this.schema }}".idx_dim_location_id',
        "create unique index idx_dim_location_id on {{ this }} (location_id)",
        'drop index if exists "{{ this.schema }}".idx_dim_location_natural_key',
        "create unique index idx_dim_location_natural_key on {{ this }} (zip_code_prefix, lower(city), state) nulls not distinct"
    ]
) }}

with locations as (
    select
        customer_zip_code_prefix::text as zip_code_prefix,
        nullif(trim(customer_city), '') as city,
        upper(nullif(trim(customer_state), '')) as state,
        created_ts
    from {{ ref('stg_olist_customers') }}

    union all

    select
        seller_zip_code_prefix::text,
        nullif(trim(seller_city), ''),
        upper(nullif(trim(seller_state), '')),
        created_ts
    from {{ ref('stg_olist_sellers') }}
), grouped as (
    -- Group by lower(city), not raw city: location_id below is hashed on
    -- lower(city) and the natural-key index is defined on (zip_code_prefix,
    -- lower(city), state), so two source rows with the same zip/state but
    -- different city casing (e.g. "Sao Paulo" vs "SAO PAULO") must collapse
    -- into one row here too -- otherwise they'd survive as two distinct
    -- `grouped` rows that both hash to the same location_id, violating the
    -- unique index at build time. No such casing collision exists in the
    -- data as of this writing (checked directly), but nothing about the
    -- source data guarantees that stays true.
    select
        zip_code_prefix,
        lower(city) as city_key,
        state,
        min(city) as city,
        min(created_ts) as first_seen_ts,
        max(created_ts) as last_seen_ts
    from locations
    group by 1, 2, 3
)
select
    md5(
        coalesce(zip_code_prefix, '<null>') || '|' ||
        coalesce(city_key, '<null>') || '|' ||
        coalesce(state, '<null>')
    )::uuid as location_id,
    zip_code_prefix,
    city,
    state,
    first_seen_ts,
    last_seen_ts,
    '{{ run_started_at }}'::timestamptz as updated_ts
from grouped
