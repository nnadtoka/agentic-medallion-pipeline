{#
    DROP + plain CREATE below, not CREATE ... IF NOT EXISTS -- see
    dim_location.sql's comment for why: on this materialized='table'
    model, "IF NOT EXISTS" silently no-ops against the new live table every
    rebuild (the name is already claimed by the outgoing table's
    renamed-to-backup copy), and dbt's cleanup then deletes that copy via
    CASCADE, leaving no index at all. All DROPs below are schema-qualified
    explicitly, not left to resolve via the connection's search_path --
    see dim_location.sql.
#}
{{ config(
    post_hook=[
        'drop index if exists "{{ this.schema }}".idx_customer_interests_snapshot_grain',
        "create unique index idx_customer_interests_snapshot_grain on {{ this }} (as_of_date, customer_unique_id)",
        'drop index if exists "{{ this.schema }}".idx_customer_interests_rank_7d',
        "create index idx_customer_interests_rank_7d on {{ this }} (as_of_date, item_value_7d desc)",
        'drop index if exists "{{ this.schema }}".idx_customer_interests_rank_30d',
        "create index idx_customer_interests_rank_30d on {{ this }} (as_of_date, item_value_30d desc)"
    ]
) }}

-- depends_on: {{ ref('fct_orders') }}
{% set as_of_date_override = var('as_of_date', none) %}

with parameters as (
    -- Must derive its default the same way agg_customer_lifetime_value.sql
    -- does (max order date in fct_orders, not real wall-clock time) --
    -- customer_360_current joins the two models together on as_of_date, so
    -- if this model's default ever drifted from that one's (e.g. by
    -- deriving it from int_customer_category_activity's own max
    -- activity_date instead, which excludes canceled orders and so isn't
    -- guaranteed to agree), the join would silently stop matching and every
    -- interest column would look empty for reasons unrelated to the actual
    -- bug this default exists to fix. See agg_customer_lifetime_value.sql's
    -- matching comment.
    {% if as_of_date_override %}
    select '{{ as_of_date_override }}'::date as as_of_date
    {% else %}
    select coalesce(max(order_purchase_timestamp)::date, current_date) as as_of_date
    from {{ ref('fct_orders') }}
    {% endif %}
), calendar_7d as (
    select generate_series(p.as_of_date - 6, p.as_of_date, interval '1 day')::date as activity_date
    from parameters p
), customers as (
    select distinct customer_unique_id
    from {{ ref('stg_olist_customers') }}
    where customer_unique_id is not null
), historical_categories as (
    select distinct a.customer_unique_id, a.category_name
    from {{ ref('int_customer_category_activity') }} a
    cross join parameters p
    where a.activity_date <= p.as_of_date
), category_summaries as (
    select
        h.customer_unique_id,
        h.category_name,
        coalesce(sum(a.item_count) filter (where a.activity_date between p.as_of_date - 6 and p.as_of_date), 0)::bigint as item_count_7d,
        coalesce(sum(a.item_value) filter (where a.activity_date between p.as_of_date - 6 and p.as_of_date), 0) as item_value_7d,
        coalesce(sum(a.freight_value) filter (where a.activity_date between p.as_of_date - 6 and p.as_of_date), 0) as freight_value_7d,
        coalesce(sum(a.total_value) filter (where a.activity_date between p.as_of_date - 6 and p.as_of_date), 0) as total_value_7d,
        coalesce(sum(a.item_count) filter (where a.activity_date between p.as_of_date - 29 and p.as_of_date), 0)::bigint as item_count_30d,
        coalesce(sum(a.item_value) filter (where a.activity_date between p.as_of_date - 29 and p.as_of_date), 0) as item_value_30d,
        coalesce(sum(a.freight_value) filter (where a.activity_date between p.as_of_date - 29 and p.as_of_date), 0) as freight_value_30d,
        coalesce(sum(a.total_value) filter (where a.activity_date between p.as_of_date - 29 and p.as_of_date), 0) as total_value_30d
    from historical_categories h
    cross join parameters p
    left join {{ ref('int_customer_category_activity') }} a
      on a.customer_unique_id = h.customer_unique_id
     and a.category_name = h.category_name
     and a.activity_date <= p.as_of_date
    group by 1, 2
), category_daily_7d as (
    select
        h.customer_unique_id,
        h.category_name,
        d.activity_date,
        coalesce(sum(a.item_count), 0)::bigint as item_count,
        coalesce(sum(a.item_value), 0) as item_value,
        coalesce(sum(a.freight_value), 0) as freight_value,
        coalesce(sum(a.total_value), 0) as total_value
    from historical_categories h
    cross join calendar_7d d
    left join {{ ref('int_customer_category_activity') }} a
      on a.customer_unique_id = h.customer_unique_id
     and a.category_name = h.category_name
     and a.activity_date = d.activity_date
    group by 1, 2, 3
), category_payloads as (
    select
        s.*,
        jsonb_object_agg(
            d.activity_date::text,
            jsonb_build_object(
                'item_count', d.item_count,
                'item_value', d.item_value,
                'freight_value', d.freight_value,
                'total_value', d.total_value
            ) order by d.activity_date
        ) as daily_7d
    from category_summaries s
    join category_daily_7d d using (customer_unique_id, category_name)
    group by
        s.customer_unique_id, s.category_name,
        s.item_count_7d, s.item_value_7d, s.freight_value_7d, s.total_value_7d,
        s.item_count_30d, s.item_value_30d, s.freight_value_30d, s.total_value_30d
), ranked as (
    select
        *,
        row_number() over (partition by customer_unique_id order by item_value_7d desc, category_name) as rank_7d,
        row_number() over (partition by customer_unique_id order by item_value_30d desc, category_name) as rank_30d
    from category_payloads
), customer_rollup as (
    select
        customer_unique_id,
        count(*)::bigint as distinct_category_count,
        jsonb_object_agg(
            category_name,
            jsonb_build_object(
                'last_7_days', jsonb_build_object(
                    'item_count', item_count_7d, 'item_value', item_value_7d,
                    'freight_value', freight_value_7d, 'total_value', total_value_7d
                ),
                'last_30_days', jsonb_build_object(
                    'item_count', item_count_30d, 'item_value', item_value_30d,
                    'freight_value', freight_value_30d, 'total_value', total_value_30d
                ),
                'daily_7d', daily_7d
            ) order by category_name
        ) as categories,
        max(category_name) filter (where rank_7d = 1 and item_count_7d > 0) as top_category_7d,
        max(category_name) filter (where rank_30d = 1 and item_count_30d > 0) as top_category_30d,
        sum(item_count_7d)::bigint as item_count_7d,
        sum(item_value_7d) as item_value_7d,
        sum(item_count_30d)::bigint as item_count_30d,
        sum(item_value_30d) as item_value_30d
    from ranked
    group by 1
), customer_daily_7d as (
    select
        c.customer_unique_id,
        jsonb_object_agg(
            d.activity_date::text,
            jsonb_build_object(
                'item_count', coalesce(x.item_count, 0),
                'item_value', coalesce(x.item_value, 0),
                'freight_value', coalesce(x.freight_value, 0),
                'total_value', coalesce(x.total_value, 0)
            ) order by d.activity_date
        ) as activity_by_day_7d
    from customers c
    cross join calendar_7d d
    left join (
        select customer_unique_id, activity_date,
               sum(item_count)::bigint as item_count,
               sum(item_value) as item_value,
               sum(freight_value) as freight_value,
               sum(total_value) as total_value
        from {{ ref('int_customer_category_activity') }}
        group by 1, 2
    ) x on x.customer_unique_id = c.customer_unique_id and x.activity_date = d.activity_date
    group by 1
)
select
    p.as_of_date,
    c.customer_unique_id,
    coalesce(r.distinct_category_count, 0) as distinct_category_count,
    coalesce(r.categories, '{}'::jsonb) as categories,
    d.activity_by_day_7d,
    r.top_category_7d,
    r.top_category_30d,
    coalesce(r.item_count_7d, 0) as item_count_7d,
    coalesce(r.item_value_7d, 0) as item_value_7d,
    coalesce(r.item_count_30d, 0) as item_count_30d,
    coalesce(r.item_value_30d, 0) as item_value_30d,
    '{{ run_started_at }}'::timestamptz as snapshot_created_ts
from customers c
cross join parameters p
join customer_daily_7d d using (customer_unique_id)
left join customer_rollup r using (customer_unique_id)
