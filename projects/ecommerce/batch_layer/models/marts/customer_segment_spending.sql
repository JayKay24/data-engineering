{{ config(materialized='table') }}

with customer_versions as (
  select
    user_id,
    segment,
    region,
    dbt_valid_from,
    dbt_valid_to,
    row_number() over (partition by user_id order by dbt_valid_from asc) as version_rank
  from {{ ref('snap_customers') }}
),
point_in_time_sales as (
  select
    ps.user_id,
    ps.revenue,
    ps.units,
    cv.segment,
    cv.region,
    row_number() over (
      partition by ps.window_start, ps.window_end, ps.user_id, ps.product_id, ps.category
      order by
        case
          when ps.window_start >= cv.dbt_valid_from
            and (cv.dbt_valid_to is null or ps.window_start < cv.dbt_valid_to)
          then 0
          else 1
        end,
        cv.version_rank asc
    ) as match_priority
  from {{ ref('product_sales') }} ps
  left join customer_versions cv
    on ps.user_id = cv.user_id
)
select
  cast(user_id as varchar) as user_id,
  cast(coalesce(segment, 'Unknown') as varchar) as segment,
  cast(coalesce(region, 'Unknown') as varchar) as region,
  cast(round(sum(revenue), 2) as double) as total_revenue,
  cast(sum(units) as bigint) as total_units,
  cast(count(*) as bigint) as purchase_windows
from point_in_time_sales
where match_priority = 1
group by user_id, coalesce(segment, 'Unknown'), coalesce(region, 'Unknown')
order by total_revenue desc
