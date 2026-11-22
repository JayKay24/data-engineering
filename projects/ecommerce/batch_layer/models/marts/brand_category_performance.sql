{{ config(materialized='table') }}

with product_versions as (
  select
    product_id,
    category,
    brand,
    unit_price,
    dbt_valid_from,
    dbt_valid_to,
    row_number() over (partition by product_id order by dbt_valid_from asc) as version_rank
  from {{ ref('snap_products') }}
),
point_in_time_products as (
  select
    ps.category as sale_category,
    ps.revenue,
    ps.units,
    pv.brand,
    pv.category as product_category,
    pv.unit_price,
    row_number() over (
      partition by ps.window_start, ps.window_end, ps.user_id, ps.product_id, ps.category
      order by
        case
          when ps.window_start >= pv.dbt_valid_from
            and (pv.dbt_valid_to is null or ps.window_start < pv.dbt_valid_to)
          then 0
          else 1
        end,
        pv.version_rank asc
    ) as match_priority
  from {{ ref('product_sales') }} ps
  left join product_versions pv
    on ps.product_id = pv.product_id
)
select
  cast(coalesce(brand, 'Unknown') as varchar) as brand,
  cast(coalesce(product_category, sale_category, 'unknown') as varchar) as category,
  cast(round(sum(revenue), 2) as double) as total_revenue,
  cast(sum(units) as bigint) as total_units,
  cast(round(avg(unit_price), 2) as double) as avg_unit_price
from point_in_time_products
where match_priority = 1
group by coalesce(brand, 'Unknown'), coalesce(product_category, sale_category, 'unknown')
order by total_revenue desc
