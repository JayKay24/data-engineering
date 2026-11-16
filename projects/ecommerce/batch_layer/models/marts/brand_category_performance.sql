{{ config(materialized='table') }}

select
  coalesce(dp.brand, 'Unknown') as brand,
  coalesce(dp.category, ps.category, 'unknown') as category,
  round(sum(ps.revenue), 2) as total_revenue,
  sum(ps.units) as total_units,
  round(avg(dp.unit_price), 2) as avg_unit_price
from {{ ref('product_sales') }} ps
left join {{ ref('dim_products') }} dp
  on ps.product_id = dp.product_id
group by coalesce(dp.brand, 'Unknown'), coalesce(dp.category, ps.category, 'unknown')
order by total_revenue desc
