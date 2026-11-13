{{ config(materialized='table') }}

select
  ps.user_id,
  coalesce(dc.segment, 'Unknown') as segment,
  coalesce(dc.region, 'Unknown') as region,
  round(sum(ps.revenue), 2) as total_revenue,
  sum(ps.units) as total_units,
  count(*) as purchase_windows
from {{ ref('product_sales') }} ps
left join {{ ref('dim_customers') }} dc
  on ps.user_id = dc.user_id
group by ps.user_id, coalesce(dc.segment, 'Unknown'), coalesce(dc.region, 'Unknown')
order by total_revenue desc
