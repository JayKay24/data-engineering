{{ config(materialized='table') }}

select
  date_trunc('month', window_start)::date as sales_month,
  category,
  round(sum(revenue), 2) as monthly_revenue,
  sum(units) as monthly_units
from {{ ref('category_sales') }}
group by 1, 2
order by sales_month desc, monthly_revenue desc
