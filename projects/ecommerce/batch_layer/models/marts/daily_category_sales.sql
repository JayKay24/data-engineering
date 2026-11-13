{{ config(materialized='table') }}

select
  window_start::date as sales_date,
  category,
  round(sum(revenue), 2) as daily_revenue,
  sum(units) as daily_units
from {{ ref('category_sales') }}
group by window_start::date, category
order by sales_date desc, daily_revenue desc
