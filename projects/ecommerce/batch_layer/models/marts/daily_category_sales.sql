{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['sales_date', 'category'],
    on_schema_change='fail'
  )
}}

select
  cast(window_start::date as date) as sales_date,
  cast(category as varchar) as category,
  cast(round(sum(revenue), 2) as double) as daily_revenue,
  cast(sum(units) as bigint) as daily_units
from {{ ref('category_sales') }}
{% if is_incremental() %}
where window_start::date >= (
  select coalesce(max(sales_date), date '1970-01-01') from {{ this }}
)
{% endif %}
group by window_start::date, category
order by sales_date desc, daily_revenue desc
