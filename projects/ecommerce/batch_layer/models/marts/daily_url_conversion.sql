{{ config(materialized='table') }}

select
  window_start::date as conversion_date,
  url,
  sum(view_count) as total_views,
  sum(purchase_count) as total_purchases,
  round(case when sum(view_count) > 0 then sum(purchase_count)::double / sum(view_count) else 0.0 end, 4) as avg_conversion_rate
from {{ ref('url_conversion') }}
group by window_start::date, url
order by conversion_date desc, total_purchases desc
