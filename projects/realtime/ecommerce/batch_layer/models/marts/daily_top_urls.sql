{{ config(materialized='table') }}

select
  window_start::date as activity_date,
  url,
  event_type,
  sum(count) as total_events
from {{ ref('url_counts') }}
group by window_start::date, url, event_type
order by activity_date desc, total_events desc
