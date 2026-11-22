{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['activity_date', 'url', 'event_type'],
    on_schema_change='fail'
  )
}}

select
  cast(window_start::date as date) as activity_date,
  cast(url as varchar) as url,
  cast(event_type as varchar) as event_type,
  cast(sum(count) as bigint) as total_events
from {{ ref('url_counts') }}
{% if is_incremental() %}
where window_start::date >= (
  select coalesce(max(activity_date), date '1970-01-01') from {{ this }}
)
{% endif %}
group by window_start::date, url, event_type
order by activity_date desc, total_events desc
