{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['conversion_date', 'url'],
    on_schema_change='fail'
  )
}}

select
  cast(window_start::date as date) as conversion_date,
  cast(url as varchar) as url,
  cast(sum(view_count) as bigint) as total_views,
  cast(sum(purchase_count) as bigint) as total_purchases,
  cast(
    round(
      case
        when sum(view_count) > 0 then sum(purchase_count)::double / sum(view_count)
        else 0.0
      end,
      4
    ) as double
  ) as avg_conversion_rate
from {{ ref('url_conversion') }}
{% if is_incremental() %}
where window_start::date >= (
  select coalesce(max(conversion_date), date '1970-01-01') from {{ this }}
)
{% endif %}
group by window_start::date, url
order by conversion_date desc, total_purchases desc
