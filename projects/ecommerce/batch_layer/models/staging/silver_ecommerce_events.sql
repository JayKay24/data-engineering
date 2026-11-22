{{ config(materialized='view') }}

select
  user_id,
  url,
  event_type,
  cast(event_time as timestamp) as event_time,
  cast(event_date as date) as event_date,
  product_id,
  category,
  cast(price as double) as price,
  ip_address,
  user_agent,
  cast(ingested_at as timestamp) as ingested_at
from {{ get_delta_path('silver_ecommerce_events') }}
