{{ config(materialized='view') }}

select
  cast(window_start as timestamp) as window_start,
  cast(window_end   as timestamp) as window_end,
  user_id,
  product_id,
  category,
  cast(revenue as double) as revenue,
  cast(units   as integer) as units
from {{ get_stream_path('product_sales') }}
