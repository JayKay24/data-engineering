{{ config(materialized='view') }}

select
  cast(window_start as timestamp) as window_start,
  cast(window_end   as timestamp) as window_end,
  url,
  cast(view_count as integer) as view_count,
  cast(add_to_cart_count as integer) as add_to_cart_count,
  cast(purchase_count as integer) as purchase_count,
  cast(add_to_cart_rate as double) as add_to_cart_rate,
  cast(cart_abandonment as double) as cart_abandonment
from {{ get_stream_path('cart_metrics') }}
