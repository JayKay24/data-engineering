{{ config(materialized='view') }}

select
  cast(window_start as timestamp) as window_start,
  cast(window_end   as timestamp) as window_end,
  user_id,
  url,
  cast(count as integer) as count
from {{ get_stream_path('top_urls_per_user') }}
