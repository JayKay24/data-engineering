{{ config(materialized='view') }}

select
  cast(window_start as timestamp) as window_start,
  cast(window_end   as timestamp) as window_end,
  url,
  event_type,
  cast(count as integer) as count
from {{ get_stream_path('url_counts') }}
