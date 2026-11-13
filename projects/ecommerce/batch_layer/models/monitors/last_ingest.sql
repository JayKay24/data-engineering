{{ config(materialized='view') }}

select
  max(window_end) as latest_stream_window,
  current_timestamp as batch_processed_at,
  age(current_timestamp, max(window_end)) as pipeline_latency
from {{ ref('url_counts') }}
