{{ config(materialized='view') }}

select
  cast(window_start as timestamp) as window_start,
  cast(window_end   as timestamp) as window_end,
  url,
  event_type,
  cast(count as integer) as count
from read_parquet('{{ env_var("CLICK_STREAM_OUTPUT_PREFIX", "../output_data") }}/url_counts/*.parquet')
