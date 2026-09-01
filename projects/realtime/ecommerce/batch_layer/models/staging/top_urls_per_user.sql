{{ config(materialized='view') }}

select
  cast(window_start as timestamp) as window_start,
  cast(window_end   as timestamp) as window_end,
  user_id,
  url,
  cast(count as integer) as count
from read_parquet('{{ env_var("CLICK_STREAM_OUTPUT_PREFIX", "../output_data") }}/top_urls_per_user/*.parquet')
