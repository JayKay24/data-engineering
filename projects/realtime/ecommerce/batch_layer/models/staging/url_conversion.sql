{{ config(materialized='view') }}

select
  cast(window_start as timestamp) as window_start,
  cast(window_end   as timestamp) as window_end,
  url,
  cast(view_count     as integer) as view_count,
  cast(purchase_count as integer) as purchase_count,
  cast(conversion_rate as double) as conversion_rate
from read_parquet('{{ env_var("CLICK_STREAM_OUTPUT_PREFIX", "../output_data") }}/url_conversion/*.parquet')
