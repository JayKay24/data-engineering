{{ config(materialized='view') }}

select
  cast(session_start as timestamp) as session_start,
  cast(session_end   as timestamp) as session_end,
  user_id,
  cast(has_view     as boolean) as has_view,
  cast(has_add      as boolean) as has_add,
  cast(has_purchase as boolean) as has_purchase
from read_parquet('{{ env_var("CLICK_STREAM_OUTPUT_PREFIX", "../output_data") }}/session_funnels/*.parquet')
