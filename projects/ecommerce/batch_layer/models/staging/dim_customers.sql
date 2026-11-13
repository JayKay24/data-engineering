{{ config(materialized='view') }}

select
  user_id,
  segment,
  region,
  cast(registration_date as date) as registration_date
from {{ get_stream_path('dim_customers') }}
