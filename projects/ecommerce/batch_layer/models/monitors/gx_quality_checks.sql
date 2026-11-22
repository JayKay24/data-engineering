{{ config(materialized='table') }}

select
  engine,
  cast(generated_at as timestamp) as generated_at,
  cast(overall_success as boolean) as overall_success,
  suite_name,
  cast(suite_success as boolean) as suite_success,
  expectation_type,
  column_name,
  cast(check_success as boolean) as check_success,
  cast(unexpected_count as integer) as unexpected_count
from {{ get_parquet_path('quality_reports') }}
