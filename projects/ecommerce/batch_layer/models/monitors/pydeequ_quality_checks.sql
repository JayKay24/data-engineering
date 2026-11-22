{{ config(materialized='table') }}

select
  engine,
  cast(generated_at as timestamp) as generated_at,
  cast(row_count as bigint) as row_count,
  overall_status,
  check_name,
  check_level,
  "constraint" as constraint_name,
  constraint_status,
  constraint_message
from {{ get_parquet_path('pydeequ_metrics') }}
