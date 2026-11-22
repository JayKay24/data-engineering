{{ config(materialized='view') }}

select
  product_id,
  category,
  brand,
  cast(unit_price as double) as unit_price
from {{ get_parquet_path('dim_products') }}
