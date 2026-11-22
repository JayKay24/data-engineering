{% snapshot snap_products %}
{{
  config(
    target_schema='snapshots',
    unique_key='product_id',
    strategy='check',
    check_cols=['category', 'brand', 'unit_price'],
    invalidate_hard_deletes=True
  )
}}

select
  product_id,
  category,
  brand,
  unit_price
from {{ ref('dim_products') }}
{% endsnapshot %}
