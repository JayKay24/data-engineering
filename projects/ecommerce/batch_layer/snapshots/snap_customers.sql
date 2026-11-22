{% snapshot snap_customers %}
{{
  config(
    target_schema='snapshots',
    unique_key='user_id',
    strategy='check',
    check_cols=['segment', 'region'],
    invalidate_hard_deletes=True
  )
}}

select
  user_id,
  segment,
  region,
  registration_date
from {{ ref('dim_customers') }}
{% endsnapshot %}
