{{ config(materialized='table') }}

with distinct_users as (
  select distinct
    user_id,
    min(window_start) as first_seen
  from {{ ref('top_urls_per_user') }}
  group by user_id
)
select
  cast(first_seen::date as date) as cohort_date,
  cast(count(distinct user_id) as bigint) as new_users,
  cast(sum(count(distinct user_id)) over (order by first_seen::date) as bigint) as cumulative_users
from distinct_users
group by first_seen::date
order by cohort_date
