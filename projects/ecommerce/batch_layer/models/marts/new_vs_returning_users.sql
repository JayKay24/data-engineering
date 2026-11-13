{{ config(materialized='table') }}

with user_first_seen as (
  select
    user_id,
    min(window_start)::date as first_seen_date
  from {{ ref('top_urls_per_user') }}
  group by user_id
),
daily_active as (
  select distinct
    window_start::date as activity_date,
    user_id
  from {{ ref('top_urls_per_user') }}
)
select
  d.activity_date,
  count(distinct case when d.activity_date = u.first_seen_date then d.user_id end) as new_users,
  count(distinct case when d.activity_date > u.first_seen_date then d.user_id end) as returning_users,
  count(distinct d.user_id) as total_active_users
from daily_active d
join user_first_seen u on d.user_id = u.user_id
group by d.activity_date
order by d.activity_date desc
