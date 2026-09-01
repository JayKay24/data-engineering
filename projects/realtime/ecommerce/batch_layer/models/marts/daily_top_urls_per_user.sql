{{ config(materialized='table') }}

with ranked as (
  select
    window_start::date as activity_date,
    user_id,
    url,
    sum(count) as total_visits,
    row_number() over (partition by window_start::date, user_id order by sum(count) desc) as rank_num
  from {{ ref('top_urls_per_user') }}
  group by window_start::date, user_id, url
)
select
  activity_date,
  user_id,
  url,
  total_visits,
  rank_num
from ranked
where rank_num <= 5
order by activity_date desc, user_id, rank_num
