{{ config(materialized='table') }}

select
  quarantine_reason,
  cast(count(*) as bigint) as quarantined_events,
  max(ingested_at) as latest_quarantined_at
from {{ ref('quarantine_events') }}
group by quarantine_reason
order by quarantined_events desc
