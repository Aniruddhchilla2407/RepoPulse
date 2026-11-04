-- One row per event_id. Earliest source partition wins, matching the silver job's rule,
-- so separate loads containing the same event cannot create duplicates downstream.
select *
from {{ source('raw', 'silver_events') }}
qualify row_number() over (
    partition by event_id
    order by source_date asc, source_hour asc
) = 1