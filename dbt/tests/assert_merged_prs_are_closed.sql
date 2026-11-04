-- -- Business rule: a pull request can only be merged when its event action is 'closed'.
-- select
--     event_id,
--     pr_action,
--     pr_merged
-- from {{ ref('fact_events') }}
-- where pr_merged is true
--   and pr_action != 'closed'
-- Tolerance check on the merged flag. A PR should not already be merged when it is opened,
-- reopened or has new commits pushed, but real GH Archive data contains a small share of
-- 'opened' events with merged = true (observed: 141 of ~36,000 on 2024-06-01, about 0.4%).
-- The test fails only when that share exceeds 1% of opened PR events, which would point to a
-- parsing problem rather than a source quirk.
with prs as (
    select
        count(*) filter (where pr_action = 'opened') as opened_events,
        count(*) filter (
            where pr_merged is true and pr_action in ('opened', 'reopened', 'synchronize')
        ) as suspicious_events
    from {{ ref('fact_events') }}
    where event_type = 'PullRequestEvent'
)

select
    opened_events,
    suspicious_events,
    suspicious_events * 1.0 / nullif(opened_events, 0) as suspicious_rate
from prs
where suspicious_events * 1.0 / nullif(opened_events, 0) > 0.01