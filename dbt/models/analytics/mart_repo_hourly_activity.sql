-- Window functions over per-repo hourly activity. "Previous" and "3 active hours" count only
-- hours in which the repo had events (the grain is sparse), not calendar hours.
with hourly as (
    select
        repo_id,
        event_hour,
        count(*) as events,
        count(distinct actor_id) as unique_actors,
        count(*) filter (where event_type = 'PushEvent') as pushes,
        count(*) filter (where event_type = 'PullRequestEvent') as pull_request_events,
        count(*) filter (where event_type = 'IssuesEvent') as issue_events,
        count(*) filter (where event_type = 'WatchEvent') as stars,
        count(*) filter (where event_type = 'ForkEvent') as forks
    from {{ ref('fact_events') }}
    where repo_id is not null
    group by repo_id, event_hour
)

select
    repo_id,
    event_hour,
    events,
    unique_actors,
    pushes,
    pull_request_events,
    issue_events,
    stars,
    forks,
    sum(stars) over (
        partition by repo_id order by event_hour
        rows between unbounded preceding and current row
    ) as cumulative_stars,
    avg(events) over (
        partition by repo_id order by event_hour
        rows between 2 preceding and current row
    ) as events_moving_avg_3_active_hours,
    events - lag(events) over (partition by repo_id order by event_hour)
        as events_change_vs_prev_active_hour,
    rank() over (partition by event_hour order by events desc) as events_rank_in_hour
from hourly
order by repo_id, event_hour