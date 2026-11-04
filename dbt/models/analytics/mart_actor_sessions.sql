{% set gap_seconds = var('session_gap_minutes') * 60 %}

-- Sessionization: a new session starts after more than N minutes of inactivity for an actor.
-- Ties on created_at are broken by event_id so both window passes order rows identically.
with events as (
    select
        f.actor_id,
        f.event_id,
        f.created_at,
        f.repo_id,
        f.event_type
    from {{ ref('fact_events') }} as f
    join {{ ref('dim_actor') }} as a
        on f.actor_id = a.actor_id
    where f.actor_id is not null
    {% if var('exclude_bots_from_sessions') %}
      and not a.is_bot
    {% endif %}
),

flagged as (
    select
        *,
        case
            when lag(created_at) over (partition by actor_id order by created_at, event_id) is null
                then 1
            when date_diff(
                    'second',
                    lag(created_at) over (partition by actor_id order by created_at, event_id),
                    created_at
                 ) > {{ gap_seconds }}
                then 1
            else 0
        end as is_new_session
    from events
),

numbered as (
    select
        *,
        sum(is_new_session) over (
            partition by actor_id order by created_at, event_id
            rows between unbounded preceding and current row
        ) as session_number
    from flagged
)

select
    concat(cast(actor_id as varchar), '-', cast(session_number as varchar)) as session_id,
    actor_id,
    session_number,
    min(created_at) as session_start,
    max(created_at) as session_end,
    date_diff('second', min(created_at), max(created_at)) as duration_seconds,
    count(*) as events,
    count(distinct repo_id) as distinct_repos,
    count(*) filter (where event_type = 'PushEvent') as pushes,
    count(*) filter (where event_type = 'PullRequestEvent') as pull_request_events,
    count(*) filter (where event_type = 'IssuesEvent') as issue_events,
    count(*) filter (where event_type = 'WatchEvent') as stars,
    count(*) filter (where event_type = 'ForkEvent') as forks
from numbered
group by actor_id, session_number