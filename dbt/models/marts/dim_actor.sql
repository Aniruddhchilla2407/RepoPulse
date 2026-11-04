-- One row per actor_id, with the latest login and a bot flag (GitHub Apps end in [bot]).
select
    actor_id,
    arg_max(actor_login, created_at) filter (where actor_login is not null) as actor_login,
    coalesce(bool_or(ends_with(actor_login, '[bot]')), false) as is_bot,
    min(created_at) as first_seen_at,
    max(created_at) as last_seen_at,
    count(*) as event_count
from {{ ref('stg_silver_events') }}
where actor_id is not null
group by actor_id