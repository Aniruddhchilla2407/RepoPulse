-- One row per repo_id. Names change when repos are renamed, so we keep the latest seen.
with latest as (
    select
        repo_id,
        arg_max(repo_name, created_at) filter (where repo_name is not null) as repo_name,
        arg_max(org_id, created_at) filter (where org_id is not null) as org_id,
        arg_max(org_login, created_at) filter (where org_login is not null) as org_login,
        min(created_at) as first_seen_at,
        max(created_at) as last_seen_at,
        count(*) as event_count
    from {{ ref('stg_silver_events') }}
    where repo_id is not null
    group by repo_id
)

select
    repo_id,
    repo_name,
    split_part(repo_name, '/', 1) as owner_login,
    org_id,
    org_login,
    first_seen_at,
    last_seen_at,
    event_count
from latest