{{ config(materialized='table') }}

-- DuckDB has no table partitions or clustering keys. The table is stored sorted by
-- (event_date, event_type, repo_id), so per-row-group min/max statistics let filters on those
-- columns skip data. This is the local stand-in for partition + cluster; Phase 7 measures it.
select
    event_id,
    event_type,
    created_at,
    event_date,
    date_trunc('hour', created_at) as event_hour,
    actor_id,
    repo_id,
    org_id,
    is_public,
    push_size,
    push_distinct_size,
    pr_action,
    pr_number,
    pr_merged,
    pr_additions,
    pr_deletions,
    pr_changed_files,
    issue_action,
    issue_number,
    watch_action,
    fork_repo_id,
    source_date,
    source_hour
from {{ ref('stg_silver_events') }}
order by event_date, event_type, repo_id