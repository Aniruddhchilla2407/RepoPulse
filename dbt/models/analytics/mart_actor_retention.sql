{% set grain = var('cohort_grain') %}
{% if grain not in ['hour', 'day', 'week'] %}
  {{ exceptions.raise_compiler_error("cohort_grain must be hour, day or week, got: " ~ grain) }}
{% endif %}

-- Retention cohorts: group actors by the period of their first observed activity, then count
-- how many are active again N periods later. Bots are excluded.
-- Caveat: we only see data from the first loaded period on, so the earliest cohort contains
-- everyone active then, including long-time users (flagged as is_left_censored).
with activity as (
    select distinct
        f.actor_id,
        date_trunc('{{ grain }}', f.created_at) as active_period
    from {{ ref('fact_events') }} as f
    join {{ ref('dim_actor') }} as a
        on f.actor_id = a.actor_id
    where f.actor_id is not null
      and not a.is_bot
),

cohorts as (
    select
        actor_id,
        min(active_period) as cohort_period
    from activity
    group by actor_id
),

cohort_sizes as (
    select
        cohort_period,
        count(*) as cohort_size
    from cohorts
    group by cohort_period
),

joined as (
    select
        c.cohort_period,
        a.actor_id,
        date_diff('{{ grain }}', c.cohort_period, a.active_period) as periods_since_first
    from activity as a
    join cohorts as c
        on a.actor_id = c.actor_id
)

select
    j.cohort_period,
    j.periods_since_first,
    s.cohort_size,
    count(distinct j.actor_id) as active_actors,
    cast(count(distinct j.actor_id) as double) / s.cohort_size as retention_rate,
    j.cohort_period = min(j.cohort_period) over () as is_left_censored
from joined as j
join cohort_sizes as s
    on j.cohort_period = s.cohort_period
group by j.cohort_period, j.periods_since_first, s.cohort_size