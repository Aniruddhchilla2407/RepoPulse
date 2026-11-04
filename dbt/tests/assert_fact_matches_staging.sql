-- Reconciliation: the fact table must contain exactly the distinct events in staging.
-- Returns a row (= test failure) only when the counts differ.
with fact as (
    select count(*) as n from {{ ref('fact_events') }}
),

staging as (
    select count(distinct event_id) as n from {{ ref('stg_silver_events') }}
)

select
    fact.n as fact_rows,
    staging.n as staging_rows
from fact
cross join staging
where fact.n != staging.n