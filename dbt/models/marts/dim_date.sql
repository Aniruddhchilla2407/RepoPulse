-- A calendar covering every event_date in the data (no gaps), so joins never drop days.
with bounds as (
    select
        min(event_date) as min_date,
        max(event_date) as max_date
    from {{ ref('stg_silver_events') }}
),

series as (
    select
        unnest(generate_series(
            cast(min_date as timestamp),
            cast(max_date as timestamp),
            interval 1 day
        )) as day_ts
    from bounds
),

days as (
    select cast(day_ts as date) as date_day
    from series
)

select
    date_day,
    cast(strftime(date_day, '%Y%m%d') as integer) as date_key,
    extract(year from date_day) as calendar_year,
    extract(quarter from date_day) as calendar_quarter,
    extract(month from date_day) as calendar_month,
    monthname(date_day) as month_name,
    extract(day from date_day) as day_of_month,
    isodow(date_day) as day_of_week,          -- 1 = Monday
    dayname(date_day) as day_name,
    isodow(date_day) in (6, 7) as is_weekend,
    cast(date_trunc('week', date_day) as date) as week_start
from days