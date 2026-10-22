{{ config(severity = 'warn') }}

with newest as (
    select max(period_start_utc) as newest_reading_utc 
    from {{ ref('fact_measurement') }}
)

select 
    newest_reading_utc,
    date_diff('hour', newest_reading_utc, now() at time zone 'UTC') as hours_since_newest_reading
from newest
where date_diff('hour', newest_reading_utc, now() at time zone 'UTC') > {{ var('feed_lag_alert_hours') }}