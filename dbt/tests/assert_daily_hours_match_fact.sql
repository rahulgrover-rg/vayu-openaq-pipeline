with daily as (
    select sum(hours_present) as n from {{ ref('fact_sensor_daily') }} 
),
hourly as (
    select count(*) as n from {{ ref('fact_measurement') }}
)

select daily.n as daily_hours, hourly.n as fact_rows
from daily, hourly 
where daily.n <> hourly.n