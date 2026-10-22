with facts as (
    SELECT * FROM {{ ref('fact_measurement') }} 
),
active_window as (
    SELECT sensor_id, location_id, parameter_id
    ,min(date_day) as first_day,
    max(date_day) as last_day 
    from facts 
    group by sensor_id, location_id, parameter_id 
),

sensor_days as (
    select w.sensor_id, w.location_id, w.parameter_id, d.date_day 
    from active_window w
    join {{ ref('dim_date') }} d 
    on d.date_day between w.first_day and w.last_day
),
daily as (
    select 
        sensor_id,
        date_day,
        count(*) as hours_present,
        count(*) filter (where not is_low_coverage) as hours_full_coverage,
        avg(value) as daily_mean,
        min(value) as min_hourly,
        max(value) as max_hourly
    from facts 
    group by sensor_id, date_day
),

joined as (
    select 
        sd.sensor_id,
        sd.location_id,
        sd.parameter_id,
        sd.date_day,
        COALESCE(d.hours_present, 0) as hours_present,
        COALESCE(d.hours_full_coverage, 0) as hours_full_coverage,
        d.daily_mean,
        d.min_hourly,
        d.max_hourly
    from sensor_days sd 
    left join daily d using (sensor_id, date_day)
)

select 
    j.sensor_id, 
    j.location_id,
    j.parameter_id,
    j.date_day,
    j.hours_present,
    j.hours_full_coverage,
    round(100.0 * j.hours_present / 24 , 1) as capture_pct,
    CASE 
        WHEN j.hours_present = 0 then 'no_data'
        when j.hours_present >= {{ var('min_daily_capture_hours') }} then 'sufficient'
        else 'insufficient'
    end as data_status,
    round(j.daily_mean, 2) as daily_mean,
    CASE 
        when j.hours_present >= {{ var('min_daily_capture_hours') }} then round(j.daily_mean,2)
    end as daily_mean_valid,
    j.min_hourly,
    j.max_hourly,
    CASE 
        when j.hours_present >= {{ var('min_daily_capture_hours') }} then j.daily_mean > p.naaqs_24h_in_units end as exceeds_naaqs_24h,
    CASE 
        when j.hours_present >= {{ var('min_daily_capture_hours') }} then j.daily_mean > p.who_24h_in_units end as exceeds_who_24h
from joined j 
join {{ ref('dim_parameter') }} p  
using (parameter_id)
 