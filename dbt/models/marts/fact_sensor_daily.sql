with facts as (
    SELECT * FROM {{ ref('fact_measurement') }} 
),
overrides as (
    select * from {{ ref('quality_overrides') }}
),
flatline_hours as (
    select f.sensor_id, f.period_start_utc 
    from facts as f
    join {{ ref('qa_flatlines') }} r
    on r.sensor_id = f.sensor_id 
    and f.period_start_utc >= r.run_start_utc 
    and f.period_start_utc < r.run_end_utc
),
facts_flagged as (
    select f.*, (fh.sensor_id is not null) as is_flatline,
    exists(
        select 1 from overrides o
        where o.sensor_id = f.sensor_id
        and f.date_day between o.valid_from and o.valid_to 
    ) as is_overridden
    from facts f
    left join flatline_hours fh using (sensor_id, period_start_utc) 
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
        count(*) filter (where is_flatline) as hours_flatline,
        count(*) filter (where is_overridden) as hours_overridden,
        count(*) filter (where not is_flatline and not is_overridden) as hours_usable,
        count(*) filter (where not is_low_coverage and not is_flatline and not is_overridden) as hours_full_coverage,
        avg(value) filter (where not is_flatline and not is_overridden) as daily_mean,
        min(value) filter (where not is_flatline and not is_overridden) as min_hourly,
        max(value) filter (where not is_flatline and not is_overridden) as max_hourly
    from facts_flagged 
    group by sensor_id, date_day
),

joined as (
    select 
        sd.sensor_id,
        sd.location_id,
        sd.parameter_id,
        sd.date_day,
        COALESCE(d.hours_present, 0) as hours_present,
        COALESCE(d.hours_flatline, 0) as hours_flatline,
        COALESCE(d.hours_overridden, 0) as hours_overridden,
        COALESCE(d.hours_usable, 0) as hours_usable,
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
    j.hours_flatline,
    j.hours_overridden,
    j.hours_usable,
    j.hours_full_coverage,
    round(100.0 * j.hours_usable / 24 , 1) as capture_pct,
    CASE 
        WHEN j.hours_present = 0 then 'no_data'
        when j.hours_usable >= {{ var('min_daily_capture_hours') }} then 'sufficient'
        else 'insufficient'
    end as data_status,
    round(j.daily_mean, 2) as daily_mean,
    CASE 
        when j.hours_usable >= {{ var('min_daily_capture_hours') }} then round(j.daily_mean,2)
    end as daily_mean_valid,
    j.min_hourly,
    j.max_hourly,
    CASE 
        when j.hours_usable >= {{ var('min_daily_capture_hours') }} then j.daily_mean > p.naaqs_24h_in_units end as exceeds_naaqs_24h,
    CASE 
        when j.hours_usable >= {{ var('min_daily_capture_hours') }} then j.daily_mean > p.who_24h_in_units end as exceeds_who_24h
from joined j 
join {{ ref('dim_parameter') }} p  
using (parameter_id)
 