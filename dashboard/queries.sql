---------------------
--    TAB 1 : AIR QUALITY
---------------------

-- 1.
with latest as (
    select max(date_day) as d
    from mart_station_day 
    where data_status = 'sufficient'
)
select 
    any_value(latest.d) as latest_complete_date,
    count(*) filter (where exceeds_naaqs_24h) as stations_over_limit,
    count(*) as stations_with_valid_data
from mart_station_day, latest
where parameter_code = 'pm25' 
and date_day = latest.d
and data_status = 'sufficient'

-- 2.

select date_day, district, round(median(daily_mean_valid), 1) as median_pm25
from mart_station_day
where parameter_code = 'pm25'
group by all
order by date_day


-- 3.
select year_month,
district, 
round(100.0 * count(*) filter (where exceeds_naaqs_24h) / nullif(count(*) filter (where daily_mean_valid is not null), 0), 1) as pct_days_over_limit
from mart_station_day
where parameter_code = 'pm25'
group by all 
order by year_month, district


--4. 
with recent as (
    select * 
    from mart_station_day
    where date_day > (select max(date_day) from mart_station_day) - interval 30 DAY
)
select 
    station_name,
    district,
    agency,
    round(avg(daily_mean_valid) filter (where parameter_code = 'pm25'), 1) as pm25,
    round(avg(daily_mean_valid) filter (where parameter_code = 'pm10'), 1) as pm10,
    round(avg(daily_mean_valid) filter (where parameter_code = 'pm25') / nullif(avg(daily_mean_valid) filter (where parameter_code = 'pm10'), 0), 2) as pm25_to_pm10,
    count(*) filter (where parameter_code = 'pm25' and exceeds_naaqs_24h) as days_over_limit,
    bool_or(is_currently_reporting) as reporting_now
from recent 
group by all 
order by pm25 desc nulls last;

-- 5.

select 
    station_name,
    latitude,
    longitude,
    round(avg(daily_mean_valid), 1) as avg_pm25
from mart_station_day
where parameter_code = 'pm25'
    and date_day > (select max(date_day) from mart_station_day) - interval 30 DAY
group by all ;

-- 6. 
select 
    f.hour_local,
    d.season,
    round(avg(f.value), 1) as avg_pm25,
from fact_measurement f
join dim_date d using (date_day)
join dim_parameter p using (parameter_id)
where p.parameter_code = 'pm25'
and not f.is_low_coverage
group by all
order by f.hour_local ; 


--------------------------
-- TAB 2: DATA QUALITY
--------------------------

-- 7a.

select 
    round(date_diff('minute', max(period_start_utc) + interval 1 hour , now() at time zone 'UTC') / 60.0, 1) as hours_old
from fact_measurement;

-- 7b.

select 
    max(period_start_utc) + interval 1 hour as newest_reading_end_utc
from fact_measurement;


-- 8.

select
    year_month,
    data_status,
    count(*) as sensor_days
from mart_station_day
group by all
order by year_month, data_status;

-- 9.
select date_day, day_type, stations_affected, stations_active, pct_affected
from qa_network_days
where day_type <> 'normal'
order by date_day desc;

-- 10. 
select 
    l.station_name,
    i.date_day,
    i.rejected_readings,
    i.sensors_affected,
    i.null_value,
    i.sentinel_value,
    i.sentinel_in_average,
    i.negative_value
from qa_station_incidents i
join dim_location l using (location_id)
order by i.rejected_readings desc, i.date_day desc 
limit 25 ;


-- 11.

select 
    l.station_name,
    p.display_name as pollutant,
    f.run_start_utc,
    f.hours,
    f.value,
    case when f.is_zero then 'zero output' else 'frozen value' end as kind 
from qa_flatlines f
join dim_location l using (location_id)
join dim_parameter p using (parameter_id)
order by f.hours DESC
limit 25 ;


-- 12.
select 
    l.station_name,
    strftime(c.date_day, '%Y-%m') as month,
    count(*) AS paired_hours,
    round(100.0*count(*) filter (where c.pm25_exceeds_pm10)/count(*),1) as pct_pm25_above_pm10,
    round(median(c.pm25_to_pm10),2) AS median_ratio
from qa_pm_cpnsistency c
join dim_location l using (location_id)
group by ALL
having count(*) >= 48
 and 100.0*count(*) filter (where c.pm25_exceeds_pm10) / count(*) > 5
order by pct_pm25_above_pm10 desc ;

-- 13.
select
    station_name,
    district,
    agency,
    last_reading_utc,
    days_quiet_at_last_check,
    is_currently_reporting,
    is_in_current_scope
from dim_location
order by is_currently_reporting, days_quiet_at_last_check desc ;

-- 14.

select
    o.sensor_id,
    l.station_name,
    p.display_name as pollutant,
    o.valid_from,
    o.valid_to,
    o.reason

from quality_overrides o
join dim_sensor s using (sensor_id)
join dim_location l using (location_id)
join dim_parameter p using (parameter_id)
order by o.valid_from 
