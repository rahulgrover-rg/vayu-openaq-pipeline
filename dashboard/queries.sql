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

