with bounds as (
    SELECT
        min(date_local) as first_day,
        make_date(year(max(date_local)) + 1, 12, 31) as last_day
    FROM {{ ref('stg_readings') }}
)

,series as (
    SELECT unnest(GENERATE_SERIES(first_day,last_day, INTERVAL 1 DAY)) as d
    FROM bounds 
)

,days as (
    select cast(d as date) as date_day 
    from series 
)

SELECT 
    date_day ,
    year(date_day) as year,
    quarter(date_day) as quarter,
    month(date_day) as month,
    monthname(date_day) as month_name,
    strftime(date_day, '%Y-%m') as year_month, 
    day(date_day) as day_of_month, 
    isodow(date_day) as day_of_week,
    dayname(date_day) as day_name,
    weekofyear(date_day) as iso_week,
    cast(date_trunc('week', date_day) as date) as week_start,
    case 
        when month(date_day) in (1,2) then 'Winter'
        when month(date_day) between 3 and 5 then 'Pre-monsoon'
        when month(date_day) between 6 and 9 then 'Monsoon'
        else 'Post-monsoon'
    end as season,
    month(date_day) in (10,11) as is_crop_burning_season
from days