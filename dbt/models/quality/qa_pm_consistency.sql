with pm as (
    select f.location_id, f.period_start_utc, f.date_day, p.parameter_code, f.value 
    from {{ ref('fact_measurement') }} as f  
    join {{ ref('dim_parameter') }} as p using(parameter_id)
    where p.parameter_code in ('pm25', 'pm10')
),
pairs as (
    select 
        location_id, 
        period_start_utc,
        date_day,
        max(value) filter (where parameter_code = 'pm25') as pm25 ,
        max(value) filter (where parameter_code = 'pm10') as pm10 
    from pm
    group by location_id, period_start_utc, date_day
)

select  
    location_id, 
    period_start_utc, 
    date_day,
    pm25,
    pm10, 
    round(pm25/ nullif(pm10,0) , 3) as pm25_to_pm10,
    pm25 > pm10 as pm25_exceeds_pm10,
    greatest(pm25 - pm10, 0) as excess 
from pairs 
where pm25 is not null and pm10 is not null 