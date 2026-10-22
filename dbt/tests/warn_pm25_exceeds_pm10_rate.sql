{{ config(severity = 'warn') }}

select 
    location_id,
    date_trunc('month', date_day) as month,
    count(*) as paired_hours,
    count(*) filter (where pm25_exceeds_pm10)  as exceed_hours,
    round(100.0 * count(*) filter (where pm25_exceeds_pm10) / count(*) , 1) as exceed_pct
from {{ ref('qa_pm_consistency') }}
group by location_id, date_trunc('month', date_day)
having count(*) >= 48 
and 100.0 * count(*) filter (where pm25_exceeds_pm10) / count(*) > 5