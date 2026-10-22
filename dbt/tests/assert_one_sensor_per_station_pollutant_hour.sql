select 
    location_id, parameter_id, period_start_utc, count(distinct sensor_id) as sensors
from {{ ref('fact_measurement') }}
group by location_id, parameter_id, period_start_utc
having count(DISTINCT sensor_id) > 1 