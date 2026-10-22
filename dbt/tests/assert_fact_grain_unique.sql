select sensor_id, period_start_utc, count(*) as rows_for_this_hour 
from {{ ref('fact_measurement') }}
group by sensor_id, period_start_utc
having count(*) > 1