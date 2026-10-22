select sensor_id, date_day, COUNT(*) as rows_for_this_day 
from {{ ref('fact_sensor_daily') }} 
group by sensor_id, date_day 
having COUNT(*) > 1