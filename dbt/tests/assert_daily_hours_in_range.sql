select sensor_id, date_day, hours_present 
from {{ ref('fact_sensor_daily') }}
where hours_present < 0 or hours_present > 24