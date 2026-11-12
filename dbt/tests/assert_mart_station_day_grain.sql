select location_id, parameter_code, date_day, count(*) as rows_for_this_day
from {{ ref('mart_station_day') }} 
group by location_id, parameter_code, date_day
having count(*) > 1