select  
    sensor_id,
    location_id,
    parameter_id,
    period_start_utc,
    date_local,
    hour_local,
    value,
    min_in_hour,
    max_in_hour,
    reject_reason,
    source_file  
from {{ source('rejected', 'hours') }}