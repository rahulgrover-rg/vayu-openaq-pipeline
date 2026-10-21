with snaps as (
    SELECT * FROM {{ ref('stg_sensor_snapshots') }}
)
select 
    sensor_id, 
    location_id,
    parameter_id,
    units,
    first_reading_utc,
    last_reading_utc,
    min(snapshot_date) over (PARTITION BY sensor_id) as first_seen_on,
    max(snapshot_date) over (PARTITION BY sensor_id) as last_seen_on
from snaps 
qualify row_number() over (PARTITION BY sensor_id order by snapshot_date desc) = 1