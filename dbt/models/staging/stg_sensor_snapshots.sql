with snapshots as (
    SELECT * FROM {{ source('raw_metadata','snapshots') }}
),
sensors as (
    SELECT snapshot_date, unnest(sensors) as x from snapshots 
)
select 
    snapshot_date,
    x.sensor_id, 
    x.location_id,
    x.parameter_id,
    x.units,
    TRY_CAST(x.first_utc as TIMESTAMP) as first_reading_utc,
    TRY_CAST(x.last_utc as TIMESTAMP) as last_reading_utc
from sensors