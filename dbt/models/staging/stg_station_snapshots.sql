with snapshots as (
    SELECT * 
    FROM {{ source('raw_metadata', 'snapshots') }}
),
stations as (
    SELECT snapshot_date, unnest(stations) AS s
    FROM SNAPSHOTS
)
SELECT 
    snapshot_date,
    s.id as location_id,
    s.name as station_name, 
    s.provider.name as provider_name,
    s.owner.name as owner_name,
    s.isMonitor as openaq_is_monitor,
    s.coordinates.latitude as latitude,
    s.coordinates.longitude as longitude,
    TRY_CAST(s.datetimeFirst.utc as TIMESTAMP) as first_reading_utc,
    TRY_CAST(s.datetimeLast.utc as TIMESTAMP) as last_reading_utc
FROM stations