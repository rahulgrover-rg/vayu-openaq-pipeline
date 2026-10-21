with snaps as ( 
    SELECT * FROM {{ ref('stg_station_snapshots') }}
),
latest as (
    SELECT
        *,
        MIN(snapshot_date) over (PARTITION BY location_id) as first_seen_on,
        MAX(snapshot_date) over (PARTITION BY location_id) as last_seen_on
    FROM SNAPS 
    qualify row_number() over (PARTITION BY LOCATION_ID ORDER BY SNAPSHOT_DATE DESC) = 1
),
parsed as (
    select 
        *,
        last_seen_on = (select max(snapshot_date) from snaps) as is_in_scope_now,
        NULLIF(regexp_extract(station_name, ' - ([A-Za-z]+)\s*$', 1), '') AS agency,
        NULLIF(TRIM(regexp_extract(station_name, ',\s*([^,]+?)\s+-\s+[A-Za-z]+\s*$', 1)), '') AS city_raw
    FROM LATEST
),
cities as (
    SELECT * 
    FROM {{ ref('city_map') }}
),
current_snapshot as (
    select max(snapshot_date) as snapshot_date from snaps
)

select 
    p.location_id,
    p.station_name,
    p.agency,
    c.city,
    c.district,
    c.state,
    p.latitude,
    p.longitude,
    case when p.provider_name is null or p.provider_name in ('','N/A') then 'Unknown' else p.provider_name end as provider_name,
    p.openaq_is_monitor,
    p.first_reading_utc,
    p.last_reading_utc,
    date_diff('day', cast(p.last_reading_utc as date), current_date) as days_quiet_at_last_check,
    p.is_in_scope_now and date_diff('day', cast(p.last_reading_utc as date), current_date) <= 3 as is_currently_reporting,
    p.first_seen_on,
    p.last_seen_on,
    p.is_in_scope_now
FROM parsed p  
left join cities c on c.city_raw = p.city_raw 
cross join current_snapshot cs 

