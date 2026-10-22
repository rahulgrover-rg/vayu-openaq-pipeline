with rejects as (
    select * FROM {{ ref('stg_rejected') }}
    where date_local is not null
)
select 
    location_id,
    date_local as date_day,
    count(*) as rejected_readings,
    count(distinct sensor_id) as sensors_affected,
    count(*) filter (where reject_reason = 'null_value') as null_value,
    count(*) filter (where reject_reason = 'sentinel_value') as sentinel_value,
    count(*) filter (where reject_reason = 'sentinel_in_average') as sentinel_in_average,
    count(*) filter (where reject_reason = 'negative_value') as negative_value,
    count(*) filter (where reject_reason = 'above_instrument_range') as above_instrument_range,
    count(*) filter (where reject_reason in ('parameter_mismatch', 'invalid_time')) as other,
    min(period_start_utc) as first_rejected_hour_utc,
    max(period_start_utc) as last_rejected_hour_utc
from rejects
group by location_id, date_local
