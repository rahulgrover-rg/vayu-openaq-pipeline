with source as (
    select * 
    from {{ source('curated', 'hours') }}
)

select 
    sensor_id,
    location_id, 
    parameter_id,
    period_start_utc,
    period_end_utc,
    period_start_local,
    date_local,
    hour_local,
    value,
    units,
    pct_complete,
    observed_count,
    expected_count,
    min_in_hour,
    max_in_hour,
    coalesce(quality_flags, []::varchar[]) as quality_flags,
    list_contains(quality_flags, 'low_coverage') as is_low_coverage,
    list_contains(quality_flags, 'source_flagged') as is_source_flagged,
    list_contains(quality_flags, 'at_range_ceiling') as is_at_range_ceiling,
    fetched_at,
    source_file
from source 