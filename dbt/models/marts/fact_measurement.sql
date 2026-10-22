{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = 'source_file',
        on_schema_change = 'fail'
    )
}}

with readings as (
    select * 
    from {{ ref('stg_readings') }}

    {% if is_incremental() %} 
    where fetched_at > (select max(fetched_at) from {{ this }})
    {% endif %}
)

select 
    sensor_id,
    period_start_utc,
    location_id,
    parameter_id,
    date_local as date_day,
    period_start_local,
    hour_local,
    value,
    units,
    pct_complete,
    observed_count,
    expected_count,
    min_in_hour,
    max_in_hour,
    quality_flags,
    is_low_coverage,
    is_source_flagged,
    is_at_range_ceiling,
    fetched_at,
    source_file
FROM readings 