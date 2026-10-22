with ordered as (
    select 
        sensor_id, location_id, parameter_id, period_start_utc, value,
        lag(value) over w as prev_value,
        lag(period_start_utc) over w as prev_start
    from {{ ref('fact_measurement') }}
    window w as (partition by sensor_id order by period_start_utc)
),
marked as (
    select 
        *,
        case when value = prev_value and period_start_utc = prev_start + interval 1 hour then 0 else 1 end as starts_new_run
    from ordered
),
numbered as (
    select 
        *,
        sum(starts_new_run) over (PARTITION BY sensor_id order by period_start_utc rows between unbounded preceding and current row) as run_id
    from marked
),
runs as (
    select 
        sensor_id,
        location_id,
        parameter_id,
        run_id,
        min(period_start_utc) as run_start_utc,
        max(period_start_utc) + interval 1 hour as run_end_utc,
        count(*) as hours,
        any_value(value) as value,
    from numbered 
    group by sensor_id, location_id, parameter_id, run_id
)
select 
    sensor_id,
    location_id,
    parameter_id,
    run_start_utc,
    run_end_utc,
    hours,
    value,
    value = 0 as is_zero
from runs 
where hours >= {{ var('flatline_min_hours') }}