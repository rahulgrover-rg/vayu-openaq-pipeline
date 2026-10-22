with per_day as (
    SELECT 
        date_day,
        count(distinct location_id) as stations_active,
        count(distinct location_id) filter (where data_status <> 'sufficient') as stations_affected
    from {{ ref('fact_sensor_daily') }}
    group by date_day
),
latest as (
    select max(date_day) as latest_day 
    from per_day
)

select 
    p.date_day,
    p.stations_active,
    p.stations_affected,
    round(100.0*p.stations_affected / p.stations_active , 1) as pct_affected,
    CASE
        WHEN p.date_day = l.latest_day then 'latest_incomplete'
        when p.stations_affected >= {{ var('network_outage_share') }} * p.stations_active then 'network_outage'
        else 'normal'
    end as day_type 
from per_day as p
cross join latest l 
