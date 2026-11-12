select 

    f.date_day,
    d.year_month,
    d.month_name,
    d.week_start,
    d.season,
    d.is_crop_burning_season,
    l.location_id,
    l.station_name,
    l.city,
    l.district,
    l.agency,
    l.latitude,
    l.longitude,
    l.is_currently_reporting,
    p.parameter_code,
    p.display_name as pollutant,
    p.units,
    p.naaqs_24h_in_units,
    p.who_24h_in_units,
    f.data_status,
    f.hours_usable,
    f.capture_pct,
    f.daily_mean_valid,
    f.exceeds_naaqs_24h,
    f.exceeds_who_24h,
    n.day_type

from {{ ref('fact_sensor_daily') }} f 
join {{ ref('dim_date') }} d using (date_day)
join {{ ref('dim_location') }} l using (location_id)
join {{ ref('dim_parameter') }} p using (parameter_id)
left join {{ ref('qa_network_days') }} n using (date_day)
