select f.sensor_id, f.period_start_utc,
f.location_id as fact_location, s.location_id as sensor_location,
f.parameter_id as fact_paramter, s.parameter_id as sensor_paramter

from {{ ref('fact_measurement') }} f 
join {{ ref('dim_sensor') }} s using (sensor_id)
where f.location_id <> s.location_id 
or f.parameter_id <> s.parameter_id