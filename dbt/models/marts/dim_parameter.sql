with params as (
    SELECT * FROM {{ ref('parameters') }}
)

select 
    parameter_id, 
    parameter_code,
    display_name,
    units,
    naaqs_24h_ugm3,
    who_24h_ugm3,
    round(naaqs_24h_ugm3/ugm3_per_unit,1) as naaqs_24h_in_units,
    round(who_24h_ugm3/ugm3_per_unit,1) as who_24h_in_units 
from params