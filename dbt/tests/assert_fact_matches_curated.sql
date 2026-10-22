with fact as (
    select count(*) as n      
    from {{ ref('fact_measurement') }}
),
curated as (
    select count(*) as n
    from {{ ref('stg_readings') }}
)
select fact.n as fact_rows, curated.n as curated_rows 
from fact, curated 
where fact.n <> curated.n