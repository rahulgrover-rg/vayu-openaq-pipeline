WITH incidents as (
    select sum(rejected_readings) as n from {{ ref('qa_station_incidents') }}
),
rejected as (
    select count(*) as n from {{ ref('stg_rejected') }} where date_local is not null

)
select 
    incidents.n AS incident_rows, rejected.n as rejected_rows 
    from incidents, rejected 
    where incidents.n <> rejected.n