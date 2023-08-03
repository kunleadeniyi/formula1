with races as (
    select driver_id from {{ ref('stg_race_results') }}
),
drivers as (
    select 
        driver_id, 
        given_name || ' ' || family_name as driver, 
        nationality
    from 
        {{ ref('stg_drivers') }}
)
select 
    b.driver,
    b.nationality,
    count(a.driver_id)::varchar as races
from races a, drivers b
where a.driver_id = b.driver_id
group by b.driver, b.nationality
order by count(a.driver_id) desc
limit 20
    