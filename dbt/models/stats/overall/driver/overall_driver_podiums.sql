with podiums as (
    select driver_id, constructor_id from {{ ref('stg_race_results') }}
    where position in (1,2,3)
),
drivers as (
    select
        driver_id,
        given_name || ' ' || family_name as driver,
        nationality
    from {{ ref('stg_drivers') }}
)
select 
    rank() over (order by count(a.driver_id) desc)::varchar as pos,
    b.driver, 
    b.nationality as nat,
    count(a.driver_id)::varchar as podiums
from podiums a, 
    drivers b
where a.driver_id = b.driver_id
group by b.driver, b.nationality
order by count(a.driver_id) desc