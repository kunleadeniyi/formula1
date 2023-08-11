with podiums as (
    select driver_id, constructor_id from {{ ref('stg_race_results') }}
    where season = date_part('year', now()) 
    and position in (1,2,3)
)
select
    a.driver_id, 
    rank() over (order by count(a.driver_id) desc)::varchar as pos,
    b.given_name || ' ' || b.family_name as driver, 
    c.name as team,
    count(a.driver_id)::varchar as podiums
from podiums a, 
    {{ ref('stg_drivers') }} b,
    {{ ref('stg_constructors') }} c
where a.driver_id = b.driver_id
    and a.constructor_id = c.constructor_id
group by  a.driver_id, b.given_name || ' ' || b.family_name, c.name 
order by count(a.driver_id) DESC