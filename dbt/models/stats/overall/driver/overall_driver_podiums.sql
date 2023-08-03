with podiums as (
    select driver_id, constructor_id from {{ ref('stg_race_results') }}
    where position in (1,2,3)
)
select 
    rank() over (order by count(a.driver_id) desc)::varchar as pos,
    b.given_name || ' ' || b.family_name as driver, 
    count(a.driver_id)::varchar as podiums
from podiums a, 
    {{ ref('stg_drivers') }} b
where a.driver_id = b.driver_id
group by b.given_name || ' ' || b.family_name
order by count(a.driver_id) desc