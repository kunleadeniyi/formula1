with winners as (
    select constructor_id from {{ ref('stg_race_results') }}
    where season = date_part('year', now()) 
    and position = 1
)
select 
    rank() over (order by count(a.constructor_id) desc)::varchar as pos, 
    c.name as team,
    count(a.constructor_id)::varchar as wins
from winners a, 
    {{ ref('stg_constructors') }} c
where a.constructor_id = c.constructor_id
group by c.name 
order by count(a.constructor_id) DESC