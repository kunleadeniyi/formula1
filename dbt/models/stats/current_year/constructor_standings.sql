with abc as ( 
    select constructor_id, round, points
    from {{ ref('stg_race_results') }}
    --formula1.race_results 
    where season = 2023
    
    union all
 
    select "Constructor_constructorId" as constructor_id, round, points
    from {{ ref('stg_sprint_results') }}
    --formula1.sprint_results 
    where season = 2023
 )
select 
    rank() over (order by sum(points) desc) as pos,
    b.name as team, 
    sum(points) as points
from abc a, {{ ref('stg_constructors') }} b -- formula1.constructors b
where a.constructor_id = b.constructor_id
group by 1 
order by sum(points) desc