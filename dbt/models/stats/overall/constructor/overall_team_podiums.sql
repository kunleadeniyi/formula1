with podiums as (
    select constructor_id, position
    from {{ ref('stg_race_results') }}
    where position in (1,2,3)
),
teams as (
    select constructor_id, name, nationality
    from {{ ref('stg_constructors') }}
)
select 
    b.name as team,
    b.nationality as nat,
    count(a.constructor_id)::varchar as podiums
from podiums a, teams b
where a.constructor_id = b.constructor_id
group by b.name, b.nationality
order by count(a.constructor_id) desc

