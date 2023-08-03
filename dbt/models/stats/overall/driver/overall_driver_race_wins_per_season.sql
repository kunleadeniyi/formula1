with winners as (
    select driver_id, constructor_id, season from {{ ref('stg_race_results') }}
    where position = 1
),
race_count as (
    select season, count(season) as races from {{ ref('stg_seasons') }}
    group by season
)
select 
    rank() over (order by count(a.driver_id) desc)::varchar as pos,
    b.given_name || ' ' || b.family_name as driver,
    c.name as team, 
    a.season::varchar,
    count(a.driver_id)::varchar as wins,
    d.races,
    round((count(a.driver_id)::numeric/d.races * 100)::numeric, 2) as percentage
from winners a, 
    {{ ref('stg_drivers') }} b, 
    {{ ref('stg_constructors') }} c,
    race_count d
where a.driver_id = b.driver_id
    and c.constructor_id = a.constructor_id
    and a.season = d.season
group by a.season, b.given_name || ' ' || b.family_name, c.name, d.races
order by count(a.driver_id) DESC, count(a.driver_id)::numeric/d.races desc, a.season desc


