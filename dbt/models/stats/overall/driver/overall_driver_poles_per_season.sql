with poles as (
    select driver_id, constructor_id, season from {{ ref('stg_race_results') }}
    where grid = 1
),
race_count as (
    select 
        season,
        max(round) as races 
    from {{ ref('stg_race_results') }}
    group by season
), 
final as (
    select 
        rank() over (partition by a.season order by count(a.driver_id) desc) as pos,
        b.given_name || ' ' || b.family_name as driver,
        c.name as team, 
        a.season,
        count(a.driver_id) as poles,
        d.races,
        round((count(a.driver_id)::numeric/d.races * 100)::numeric, 2) as percentage
    from poles a, 
        {{ ref('stg_drivers') }} b, 
        {{ ref('stg_constructors') }} c,
        race_count d
    where a.driver_id = b.driver_id
        and c.constructor_id = a.constructor_id
        and a.season = d.season
    group by a.season, b.given_name || ' ' || b.family_name, c.name, d.races
    order by a.season desc, count(a.driver_id) DESC, count(a.driver_id)::numeric/d.races desc--, a.season desc
)
select 
    season::varchar, 
    driver, 
    team,  
    poles::varchar, 
    races::varchar 
from final
where pos = 1

