
with current_season_drivers as (
    select distinct driver_id, constructor_id from {{ ref('stg_race_results') }}
    where season = date_part('year', now())
),
teams as (
    select * from {{ ref('stg_constructors') }}
)
,driver_info as (
    select 
    a.driver_id, 
    given_name || ' ' || family_name as driver,
    date_part('year', age(now(), date_of_birth)) as age,
    c.name as team,
    a.nationality,
    code
    from {{ ref('stg_drivers') }} a, 
        current_season_drivers b,
        teams c
    where a.driver_id = b.driver_id
    and c.constructor_id = b.constructor_id
)
select 
    driver_id,
    driver,
    age,
    team,
    nationality,
    code
from driver_info