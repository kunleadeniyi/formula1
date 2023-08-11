/*
driver fact table for dashboard
only contains drivers for the current season
*/

with driver_info as (
    select * from {{ ref('int_driver_info') }}
)
, current_season_wins as (
    select * from {{ ref('driver_race_wins') }}
)
, current_podiums as (
    select * from {{ ref('driver_race_podiums') }}
),
current_standings as (
    select * from {{ ref('driver_standings') }}
), 
overall_wins as (
    select * from {{ref('overall_driver_race_wins')}}
),
overall_podiums as (
    select * from {{ ref('overall_driver_podiums') }}
), 
current_race_info as (
    select * from {{ ref('int_most_recent_driver_info') }}
)
-- select * from driver_info
select 
    s.pos,
    a.code,
    a.driver,
    a.age::varchar,
    a.team,
    a.nationality,
    coalesce(w.wins::integer, 0)::varchar as wins,
    coalesce(p.podiums::integer, 0)::varchar as podiums,
    s.points as points,
    r.races_count::varchar,
    r.race_name last_race,
    r.position_text last_race_pos,
    r.grid::varchar,
    r.circuit_name,
    coalesce(ow.wins::integer, 0)::varchar as overall_wins,
    coalesce(op.podiums::integer, 0)::varchar as overall_podiums
from   
    driver_info a /*can join on driver because it is a derived column would not change (driver_id is still a better option)*/
    left join current_season_wins w using (driver)
    left join current_podiums p using (driver)
    left join current_standings s using (driver)
    left join overall_podiums op using (driver)
    left join overall_wins ow using (driver)
    left join current_race_info r using (driver)
order by s.pos::integer

