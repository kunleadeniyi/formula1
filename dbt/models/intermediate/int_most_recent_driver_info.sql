with
    abc as (
        select driver_id, count(driver_id) races_count, season, max(round) round  -- last race
        from {{ ref("stg_race_results") }}
        where season = date_part('year', now())
        group by season, driver_id
    ),
    most_recent_driver_info as (
        select a.*, b.race_name, b.circuit_id, b.position_text, b.grid
        from abc a
        left join {{ ref("stg_race_results") }} b using (season, round)
        where a.season = b.season and a.round = b.round and a.driver_id = b.driver_id
    ),
    drivers as (
        select driver_id, 
            given_name || ' ' || family_name as driver
        from {{ ref('stg_drivers') }}
    ), circuits as (
        select * 
        from {{ ref('stg_circuits') }}
    )

select 
    a.driver_id,
    c.driver,
    a.races_count,
    a.season, 
    a.round,
    a.race_name,
    b.circuit_name,
    a.position_text,
    a.grid
from most_recent_driver_info a,
    circuits b,
    drivers c
where a.circuit_id = b.circuit_id
and c.driver_id = a.driver_id

