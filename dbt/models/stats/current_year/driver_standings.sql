with abc as ( 
    select driver_id, constructor_id, round, points
    from {{ ref('stg_race_results') }}
    --formula1.race_results 
    where season = date_part('year', now())
    
    union all
 
    select driver_id, constructor_id, round, points
    from {{ ref('stg_sprint_results') }}
    --formula1.sprint_results 
    where season = date_part('year', now())
 )
SELECT 
    RANK() OVER (ORDER BY SUM(points) DESC)::VARCHAR as pos,
    b.given_name || ' ' || b.family_name AS driver, 
    b.nationality AS nationality,
    c.name as team,
    SUM(points)::VARCHAR AS points
FROM abc a, 
    {{ ref('stg_drivers') }} b,
    {{ ref('stg_constructors') }} c
WHERE a.driver_id = b.driver_id
AND a.constructor_id = c.constructor_id
GROUP BY b.given_name || ' ' || b.family_name, b.nationality, c.name
ORDER BY sum(points) DESC