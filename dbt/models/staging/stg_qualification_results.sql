with quali as (
    SELECT
        season,
        round,
        url,
        "raceName" as race_name,
        date,
        time,
        "circuitId" as circuit_id,
        number,
        position,
        "Q1" as q1,
        "Driver_driverId" as driver_id,
        "Constructor_constructorId" as constructor_id,
        "Q2" as q2,
        "Q3" as q3
    FROM
        {{ source('formula1', 'qualification_results') }}
)
select * from quali 