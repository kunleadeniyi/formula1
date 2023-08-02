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
        Q1,
        "Driver_driverId" as driver_id,
        "Constructor_constructorId" as constructor_id,
        Q2,
        Q3
    FROM
        formula1.qualifying_results
)
select * from quali 