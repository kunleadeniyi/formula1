with sprint_results as 
(
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
    "positionText" as position_text,
    points,
    grid,
    laps,
    status,
    "Driver_driverId" as driver_id,
    "Constructor_constructorId" as constructor_id,
    "Time_millis" as race_time_millis,
    "Time_time" as race_time,
    "FastestLap_lap" as fastest_lap,
    "FastestLap_Time_time" as fastest_lap_time
FROM
    formula1.sprint_results
)
select * from sprint_results
