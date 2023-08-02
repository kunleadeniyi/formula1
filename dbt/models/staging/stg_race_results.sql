with race_results as 
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
    "FastestLap_rank" as fastest_lap_rank,
    "FastestLap_lap" as fastest_lap,
    "FastestLap_Time_time" as fastest_lap_time,
    "FastestLap_AverageSpeed_units" as fastest_lap_time_speed_unit,
    "FastestLap_AverageSpeed_speed" as fastest_lap_time_avg_speed
FROM
    formula1.race_results
)
select * from race_results
