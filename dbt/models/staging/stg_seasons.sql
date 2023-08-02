with seasons as (
    SELECT
        season,
        round,
        url,
        "raceName" as race_name,
        date,
        time,
        "circuitId" as circuit_id,
        fp1_date,
        fp1_time,
        fp2_date,
        fp2_time,
        fp3_date,
        fp3_time,
        qualifying_date,
        qualifying_time,
        sprint_date,
        sprint_time
    FROM
        {{ source('formula1', 'seasons') }}
)
select * from seasons