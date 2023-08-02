with circuits as (
    SELECT
        "circuitId" as circuit_id,
        url,
        "circuitName" as circuit_name,
        lat,
        long,
        locality,
        country
    FROM
        --formula1.circuits
        {{ source('formula1', 'circuits') }}
)
select * from circuits