with teams as (
    SELECT
        "constructorId" as constructor_id,
        url,
        name,	
        nationality
    from {{ source('formula1', 'constructors') }}
)
selct * from teams