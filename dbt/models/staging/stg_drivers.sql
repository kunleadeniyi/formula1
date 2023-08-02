with drivers as (
    SELECT
        "driverId" as driver_id,
        url,
        "givenName" as given_name,
        "familyName" as family_name,
        "dateOfBirth" as date_of_birth,
        nationality,
        "permanentNumber" as permanent_number,
        code
    FROM
        {{ source('formula1', 'drivers') }}
        
)
selct * from drivers