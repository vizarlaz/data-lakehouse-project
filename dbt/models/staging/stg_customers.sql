WITH source AS (
    SELECT * FROM {{ source('raw', 'customers')}}
),

cleaned AS (
    SELECT
        customer_id,
        LOWER(TRIM(email)) AS email,
        INITCAP(TRIM(first_name)) AS first_name,
        INITCAP(TRIM(last_name)) AS last_name,
        UPPER(country) AS country,
        city,
        created_at,
        updated_at
    FROM source
    WHERE email IS NOT NULL
)

SELECT * FROM cleaned