WITH all_boro AS (
    SELECT DISTINCT borough
    FROM {{ ref('stg_311_requests') }}
    WHERE borough IS NOT NULL

    UNION DISTINCT

    SELECT DISTINCT borough
    FROM {{ ref('stg_traffic_volumes') }} 
    WHERE borough IS NOT NULL 
),

location_dimension AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['borough']) }} AS location_key,
        borough
    FROM all_boro
)

SELECT * FROM location_dimension