WITH all_date AS (
    SELECT DISTINCT CAST(created_date AS DATE) AS full_date
    FROM {{ ref('stg_311_requests') }}
    WHERE created_date IS NOT NULL

    UNION DISTINCT
    SELECT DISTINCT CAST(closed_date AS DATE) AS full_date
    FROM {{ ref('stg_311_requests') }}
    WHERE closed_date IS NOT NULL

    UNION DISTINCT
    SELECT DISTINCT DATE(year, month, day) AS full_date
    FROM {{ ref('stg_traffic_volumes') }}
    WHERE year IS NOT NULL 
      AND month IS NOT NULL 
      AND day IS NOT NULL
),

date_dimension AS (
   SELECT
       {{ dbt_utils.generate_surrogate_key(['full_date']) }} AS date_key,
       full_date,
       EXTRACT(YEAR FROM full_date) AS year,
       EXTRACT(MONTH FROM full_date) AS month,
       EXTRACT(DAY FROM full_date) AS date
   FROM all_date
)

SELECT * FROM date_dimension