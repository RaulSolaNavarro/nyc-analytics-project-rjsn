WITH all_times AS (
    SELECT DISTINCT 
        EXTRACT(TIME FROM created_date) AS time_of_day
    FROM {{ ref('stg_311_requests') }}
    WHERE created_date IS NOT NULL

    UNION DISTINCT

    SELECT DISTINCT 
        EXTRACT(TIME FROM closed_date) AS time_of_day
    FROM {{ ref('stg_311_requests') }}
    WHERE closed_date IS NOT NULL

    UNION DISTINCT


    SELECT DISTINCT TIME(hour, minute, 0) AS time_of_day
    FROM {{ ref('stg_traffic_volumes') }}
    WHERE hour IS NOT NULL 
    AND minute IS NOT NULL
),

time_dimension AS (
    SELECT

        {{ dbt_utils.generate_surrogate_key(['time_of_day']) }} AS time_key, 

        time_of_day,
        EXTRACT(HOUR FROM time_of_day) AS hour,
        EXTRACT(MINUTE FROM time_of_day) AS minute,
        EXTRACT(SECOND FROM time_of_day) AS second
    FROM all_times
)

SELECT * FROM time_dimension