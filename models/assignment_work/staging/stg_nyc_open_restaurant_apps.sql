-- Clean and standardize NYC Open Restaurant Applications data
-- One row per restaurant application

WITH source AS (
    SELECT * FROM {{ source('raw', 'source_nyc_open_restaurant_apps') }}
),

cleaned AS (
    SELECT
        * EXCEPT (
            objectid,
            time_of_submission,
            zip,
            latitude,
            longitude,
            approved_for_sidewalk_seating,
            approved_for_roadway_seating,
            roadway_dimensions_length,
            roadway_dimensions_width,
            roadway_dimensions_area,
            sidewalk_dimensions_length,
            sidewalk_dimensions_width,
            sidewalk_dimensions_area
        ),

        -- Identifiers
        CAST(objectid AS STRING) AS application_id,

        -- Date/Time
        SAFE_CAST(REPLACE(time_of_submission, '.000', '') AS TIMESTAMP) AS time_of_submission,

        -- Location
        CASE
            WHEN UPPER(TRIM(zip)) IN ('N/A', 'NA') THEN NULL
            WHEN LENGTH(CAST(zip AS STRING)) = 5 THEN CAST(zip AS STRING)
            WHEN LENGTH(CAST(zip AS STRING)) = 9 THEN CAST(zip AS STRING)
            WHEN LENGTH(CAST(zip AS STRING)) = 10
                AND REGEXP_CONTAINS(CAST(zip AS STRING), r'^\d{5}-\d{4}')
            THEN CAST(zip AS STRING)
            ELSE NULL
        END AS zip,

        SAFE_CAST(latitude AS DECIMAL) AS latitude,
        SAFE_CAST(longitude AS DECIMAL) AS longitude,

        -- Approval status as booleans
        CASE
            WHEN UPPER(TRIM(approved_for_sidewalk_seating)) = 'YES' THEN TRUE
            WHEN UPPER(TRIM(approved_for_sidewalk_seating)) = 'NO' THEN FALSE
            ELSE NULL
        END AS approved_for_sidewalk_seating,

        CASE
            WHEN UPPER(UPPER(TRIM(approved_for_roadway_seating))) = 'YES' THEN TRUE
            WHEN UPPER(TRIM(approved_for_roadway_seating)) = 'NO' THEN FALSE
            ELSE NULL
        END AS approved_for_roadway_seating,

        -- Dimensions as numeric
        SAFE_CAST(sidewalk_dimensions_length AS DECIMAL) AS sidewalk_dimensions_length,
        SAFE_CAST(sidewalk_dimensions_width AS DECIMAL) AS sidewalk_dimensions_width,
        SAFE_CAST(sidewalk_dimensions_area AS DECIMAL) AS sidewalk_dimensions_area,
        SAFE_CAST(roadway_dimensions_length AS DECIMAL) AS roadway_dimensions_length,
        SAFE_CAST(roadway_dimensions_width AS DECIMAL) AS roadway_dimensions_width,
        SAFE_CAST(roadway_dimensions_area AS DECIMAL) AS roadway_dimensions_area,

        -- Metadata
        CURRENT_TIMESTAMP() AS _stg_loaded_at

    FROM source

    WHERE objectid IS NOT NULL

    QUALIFY ROW_NUMBER() OVER (PARTITION BY objectid ORDER BY time_of_submission DESC) = 1
)

SELECT * FROM cleaned




