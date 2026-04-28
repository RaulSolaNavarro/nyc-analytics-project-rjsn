WITH source AS (
    SELECT * FROM {{ source('traffic_raw_source', 'group_3_source_traffic_volume_history') }}
),

cleaned AS (
    SELECT
        -- 1. Create Surrogate key
        {{ dbt_utils.generate_surrogate_key(['request_id', 'segment_id', 'year', 'month', 'day', 'hour', 'minute']) }} as traffic_request_key,

        -- 2. Rename & IDs
        CAST(request_id AS STRING) AS traffic_request_id,
        CAST(segment_id AS STRING) AS segment_id,

        -- 3. String 
        CAST(direction AS STRING) AS direction,
        CAST(from_street AS STRING) AS from_street,
        CAST(to_street AS STRING) AS to_street,
        CAST(street_name AS STRING) AS street_name,

        -- 4. Numeric 
        CAST(traffic_volume AS INT64) AS traffic_volume,
        CAST(year AS INT64) AS year,
        CAST(month AS INT64) AS month,
        CAST(day AS INT64) AS day,
        CAST(hour AS INT64) AS hour,
        CAST(minute AS INT64) AS minute,

        -- 5. Longitude and Latitude Extraction
        SAFE_CAST(SPLIT(TRIM(REPLACE(REPLACE(wkt_geom, 'POINT (', ''), ')', '')), ' ')[OFFSET(0)] AS FLOAT64) AS longitude,
        SAFE_CAST(SPLIT(TRIM(REPLACE(REPLACE(wkt_geom, 'POINT (', ''), ')', '')), ' ')[OFFSET(1)] AS FLOAT64) AS latitude,

        -- 6. Borough
        CASE
           WHEN UPPER(TRIM(borough)) IN ('MANHATTAN', 'NEW YORK COUNTY') THEN 'Manhattan'
           WHEN UPPER(TRIM(borough)) IN ('BRONX', 'THE BRONX') THEN 'Bronx'
           WHEN UPPER(TRIM(borough)) IN ('BROOKLYN', 'KINGS COUNTY') THEN 'Brooklyn'
           WHEN UPPER(TRIM(borough)) IN ('QUEENS', 'QUEEN', 'QUEENS COUNTY') THEN 'Queens'
           WHEN UPPER(TRIM(borough)) IN ('STATEN ISLAND', 'RICHMOND COUNTY') THEN 'Staten Island'
           ELSE 'UNKNOWN or CITYWIDE'
        END AS borough,

    FROM source
    WHERE request_id IS NOT NULL
    
    -- 6. Deduplication 
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY request_id, segment_id, year, month, day, hour, minute 
        ORDER BY year DESC
    ) = 1
)

SELECT * FROM cleaned