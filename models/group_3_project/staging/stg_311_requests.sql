WITH source AS (
    SELECT * FROM {{ source('traffic_raw_source', 'group_3_source_dot_311_requests_history') }}
),

cleaned AS (
   SELECT
       -- Get all columns from source, except ones we're transforming below
       -- To do cleaning on them or explicitly cast them as types just in case
       * EXCEPT (
           unique_key,
           created_date,
           closed_date,
           agency,
           agency_name,
           complaint_type,
           descriptor,
           status,
           incident_zip,
           borough,
           incident_address,
           street_name,
           cross_street_1,
           cross_street_2,
           latitude,
           longitude,
           open_data_channel_type,
           police_precinct 
       ),

       -- Identifiers
       {{ dbt_utils.generate_surrogate_key(['unique_key']) }} as dot_request_key,
       CAST(unique_key AS STRING) AS dot_request_id,

       -- Date/Time
       CAST(created_date AS TIMESTAMP) AS created_date,
       CAST(closed_date AS TIMESTAMP) AS closed_date,

       -- Request details
       CAST(agency AS STRING) AS agency,
       CAST(agency_name AS STRING) AS agency_name,
       CAST(complaint_type AS STRING) AS complaint_type,
       CAST(descriptor AS STRING) AS descriptor,
       UPPER(TRIM(CAST(status AS STRING))) AS status,
               CASE
            WHEN police_precinct IN ('Unspecified', 'unspecified') THEN NULL
            ELSE police_precinct 
        END AS police_precinct,

       -- Location - clean zip code, handling several common zip code data problems
       CASE
           WHEN UPPER(TRIM(CAST(incident_zip AS STRING))) IN ('N/A', 'NA') THEN NULL
           WHEN UPPER(TRIM(CAST(incident_zip AS STRING))) = 'ANONYMOUS' THEN 'Anonymous'
           WHEN LENGTH(CAST(incident_zip AS STRING)) = 5 THEN CAST(incident_zip AS STRING)
           WHEN LENGTH(CAST(incident_zip AS STRING)) = 9 THEN CAST(incident_zip AS STRING)
           WHEN LENGTH(CAST(incident_zip AS STRING)) = 10
               AND REGEXP_CONTAINS(CAST(incident_zip AS STRING), r'^\d{5}-\d{4}')
           THEN CAST(incident_zip AS STRING)
           ELSE NULL
       END AS incident_zip,

       -- Location - standardized borough, just in case
       CASE
           WHEN UPPER(TRIM(borough)) IN ('MANHATTAN', 'NEW YORK COUNTY') THEN 'Manhattan'
           WHEN UPPER(TRIM(borough)) IN ('BRONX', 'THE BRONX') THEN 'Bronx'
           WHEN UPPER(TRIM(borough)) IN ('BROOKLYN', 'KINGS COUNTY') THEN 'Brooklyn'
           WHEN UPPER(TRIM(borough)) IN ('QUEENS', 'QUEEN', 'QUEENS COUNTY') THEN 'Queens'
           WHEN UPPER(TRIM(borough)) IN ('STATEN ISLAND', 'RICHMOND COUNTY') THEN 'Staten Island'
       END AS borough,

       CAST(incident_address AS STRING) AS incident_address,
       CAST(street_name AS STRING) AS street_name,
       CAST(cross_street_1 AS STRING) AS cross_street_1,
       CAST(cross_street_2 AS STRING) AS cross_street_2,
       CAST(latitude AS DECIMAL) AS latitude,
       CAST(longitude AS DECIMAL) AS longitude,

       -- Clearer column name as well for this one
       CAST(open_data_channel_type AS STRING) AS method_of_submission,

       -- Metadata
       CURRENT_TIMESTAMP() AS _stg_loaded_at

   FROM source

   -- Filters
   WHERE (agency = 'DOT' OR agency_name LIKE '%Transportation%')
   AND unique_key IS NOT NULL
   AND created_date IS NOT NULL
   AND CAST(created_date AS DATETIME) >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 7 YEAR)
   AND borough IS NOT NULL
   AND UPPER(TRIM(borough)) IN (
    'MANHATTAN', 'NEW YORK COUNTY', 
    'BRONX', 'THE BRONX', 
    'BROOKLYN', 'KINGS COUNTY', 
    'QUEENS', 'QUEEN', 'QUEENS COUNTY', 
    'STATEN ISLAND', 'RICHMOND COUNTY'
)

   -- Deduplicate
   QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_key ORDER BY created_date DESC) = 1
)

SELECT * FROM cleaned