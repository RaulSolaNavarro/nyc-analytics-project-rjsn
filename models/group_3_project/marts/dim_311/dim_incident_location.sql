{{ config(schema='group_3_marts') }}

WITH staging AS (
    SELECT
        UPPER(TRIM(CAST(incident_zip AS STRING))) AS zip_code,
        UPPER(TRIM(incident_address)) AS address,
        UPPER(TRIM(street_name)) AS street,
        UPPER(TRIM(cross_street_1)) AS cross_1,
        UPPER(TRIM(cross_street_2)) AS cross_2,
        UPPER(TRIM(intersection_street_1)) AS int_street_1,
        UPPER(TRIM(intersection_street_2)) AS int_street_2,
        UPPER(TRIM(city)) AS city,
        police_precinct,
        community_board,
        council_district
    FROM {{ ref('stg_311_requests') }}
),

location_dimension AS (
    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key([
            "COALESCE(zip_code, 'NA')",
            "COALESCE(address, 'NA')",
            "COALESCE(int_street_1, 'NA')",
            "COALESCE(int_street_2, 'NA')"
        ]) }} AS incident_location_key,

        zip_code,
        address,
        street,
        cross_1,
        cross_2,
        int_street_1,
        int_street_2,
        city,
        police_precinct,
        community_board,
        council_district

    FROM staging
    WHERE zip_code IS NOT NULL 
       OR address IS NOT NULL 
       OR int_street_1 IS NOT NULL
)

SELECT * FROM location_dimension