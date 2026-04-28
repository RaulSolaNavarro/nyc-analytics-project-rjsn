{{ config(schema='group_3_marts') }}

WITH dim_date AS (
    SELECT date_key FROM {{ ref('dim_date') }}
),
dim_time AS (
    SELECT time_key FROM {{ ref('dim_time') }}
),
dim_location AS (
    SELECT location_key FROM {{ ref('dim_location') }}
),
dim_agency AS (
    SELECT agency_key FROM {{ ref('dim_agency') }}
),
dim_problem AS (
    SELECT problem_key FROM {{ ref('dim_problem') }}
),
dim_incident_location AS (
    SELECT incident_location_key FROM {{ ref('dim_incident_location') }}
),

stg_311 AS (
    SELECT * FROM {{ ref('stg_311_requests') }}
)

SELECT
    dot_request_id AS unique_key, 
    {{ dbt_utils.generate_surrogate_key(['borough']) }} AS location_id,
    {{ dbt_utils.generate_surrogate_key(['agency']) }} AS agency_id,
    {{ dbt_utils.generate_surrogate_key(['complaint_type', 'descriptor']) }} AS problem_id,
    

    {{ dbt_utils.generate_surrogate_key([
        "COALESCE(UPPER(TRIM(incident_zip)), 'NA')",
        "COALESCE(UPPER(TRIM(incident_address)), 'NA')",
        "COALESCE(UPPER(TRIM(intersection_street_1)), 'NA')",
        "COALESCE(UPPER(TRIM(intersection_street_2)), 'NA')"
    ]) }} AS incident_location_id,


    {{ dbt_utils.generate_surrogate_key(["CAST(created_date AS DATE)"]) }} AS created_date_id,
    {{ dbt_utils.generate_surrogate_key(["EXTRACT(TIME FROM created_date)"]) }} AS created_time_id,



    {{ dbt_utils.generate_surrogate_key(["CAST(closed_date AS DATE)"]) }} AS closed_date_id,


    {{ dbt_utils.generate_surrogate_key(["EXTRACT(TIME FROM closed_date)"]) }} AS closed_time_id,

    latitude,
    longitude,


    status AS additional_details

FROM stg_311