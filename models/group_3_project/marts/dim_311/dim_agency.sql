{{ config(schema='group_3_marts') }}
WITH agency_dimension AS (
    SELECT DISTINCT 
        {{ dbt_utils.generate_surrogate_key(['agency']) }} AS agency_key,
        agency,         
        agency_name     
    FROM {{ ref('stg_311_requests') }}
    WHERE agency IS NOT NULL 
)

SELECT * FROM agency_dimension