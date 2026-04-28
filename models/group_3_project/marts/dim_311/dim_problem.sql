{{ config(schema='group_3_marts') }}

WITH problem_dimension AS (
    SELECT DISTINCT 
        {{ dbt_utils.generate_surrogate_key(['complaint_type', 'descriptor']) }} AS problem_key,
        
        complaint_type AS problem_type,         
        descriptor     
    FROM {{ ref('stg_311_requests') }}
    WHERE descriptor IS NOT NULL 
)
SELECT * FROM problem_dimension