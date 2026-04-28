{{ config(schema='group_3_marts') }}

WITH street_segment_dimension AS (
    SELECT DISTINCT 
        {{ dbt_utils.generate_surrogate_key(['street_name', 'from_street', 'to_street', 'direction']) }} AS street_segment_key,

        street_name,
        from_street,
        to_street,
        direction
        
    FROM {{ ref('stg_traffic_volumes') }}
    WHERE street_name IS NOT NULL 
       OR from_street IS NOT NULL
)

SELECT * FROM street_segment_dimension