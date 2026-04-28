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
dim_street_segment AS (
    SELECT street_segment_key FROM {{ ref('dim_street_segment') }}
),

traffic_staging AS (
    SELECT * FROM {{ ref('stg_traffic_volumes') }} -- 
)

SELECT
    ts.traffic_request_id AS requestid,

    {{ dbt_utils.generate_surrogate_key(["DATE(year, month, day)"]) }} AS date_id,
    {{ dbt_utils.generate_surrogate_key(["TIME(hour, minute, 0)"]) }} AS time_id,
    {{ dbt_utils.generate_surrogate_key(['borough']) }} AS location_id,
    {{ dbt_utils.generate_surrogate_key(['street_name', 'from_street', 'to_street', 'direction']) }} AS segment_id,

    ts.longitude,
    ts.latitude,
    ts.traffic_volume AS vol

FROM traffic_staging ts