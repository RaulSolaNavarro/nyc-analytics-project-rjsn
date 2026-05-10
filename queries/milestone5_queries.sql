-- ========================================================================
-- CIS 9440 - Group Project - Milestone 5 - Analytics Queries
-- Group 3: Zesen Chen, Bridgette Wang, Raúl J. Solá Navarro, Sung Ik Park
-- Project: Traffic Density & Road Infrastructure Degradation
-- BigQuery Project: raul-cis-9440-spring2026
-- ========================================================================


-- ============================================================
-- QUERY 1: Street-related 311 complaint volume by borough
-- Aggregation: by borough and problem type
-- Purpose: Identifies which boroughs generate the most
--          road/street infrastructure complaints. Serves as
--          the baseline for cross-mart comparison with traffic.
-- ============================================================

SELECT
    l.borough                                           AS borough,
    p.problem_type                                      AS problem_type,
    COUNT(*)                                            AS total_complaints
FROM `raul-cis-9440-spring2026.group_3_marts.fact_311service_request` f
INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_location` l
    ON f.location_id = l.location_key
INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_problem` p
    ON f.problem_id = p.problem_key
WHERE p.problem_type IN (
    'Street Condition',
    'Street Light Condition',
    'Traffic Signal Condition',
    'Highway Condition',
    'DEP Highway Condition'
)
GROUP BY l.borough, p.problem_type
ORDER BY total_complaints DESC;

-- ============================================================
-- QUERY 1 - CREATE TABLE: complaints_by_borough
-- Saves the borough-level complaint counts by problem type as
-- a permanent table in group_3_marts for direct use in
-- Data Studio without re-running the full query each time.
-- Powers the stacked bar chart and borough comparison visuals
-- in the dashboard.
-- ============================================================


CREATE TABLE IF NOT EXISTS `raul-cis-9440-spring2026.group_3_marts.complaints_by_borough`
AS
SELECT
    l.borough                   AS borough,
    p.problem_type              AS problem_type,
    COUNT(*)                    AS total_complaints
FROM `raul-cis-9440-spring2026.group_3_marts.fact_311service_request` f
INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_location` l
    ON f.location_id = l.location_key
INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_problem` p
    ON f.problem_id = p.problem_key
WHERE p.problem_type IN (
    'Street Condition','Street Light Condition',
    'Traffic Signal Condition','Highway Condition','DEP Highway Condition'
)
GROUP BY l.borough, p.problem_type;


-- ============================================================
-- QUERY 2: Average 311 response time (days to close) by borough
-- Aggregation: by borough
-- Purpose: Answers analytics Question 2: Does agency response
--          time vary by borough? Boroughs with higher traffic
--          may show longer repair lag times.
-- Note: Response time = difference in days between
--       created_date and closed_date using dim_date joins.
-- ============================================================

WITH created_dates AS (
    -- Get the created date for each 311 request
    SELECT
        f.unique_key                                    AS unique_key,
        f.location_id                                   AS location_id,
        f.problem_id                                    AS problem_id,
        f.additional_details                            AS status,
        d.full_date                                     AS created_date
    FROM `raul-cis-9440-spring2026.group_3_marts.fact_311service_request` f
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_date` d
        ON f.created_date_id = d.date_key
),

closed_dates AS (
    -- Get the closed date for each 311 request
    SELECT
        f.unique_key                                    AS unique_key,
        d.full_date                                     AS closed_date
    FROM `raul-cis-9440-spring2026.group_3_marts.fact_311service_request` f
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_date` d
        ON f.closed_date_id = d.date_key
)

SELECT
    l.borough                                           AS borough,
    COUNT(*)                                            AS total_closed_requests,
    ROUND(AVG(DATE_DIFF(cl.closed_date,
              cr.created_date, DAY)), 2)                AS avg_days_to_close,
    MIN(DATE_DIFF(cl.closed_date,
        cr.created_date, DAY))                          AS min_days_to_close,
    MAX(DATE_DIFF(cl.closed_date,
        cr.created_date, DAY))                          AS max_days_to_close
FROM created_dates cr
INNER JOIN closed_dates cl
    ON cr.unique_key = cl.unique_key
INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_location` l
    ON cr.location_id = l.location_key
INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_problem` p
    ON cr.problem_id = p.problem_key
WHERE p.problem_type IN (
    'Street Condition',
    'Street Light Condition',
    'Traffic Signal Condition',
    'Highway Condition',
    'DEP Highway Condition'
)
  AND cl.closed_date >= cr.created_date   -- exclude data anomalies where closed < created
GROUP BY l.borough
ORDER BY avg_days_to_close DESC;


-- ============================================================
-- QUERY 2 - CREATE TABLE: response_time_by_borough
-- Saves the average days-to-close by borough as a permanent
-- table in group_3_marts for direct use in Data Studio.
-- Powers the response time bar chart in the dashboard and
-- supports cross-mart comparison with traffic volume per
-- borough from Query 4.
-- ============================================================

CREATE TABLE IF NOT EXISTS `raul-cis-9440-spring2026.group_3_marts.response_time_by_borough`
AS
WITH created_dates AS (
    SELECT f.unique_key, f.location_id, f.problem_id, d.full_date AS created_date
    FROM `raul-cis-9440-spring2026.group_3_marts.fact_311service_request` f
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_date` d
        ON f.created_date_id = d.date_key
),
closed_dates AS (
    SELECT f.unique_key, d.full_date AS closed_date
    FROM `raul-cis-9440-spring2026.group_3_marts.fact_311service_request` f
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_date` d
        ON f.closed_date_id = d.date_key
)
SELECT
    l.borough                                           AS borough,
    COUNT(*)                                            AS total_closed_requests,
    ROUND(AVG(DATE_DIFF(cl.closed_date, cr.created_date, DAY)), 2) AS avg_days_to_close
FROM created_dates cr
INNER JOIN closed_dates cl ON cr.unique_key = cl.unique_key
INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_location` l
    ON cr.location_id = l.location_key
INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_problem` p
    ON cr.problem_id = p.problem_key
WHERE p.problem_type IN (
    'Street Condition','Street Light Condition',
    'Traffic Signal Condition','Highway Condition','DEP Highway Condition'
)
AND cl.closed_date >= cr.created_date
GROUP BY l.borough;


-- ============================================================
-- QUERY 3: Monthly 311 street complaint trends by year
-- Aggregation: by year and month
-- Purpose: Identifies seasonal patterns in street infrastructure
--          complaints over time. Used in cross-mart comparison
--          with monthly traffic volume (see Query 5).
-- ============================================================

SELECT
    d.year                                              AS complaint_year,
    d.month                                             AS complaint_month,
    COUNT(*)                                            AS total_complaints
FROM `raul-cis-9440-spring2026.group_3_marts.fact_311service_request` f
INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_date` d
    ON f.created_date_id = d.date_key
INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_problem` p
    ON f.problem_id = p.problem_key
WHERE p.problem_type IN (
    'Street Condition',
    'Street Light Condition',
    'Traffic Signal Condition',
    'Highway Condition',
    'DEP Highway Condition'
)
  AND d.year BETWEEN 2020 AND 2025
GROUP BY d.year, d.month
ORDER BY d.year, d.month;


-- ============================================================
-- QUERY 4: Average daily traffic volume by borough
-- Aggregation: by borough
-- Cross-mart: joins fact_traffic_volume + dim_location
-- Purpose: Establishes the traffic volume baseline per borough
--          to compare against complaint volumes and response
--          times in Queries 1 and 2.
-- ============================================================

WITH daily_traffic AS (
    -- Aggregate traffic volume to the day level first before averaging
    SELECT
        f.location_id                                   AS location_id,
        d.full_date                                     AS traffic_date,
        SUM(f.vol)                                      AS daily_volume
    FROM `raul-cis-9440-spring2026.group_3_marts.fact_traffic_volume` f
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_date` d
        ON f.date_id = d.date_key
    GROUP BY f.location_id, d.full_date
)

SELECT
    l.borough                                           AS borough,
    COUNT(DISTINCT dt.traffic_date)                     AS days_measured,
    ROUND(AVG(dt.daily_volume), 0)                      AS avg_daily_traffic_volume,
    ROUND(SUM(dt.daily_volume), 0)                      AS total_traffic_volume
FROM daily_traffic dt
INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_location` l
    ON dt.location_id = l.location_key
GROUP BY l.borough
ORDER BY avg_daily_traffic_volume DESC;


-- ============================================================
-- QUERY 5: Cross-mart: Monthly traffic volume vs.
--          311 street complaints by borough
-- Aggregation: by borough, year, and month
-- Cross-mart: combines fact_traffic_volume + fact_311service_request
-- Purpose: Core cross-mart analysis: Directly compares monthly
--          traffic volume with monthly complaint counts per
--          borough to evaluate the Peak-Load Service Lag KPI
--          and Question 1 from the project proposal.
-- ============================================================

WITH monthly_traffic AS (
    -- Total traffic volume per borough per month
    SELECT
        l.borough                                       AS borough,
        d.year                                          AS year,
        d.month                                         AS month,
        SUM(f.vol)                                      AS monthly_traffic_volume
    FROM `raul-cis-9440-spring2026.group_3_marts.fact_traffic_volume` f
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_date` d
        ON f.date_id = d.date_key
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_location` l
        ON f.location_id = l.location_key
    WHERE d.year BETWEEN 2020 AND 2025
    GROUP BY l.borough, d.year, d.month
),

monthly_complaints AS (
    -- Total street-related 311 complaints per borough per month
    SELECT
        l.borough                                       AS borough,
        d.year                                          AS year,
        d.month                                         AS month,
        COUNT(*)                                        AS monthly_complaints
    FROM `raul-cis-9440-spring2026.group_3_marts.fact_311service_request` f
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_date` d
        ON f.created_date_id = d.date_key
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_location` l
        ON f.location_id = l.location_key
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_problem` p
        ON f.problem_id = p.problem_key
    WHERE p.problem_type IN (
        'Street Condition',
        'Street Light Condition',
        'Traffic Signal Condition',
        'Highway Condition',
        'DEP Highway Condition'
    )
      AND d.year BETWEEN 2020 AND 2025
    GROUP BY l.borough, d.year, d.month
)

SELECT
    mt.borough                                          AS borough,
    mt.year                                             AS year,
    mt.month                                            AS month,
    mt.monthly_traffic_volume                           AS monthly_traffic_volume,
    mc.monthly_complaints                               AS monthly_street_complaints,
    -- Complaints per 1 million vehicle counts — normalizes for borough size
    ROUND(mc.monthly_complaints /
          NULLIF(mt.monthly_traffic_volume, 0) * 1000000, 4)
                                                        AS complaints_per_million_vehicles
FROM monthly_traffic mt
INNER JOIN monthly_complaints mc
    ON mt.borough = mc.borough
   AND mt.year    = mc.year
   AND mt.month   = mc.month
ORDER BY mt.borough, mt.year, mt.month;

-- ============================================================
-- QUERY 5 - CREATE TABLE: monthly_traffic_vs_complaints
-- Saves the cross-mart monthly traffic vs. complaints result
-- as a permanent table in group_3_marts for direct use in
-- DataStudio without re-running the full CTE each time.
-- ============================================================

-- Create table based on the above query to use directly in DataStudio
CREATE TABLE IF NOT EXISTS `raul-cis-9440-spring2026.group_3_marts.monthly_traffic_vs_complaints`
AS

WITH monthly_traffic AS (
    SELECT
        l.borough                                       AS borough,
        d.year                                          AS year,
        d.month                                         AS month,
        SUM(f.vol)                                      AS monthly_traffic_volume
    FROM `raul-cis-9440-spring2026.group_3_marts.fact_traffic_volume` f
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_date` d
        ON f.date_id = d.date_key
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_location` l
        ON f.location_id = l.location_key
    WHERE d.year BETWEEN 2020 AND 2025
    GROUP BY l.borough, d.year, d.month
),

monthly_complaints AS (
    SELECT
        l.borough                                       AS borough,
        d.year                                          AS year,
        d.month                                         AS month,
        COUNT(*)                                        AS monthly_complaints
    FROM `raul-cis-9440-spring2026.group_3_marts.fact_311service_request` f
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_date` d
        ON f.created_date_id = d.date_key
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_location` l
        ON f.location_id = l.location_key
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_problem` p
        ON f.problem_id = p.problem_key
    WHERE p.problem_type IN (
        'Street Condition',
        'Street Light Condition',
        'Traffic Signal Condition',
        'Highway Condition',
        'DEP Highway Condition'
    )
      AND d.year BETWEEN 2020 AND 2025
    GROUP BY l.borough, d.year, d.month
)

SELECT
    mt.borough                                          AS borough,
    mt.year                                             AS year,
    mt.month                                            AS month,
    mt.monthly_traffic_volume                           AS monthly_traffic_volume,
    mc.monthly_complaints                               AS monthly_street_complaints,
    ROUND(mc.monthly_complaints /
          NULLIF(mt.monthly_traffic_volume, 0) * 1000000, 4)
                                                        AS complaints_per_million_vehicles
FROM monthly_traffic mt
INNER JOIN monthly_complaints mc
    ON mt.borough = mc.borough
   AND mt.year    = mc.year
   AND mt.month   = mc.month
ORDER BY mt.borough, mt.year, mt.month;

-- ============================================================
-- QUERY 6: Street complaints by zip code (choropleth map data)
-- Aggregation: by zip code, borough, and problem type
-- Cross-mart: joins fact_311service_request +
--             dim_incident_location + dim_location + dim_problem
-- Purpose: Provides zip-code-level complaint counts to power
--          a choropleth map in Data Studio. Enables granular
--          geographic analysis of road infrastructure complaints
--          across NYC neighborhoods, going beyond borough-level
--          aggregation to identify specific high-complaint areas.
-- Note: Results saved as complaints_by_zip_code table in
--       group_3_marts for direct use in Data Studio.
-- ============================================================

CREATE OR REPLACE TABLE `raul-cis-9440-spring2026.group_3_marts.complaints_by_zip_code`
AS
SELECT
    il.zip_code                                         AS zip_code,
    l.borough                                           AS borough,
    p.problem_type                                      AS problem_type,
    COUNT(*)                                            AS total_complaints
FROM `raul-cis-9440-spring2026.group_3_marts.fact_311service_request` f
INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_incident_location` il
    ON f.incident_location_id = il.incident_location_key
INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_location` l
    ON f.location_id = l.location_key
INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_problem` p
    ON f.problem_id = p.problem_key
WHERE p.problem_type IN (
    'Street Condition',
    'Street Light Condition',
    'Traffic Signal Condition',
    'Highway Condition',
    'DEP Highway Condition'
)
  AND il.zip_code IS NOT NULL
  AND il.zip_code != '10000' -- not a real zip code
GROUP BY il.zip_code, l.borough, p.problem_type
ORDER BY total_complaints DESC;

-- ============================================================
-- QUERY 7: Combined complaints vs. traffic volume by borough
-- Aggregation: by borough
-- Cross-mart: combines fact_traffic_volume + fact_311service_request
-- Purpose: Creates a single borough-level table combining avg
--          daily traffic volume with total street complaints
--          to directly answer Question 1: Does traffic volume
--          correlate with complaint frequency? Powers the
--          dual-axis chart in the dashboard.
-- Note: Results saved as complaints_vs_traffic_by_borough in
--       group_3_marts for direct use in Data Studio.
-- ============================================================

CREATE TABLE IF NOT EXISTS `raul-cis-9440-spring2026.group_3_marts.complaints_vs_traffic_by_borough`
AS

WITH traffic AS (
    SELECT
        l.borough                                       AS borough,
        ROUND(AVG(daily.daily_vol), 0)                  AS avg_daily_traffic_volume
    FROM (
        SELECT
            f.location_id,
            d.full_date,
            SUM(f.vol)                                  AS daily_vol
        FROM `raul-cis-9440-spring2026.group_3_marts.fact_traffic_volume` f
        INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_date` d
            ON f.date_id = d.date_key
        GROUP BY f.location_id, d.full_date
    ) daily
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_location` l
        ON daily.location_id = l.location_key
    GROUP BY l.borough
),

complaints AS (
    SELECT
        l.borough                                       AS borough,
        COUNT(*)                                        AS total_complaints
    FROM `raul-cis-9440-spring2026.group_3_marts.fact_311service_request` f
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_location` l
        ON f.location_id = l.location_key
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_problem` p
        ON f.problem_id = p.problem_key
    WHERE p.problem_type IN (
        'Street Condition',
        'Street Light Condition',
        'Traffic Signal Condition',
        'Highway Condition',
        'DEP Highway Condition'
    )
    GROUP BY l.borough
)

SELECT
    t.borough                                           AS borough,
    t.avg_daily_traffic_volume                          AS avg_daily_traffic_volume,
    c.total_complaints                                  AS total_complaints
FROM traffic t
INNER JOIN complaints c
    ON t.borough = c.borough
ORDER BY t.avg_daily_traffic_volume DESC;

-- ============================================================
-- QUERY 8: Traffic-to-Repair Correlation (T2R) by borough
-- Aggregation: by borough
-- Cross-mart: combines complaints_vs_traffic_by_borough +
--             response_time_by_borough
-- Purpose: Directly addresses the T2R KPI from the project
--          proposal: Does higher traffic volume correlate with
--          slower repair times? 
--          Computes two derived metrics:
--          (1) traffic_per_repair_day: avg daily traffic volume
--          divided by avg days to close (a higher value means
--          more traffic is being absorbed per repair day);
--          (2) complaints_per_repair_day: total complaints
--          divided by avg days to close (measures throughput
--          of the repair pipeline per borough).
--          Results saved as traffic_to_repair_correlation in
--          group_3_marts for use in Data Studio dashboard.
-- ============================================================

CREATE TABLE IF NOT EXISTS `raul-cis-9440-spring2026.group_3_marts.traffic_to_repair_correlation`
AS

SELECT
    t.borough                                           AS borough,
    t.avg_daily_traffic_volume                          AS avg_daily_traffic_volume,
    t.total_complaints                                  AS total_complaints,
    r.avg_days_to_close                                 AS avg_days_to_close,
    r.total_closed_requests                             AS total_closed_requests,
    -- T2R metric: does higher traffic correlate with longer repair time?
    ROUND(t.avg_daily_traffic_volume / 
          NULLIF(r.avg_days_to_close, 0), 2)            AS traffic_per_repair_day,
    -- Complaint resolution rate: complaints per day to close
    ROUND(t.total_complaints / 
          NULLIF(r.avg_days_to_close, 0), 2)            AS complaints_per_repair_day
FROM `raul-cis-9440-spring2026.group_3_marts.complaints_vs_traffic_by_borough` t
INNER JOIN `raul-cis-9440-spring2026.group_3_marts.response_time_by_borough` r
    ON t.borough = r.borough
ORDER BY avg_daily_traffic_volume DESC;

-- ============================================================
-- QUERY 9: Peak-Load Service Lag KPI
-- Aggregation: by traffic period (Peak vs Non-Peak months)
-- Cross-mart: combines fact_traffic_volume + 
--             fact_311service_request + dim_date + dim_problem
-- Purpose: Directly addresses the Peak-Load Service Lag KPI
--          from the project proposal: Does avg time to close
--          311 street complaints increase during months with
--          above-average traffic volume? Peak months are defined
--          as months where total traffic volume exceeds the
--          overall monthly average across 2020-2025.
-- Finding: Response times are slightly faster during peak
--          traffic months (11.99 days) vs non-peak (12.90 days)
--          suggesting no significant maintenance lag during
--          high-traffic periods.
-- Note: Results saved as peak_load_service_lag in
--       group_3_marts for use in Data Studio dashboard.
-- ============================================================

CREATE TABLE IF NOT EXISTS `raul-cis-9440-spring2026.group_3_marts.peak_load_service_lag`
AS

WITH monthly_avg_traffic AS (
    SELECT
        d.year                                          AS year,
        d.month                                         AS month,
        SUM(f.vol)                                      AS monthly_traffic_volume
    FROM `raul-cis-9440-spring2026.group_3_marts.fact_traffic_volume` f
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_date` d
        ON f.date_id = d.date_key
    WHERE d.year BETWEEN 2020 AND 2025
    GROUP BY d.year, d.month
),

annual_avg_traffic AS (
    SELECT
        AVG(monthly_traffic_volume)                     AS avg_monthly_traffic
    FROM monthly_avg_traffic
),

peak_months AS (
    SELECT
        mt.year,
        mt.month,
        mt.monthly_traffic_volume,
        CASE
            WHEN mt.monthly_traffic_volume > aat.avg_monthly_traffic
            THEN 'Peak'
            ELSE 'Non-Peak'
        END                                             AS traffic_period
    FROM monthly_avg_traffic mt
    CROSS JOIN annual_avg_traffic aat
),

response_times AS (
    SELECT
        cd.year                                         AS year,
        cd.month                                        AS month,
        DATE_DIFF(cl.full_date, cd.full_date, DAY)      AS days_to_close
    FROM `raul-cis-9440-spring2026.group_3_marts.fact_311service_request` f
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_date` cd
        ON f.created_date_id = cd.date_key
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_date` cl
        ON f.closed_date_id = cl.date_key
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_problem` p
        ON f.problem_id = p.problem_key
    WHERE p.problem_type IN (
        'Street Condition',
        'Street Light Condition',
        'Traffic Signal Condition',
        'Highway Condition',
        'DEP Highway Condition'
    )
    AND cl.full_date >= cd.full_date
    AND cd.year BETWEEN 2020 AND 2025
)

SELECT
    pm.traffic_period                                   AS traffic_period,
    COUNT(*)                                            AS total_requests,
    ROUND(AVG(CAST(rt.days_to_close AS FLOAT64)), 2)   AS avg_days_to_close,
    MIN(rt.days_to_close)                               AS min_days_to_close,
    MAX(rt.days_to_close)                               AS max_days_to_close
FROM response_times rt
INNER JOIN peak_months pm
    ON rt.year  = pm.year
    AND rt.month = pm.month
GROUP BY pm.traffic_period
ORDER BY pm.traffic_period;



