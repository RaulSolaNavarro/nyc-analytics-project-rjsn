-- ========================================================================
-- CIS 9440 - Group Project - Milestone 5 - Analytics Queries
-- Group 3: 
--   > Zesen Chen
--   > Bridgette Wang
--   > Paven Oommen
--   > Sung Ik Park
--   > Raúl J. Solá Navarro
-- Project: Traffic Density & Road Infrastructure Degradation
-- BigQuery Project: raul-cis-9440-spring2026
-- ========================================================================

-- ========================================================================
-- ANALYSIS NOTES: Difficulties, Decisions & Deviations from Proposal
-- ========================================================================
--
-- 1. GEOGRAPHIC GRANULARITY (PIR scope adjustment):
--    The original proposal aimed to compute the Pothole Incidence Rate
--    at the street segment level by joining traffic counts to 311 complaints
--    on street name/location. This was not feasible because dim_location
--    only stores borough-level data. All KPIs were therefore computed at
--    the borough level instead of the segment level as originally proposed.
--
-- 2. DATA QUALITY (MISSING AND PLACEHOLDER ZIP CODES):
--    Of the 905,534 street-related complaints created from 2020 to 2025,
--    825,575 (91.2%) have a valid ZIP code, 78,254 (8.6%) have no ZIP
--    code (null), and 1,705 (0.2%) carry ZIP code 10000, which is not a
--    valid NYC ZIP code and
--    appears to be a placeholder for an unknown ZIP. Both the null and
--    the 10000 records are excluded from the geographic query and map
--    (see Query 6). An earlier count of 158,081 records (about 17%) for
--    ZIP 10000 was inflated by the duplicate-key join described in
--    note 3.
--
-- 3. DATA QUALITY (DUPLICATE KEYS IN dim_incident_location):
--    incident_location_key is not unique in dim_incident_location. The
--    same key appears on several rows (each copy carries the same ZIP
--    code). Joining the fact table directly to this dimension repeats
--    each complaint once per copy, which inflated the ZIP-level counts
--    above the true borough totals. Query 6 now collapses the dimension
--    to one row per key before joining. Reconciliation check: the ZIP
--    table must never exceed the borough totals from Query 1
--    (Staten Island 77,886; Queens 279,093; Bronx 141,874;
--    Manhattan 154,282; Brooklyn 252,399; total 905,534). The
--    underlying dimension should also get a uniqueness test in dbt.
--
-- 4. TRAFFIC DATASET GRAIN (segment-day):
--    fact_traffic_volume holds 15-minute counts per street segment and
--    direction, and the DOT only counts a few segments per borough on a
--    given day (about 2.0 to 2.6 on average). Summing vol across every
--    segment counted in a borough-day mixes "how busy is a street" with
--    "how many streets happened to be counted that day." All traffic
--    metrics now first sum to the segment-day level (one segment, one
--    day) and then average across segment-days. The result reads as
--    "average vehicles counted per street segment per day." The earlier
--    borough-day totals (roughly 28,000 vehicles for Manhattan) are
--    superseded by this metric. Borough averages rest on few counts
--    (178 segment-days for Staten Island, 639 Bronx, 739 Manhattan,
--    1,179 Queens, 1,454 Brooklyn), so the borough traffic ranking is
--    sensitive to which segments were counted and is indicative only.
--    Query 9 goes one step further. Monthly averages of raw segment-day
--    volume still swing with which segments were counted (for example,
--    31,046 in April 2023 vs 5,983 in May 2023), so peak months are
--    defined with a within-segment traffic index (each segment-day
--    divided by that segment's own average). Months with fewer than 30
--    segment-days of counts are excluded. Under this definition, average
--    days to close was 12.66 for peak months and 12.69 for non-peak
--    months, with a median of 1 day in both.
--
-- 5. REMOVED METRIC (complaints_per_million_vehicles):
--    The original Query 5 normalized monthly complaints by total counted
--    vehicles. Because coverage differs by month and borough (see note 4),
--    the denominator reflects counting effort and not traffic, which
--    produced extreme outliers. The metric was removed. Query 5 now
--    reports monthly complaints next to the average daily traffic per
--    segment, and any comparison should be read as descriptive only.
--
-- 6. T2R SCOPE ADJUSTMENT:
--    The original proposal defined T2R as a correlation coefficient
--    computed across zip codes. Due to the borough-level limitation
--    described above, T2R was computed at the borough level instead,
--    combining response_time_by_borough and complaints_vs_traffic_by_borough
--    tables (see Query 8). With five boroughs, it is a descriptive
--    comparison and not a statistical test.
--
-- 7. RESERVED KEYWORD ERROR (at):
--    During development of Query 9, the CTE alias 'at' caused a BigQuery
--    syntax error because AT is a reserved keyword. It was renamed to
--    'aat' (annual_avg_traffic) to resolve this.
--
-- 8. TABLE REFRESH (CREATE OR REPLACE):
--    CREATE TABLE IF NOT EXISTS does nothing when the table already
--    exists, so a corrected query would silently leave the old numbers
--    in place. Every table that is rebuilt now uses CREATE OR REPLACE
--    TABLE.
--
-- 9. SEASONAL CHART SOURCE (complaints_by_month):
--    monthly_traffic_vs_complaints (Query 5) only contains borough-months
--    that also have DOT traffic counts, so it understates complaints in
--    months without counts (for example, April to July 2020). The
--    seasonal chart in the dashboard therefore reads complaints_by_month
--    (Query 3 saved as a table), which has every month from 2020 to 2025.
--
-- 10. ANALYSIS WINDOW (2020 to 2025):
--    The 311 fact table also holds 38,861 street complaints created in
--    2026 (the data was pulled during 2026, so 2026 is a partial year).
--    The project scope, the traffic data window, and the report all
--    cover 2020 to 2025, so every query now filters the 311 created
--    date (and traffic date) to 2020 through 2025. This lowers the
--    street complaint total from 944,395 to 905,534 and changes the
--    borough traffic averages, because some 2026 traffic counts were
--    also removed. Notes 2, 3 and 4 use the filtered figures.
--
-- RUN ORDER (tables depend on each other):
--    Query 1 table, Query 2 table, Query 3 table, Query 6, Query 4,
--    Query 7, Query 5, Query 8, Query 9.
--    Query 8 reads the tables built by Queries 2 and 7, so run it after
--    them. Looker Studio sources must be refreshed after the rebuild.
--
-- ========================================================================

-- ============================================================
-- QUERY 1: Street-related 311 complaint volume by borough
-- Aggregation: by borough and problem type
-- Purpose: Identifies which boroughs generate the most
--          road/street infrastructure complaints. Serves as
--          the baseline for cross-mart comparison with traffic.
-- Note: Only complaints created from 2020 through 2025 are counted
--       (see Analysis Note 10).
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
INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_date` d
    ON f.created_date_id = d.date_key
WHERE p.problem_type IN (
    'Street Condition',
    'Street Light Condition',
    'Traffic Signal Condition',
    'Highway Condition',
    'DEP Highway Condition'
)
  AND d.year BETWEEN 2020 AND 2025
GROUP BY l.borough, p.problem_type
ORDER BY total_complaints DESC;

-- ============================================================
-- QUERY 1 - CREATE TABLE: complaints_by_borough
-- Saves the borough-level complaint counts by problem type as
-- a permanent table in group_3_marts for direct use in
-- Looker Studio without re-running the full query each time.
-- Powers the stacked bar chart and borough comparison visuals
-- in the dashboard.
-- ============================================================


CREATE OR REPLACE TABLE `raul-cis-9440-spring2026.group_3_marts.complaints_by_borough`
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
INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_date` d
    ON f.created_date_id = d.date_key
WHERE p.problem_type IN (
    'Street Condition','Street Light Condition',
    'Traffic Signal Condition','Highway Condition','DEP Highway Condition'
)
  AND d.year BETWEEN 2020 AND 2025
GROUP BY l.borough, p.problem_type;


-- ============================================================
-- QUERY 2: Average 311 response time (days to close) by borough
-- Aggregation: by borough
-- Purpose: Answers analytics Question 2: Does agency response
--          time vary by borough? Boroughs with higher traffic
--          may show longer repair lag times.
-- Note: Response time = difference in days between
--       created_date and closed_date using dim_date joins.
--       Only complaints created from 2020 through 2025 are counted
--       (see Analysis Note 10).
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
    WHERE d.year BETWEEN 2020 AND 2025
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
-- table in group_3_marts for direct use in Looker Studio.
-- Powers the response time bar chart in the dashboard and
-- supports cross-mart comparison with traffic volume per
-- borough from Query 4.
-- ============================================================

CREATE OR REPLACE TABLE `raul-cis-9440-spring2026.group_3_marts.response_time_by_borough`
AS
WITH created_dates AS (
    SELECT f.unique_key, f.location_id, f.problem_id, d.full_date AS created_date
    FROM `raul-cis-9440-spring2026.group_3_marts.fact_311service_request` f
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_date` d
        ON f.created_date_id = d.date_key
    WHERE d.year BETWEEN 2020 AND 2025
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
-- QUERY 3 - CREATE TABLE: complaints_by_month
-- Saves the monthly complaint counts as a permanent table in
-- group_3_marts for direct use in Looker Studio. Powers the
-- seasonal line chart on the Geographic & Temporal Patterns
-- page. Unlike monthly_traffic_vs_complaints (Query 5), this
-- table does not depend on traffic counts, so it includes every
-- month from 2020 to 2025.
-- ============================================================

CREATE OR REPLACE TABLE `raul-cis-9440-spring2026.group_3_marts.complaints_by_month`
AS
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
GROUP BY d.year, d.month;


-- ============================================================
-- QUERY 4: Average daily traffic volume per street segment,
--          by borough
-- Aggregation: by borough
-- Cross-mart: joins fact_traffic_volume + dim_date + dim_location
-- Purpose: Establishes the traffic volume baseline per borough
--          to compare against complaint volumes and response
--          times in Queries 1 and 2.
-- Note: Grain is the segment-day. Each row in the fact table is a
--       15-minute count for one segment and direction, so volumes
--       are first summed to one value per segment per day, then
--       averaged by borough. This avoids rewarding boroughs that
--       simply had more segments counted on a given day
--       (see Analysis Note 4).
-- ============================================================

WITH segment_day AS (
    -- One row per street segment per day (all 15-minute counts and
    -- directions summed)
    SELECT
        f.location_id                                   AS location_id,
        f.segment_id                                    AS segment_id,
        d.full_date                                     AS traffic_date,
        SUM(f.vol)                                      AS daily_vol
    FROM `raul-cis-9440-spring2026.group_3_marts.fact_traffic_volume` f
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_date` d
        ON f.date_id = d.date_key
    WHERE d.year BETWEEN 2020 AND 2025
    GROUP BY f.location_id, f.segment_id, d.full_date
)

SELECT
    l.borough                                           AS borough,
    COUNT(DISTINCT sd.traffic_date)                     AS days_measured,
    COUNT(*)                                            AS segment_days_measured,
    ROUND(AVG(sd.daily_vol), 0)                         AS avg_daily_traffic_volume
FROM segment_day sd
INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_location` l
    ON sd.location_id = l.location_key
GROUP BY l.borough
ORDER BY avg_daily_traffic_volume DESC;


-- ============================================================
-- QUERY 5 - Cross-mart: Monthly average daily traffic per segment
--          vs. 311 street complaints by borough
-- Aggregation: by borough, year, and month
-- Cross-mart: combines fact_traffic_volume + fact_311service_request
-- Purpose: Core cross-mart analysis: Directly compares monthly
--          traffic levels with monthly complaint counts per
--          borough to evaluate the Peak-Load Service Lag KPI
--          and Question 1 from the project proposal.
-- Note: Traffic is the monthly average of segment-day volumes
--       (see Analysis Note 4), not a sum across all segments
--       counted. The complaints_per_million_vehicles metric was
--       removed (see Analysis Note 5). Borough-months without
--       traffic counts do not appear in the result.
-- ============================================================

WITH segment_day AS (
    SELECT
        f.location_id                                   AS location_id,
        f.segment_id                                    AS segment_id,
        f.date_id                                       AS date_id,
        SUM(f.vol)                                      AS daily_vol
    FROM `raul-cis-9440-spring2026.group_3_marts.fact_traffic_volume` f
    GROUP BY f.location_id, f.segment_id, f.date_id
),

monthly_traffic AS (
    -- Average daily traffic per segment, per borough per month
    SELECT
        l.borough                                       AS borough,
        d.year                                          AS year,
        d.month                                         AS month,
        ROUND(AVG(sd.daily_vol), 0)                     AS avg_daily_traffic_per_segment
    FROM segment_day sd
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_date` d
        ON sd.date_id = d.date_key
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_location` l
        ON sd.location_id = l.location_key
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
    mt.avg_daily_traffic_per_segment                    AS avg_daily_traffic_per_segment,
    mc.monthly_complaints                               AS monthly_street_complaints
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
-- Looker Studio without re-running the full CTE each time.
-- Column monthly_traffic_volume was replaced by
-- avg_daily_traffic_per_segment, and complaints_per_million_vehicles
-- was dropped, so Looker Studio fields must be remapped after
-- the refresh.
-- ============================================================

CREATE OR REPLACE TABLE `raul-cis-9440-spring2026.group_3_marts.monthly_traffic_vs_complaints`
AS

WITH segment_day AS (
    SELECT
        f.location_id                                   AS location_id,
        f.segment_id                                    AS segment_id,
        f.date_id                                       AS date_id,
        SUM(f.vol)                                      AS daily_vol
    FROM `raul-cis-9440-spring2026.group_3_marts.fact_traffic_volume` f
    GROUP BY f.location_id, f.segment_id, f.date_id
),

monthly_traffic AS (
    SELECT
        l.borough                                       AS borough,
        d.year                                          AS year,
        d.month                                         AS month,
        ROUND(AVG(sd.daily_vol), 0)                     AS avg_daily_traffic_per_segment
    FROM segment_day sd
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_date` d
        ON sd.date_id = d.date_key
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_location` l
        ON sd.location_id = l.location_key
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
    mt.avg_daily_traffic_per_segment                    AS avg_daily_traffic_per_segment,
    mc.monthly_complaints                               AS monthly_street_complaints
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
--          a choropleth map in Looker Studio. Enables granular
--          geographic analysis of road infrastructure complaints
--          across NYC neighborhoods, going beyond borough-level
--          aggregation to identify specific high-complaint areas.
-- Note: Results saved as complaints_by_zip_code table in
--       group_3_marts for direct use in Looker Studio.
--       dim_incident_location contains duplicate
--       incident_location_key values, so it is collapsed to one
--       row per key before joining. Without this step each
--       complaint was counted once per duplicate row and the ZIP
--       totals exceeded the borough totals (see Analysis Note 3).
-- ============================================================

CREATE OR REPLACE TABLE `raul-cis-9440-spring2026.group_3_marts.complaints_by_zip_code`
AS
SELECT
    il.zip_code                                         AS zip_code,
    l.borough                                           AS borough,
    p.problem_type                                      AS problem_type,
    COUNT(*)                                            AS total_complaints
FROM `raul-cis-9440-spring2026.group_3_marts.fact_311service_request` f
INNER JOIN (
    -- dim_incident_location has duplicate keys (same ZIP on every copy),
    -- so collapse to one row per key before joining
    SELECT
        incident_location_key,
        ANY_VALUE(zip_code)                             AS zip_code
    FROM `raul-cis-9440-spring2026.group_3_marts.dim_incident_location`
    GROUP BY incident_location_key
) il
    ON f.incident_location_id = il.incident_location_key
INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_location` l
    ON f.location_id = l.location_key
INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_problem` p
    ON f.problem_id = p.problem_key
INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_date` d
    ON f.created_date_id = d.date_key
WHERE p.problem_type IN (
    'Street Condition',
    'Street Light Condition',
    'Traffic Signal Condition',
    'Highway Condition',
    'DEP Highway Condition'
)
  AND d.year BETWEEN 2020 AND 2025
  AND il.zip_code IS NOT NULL
  AND il.zip_code != '10000' -- not a real zip code
GROUP BY il.zip_code, l.borough, p.problem_type
ORDER BY total_complaints DESC;

-- ============================================================
-- QUERY 7: Combined complaints vs. traffic volume by borough
-- Aggregation: by borough
-- Cross-mart: combines fact_traffic_volume + fact_311service_request
-- Purpose: Creates a single borough-level table combining avg
--          daily traffic per street segment with total street
--          complaints to directly answer Question 1: Does traffic
--          volume correlate with complaint frequency? Powers the
--          dual-axis chart in the dashboard.
-- Note: Results saved as complaints_vs_traffic_by_borough in
--       group_3_marts for direct use in Looker Studio. Traffic
--       uses the segment-day grain (see Analysis Note 4), so it
--       matches Query 4.
-- ============================================================

CREATE OR REPLACE TABLE `raul-cis-9440-spring2026.group_3_marts.complaints_vs_traffic_by_borough`
AS

WITH segment_day AS (
    -- One row per street segment per day
    SELECT
        f.location_id                                   AS location_id,
        f.segment_id                                    AS segment_id,
        f.date_id                                       AS date_id,
        SUM(f.vol)                                      AS daily_vol
    FROM `raul-cis-9440-spring2026.group_3_marts.fact_traffic_volume` f
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_date` d
        ON f.date_id = d.date_key
    WHERE d.year BETWEEN 2020 AND 2025
    GROUP BY f.location_id, f.segment_id, f.date_id
),

traffic AS (
    SELECT
        l.borough                                       AS borough,
        ROUND(AVG(sd.daily_vol), 0)                     AS avg_daily_traffic_volume
    FROM segment_day sd
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_location` l
        ON sd.location_id = l.location_key
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
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_date` d
        ON f.created_date_id = d.date_key
    WHERE p.problem_type IN (
        'Street Condition',
        'Street Light Condition',
        'Traffic Signal Condition',
        'Highway Condition',
        'DEP Highway Condition'
    )
      AND d.year BETWEEN 2020 AND 2025
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
--          (1) traffic_per_repair_day: avg daily traffic per
--          segment divided by avg days to close (a higher value
--          means more traffic is being absorbed per repair day);
--          (2) complaints_per_repair_day: total complaints
--          divided by avg days to close (measures throughput
--          of the repair pipeline per borough).
--          Results saved as traffic_to_repair_correlation in
--          group_3_marts for use in Looker Studio dashboard.
-- Note: Run after Queries 2 and 7, since it reads the tables they
--       build. With five boroughs this is a descriptive
--       comparison, not a statistical test.
-- ============================================================

CREATE OR REPLACE TABLE `raul-cis-9440-spring2026.group_3_marts.traffic_to_repair_correlation`
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
--          above-average traffic volume? Each street segment is
--          compared with its own average daily volume (a traffic
--          index, where 1.0 means a typical day for that segment).
--          A month is Peak when its average index exceeds the mean
--          index across all qualifying months in 2020-2025.
-- Note: Raw citywide volume was not used to define peak months.
--       The DOT counts only a few segments per month, so raw
--       volume mostly reflects which segments were counted (for
--       example, 5,983 vehicles in May 2023 vs 31,046 in April
--       2023). The segment-relative index removes that effect.
--       Months with fewer than 30 segment-days of counts are
--       excluded because one segment can flip their label.
--       The peak classification is citywide, so every borough's
--       complaints in a given month share one label.
--       Results saved as peak_load_service_lag in group_3_marts
--       for use in Looker Studio dashboard.
-- Finding: Under the raw-volume definition, peak months averaged
--          13.97 days to close vs 11.40 for non-peak months, but
--          that split was driven by which segments the DOT counted.
--          Under the segment-index definition, peak months average
--          12.66 days vs 12.69 for non-peak months (median 1 day
--          in both). No difference was observed. No significance
--          test was run and traffic counts are sparse, so this
--          shows no evidence of a peak-load lag, not proof that
--          none exists.
-- ============================================================

CREATE OR REPLACE TABLE `raul-cis-9440-spring2026.group_3_marts.peak_load_service_lag`
AS

WITH segment_day AS (
    SELECT f.segment_id, f.date_id, SUM(f.vol) AS daily_vol
    FROM `raul-cis-9440-spring2026.group_3_marts.fact_traffic_volume` f
    GROUP BY f.segment_id, f.date_id
),

segment_avg AS (
    -- each segment's own typical daily volume
    SELECT segment_id, AVG(daily_vol) AS seg_avg
    FROM segment_day
    GROUP BY segment_id
),

monthly_index AS (
    -- 1.0 = the counted segments were at their own average; above 1.0 = busier than usual
    SELECT
        d.year                                          AS year,
        d.month                                         AS month,
        AVG(sd.daily_vol / NULLIF(sa.seg_avg, 0))       AS traffic_index,
        COUNT(*)                                        AS segment_days
    FROM segment_day sd
    INNER JOIN segment_avg sa
        ON sd.segment_id = sa.segment_id
    INNER JOIN `raul-cis-9440-spring2026.group_3_marts.dim_date` d
        ON sd.date_id = d.date_key
    WHERE d.year BETWEEN 2020 AND 2025
    GROUP BY d.year, d.month
    HAVING COUNT(*) >= 30   -- drop months with too few counts to trust
),

peak_months AS (
    SELECT
        year,
        month,
        CASE
            WHEN traffic_index > (SELECT AVG(traffic_index) FROM monthly_index)
            THEN 'Peak'
            ELSE 'Non-Peak'
        END                                             AS traffic_period
    FROM monthly_index
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
        'Street Condition','Street Light Condition',
        'Traffic Signal Condition','Highway Condition','DEP Highway Condition'
    )
    AND cl.full_date >= cd.full_date
    AND cd.year BETWEEN 2020 AND 2025
)

SELECT
    pm.traffic_period                                   AS traffic_period,
    COUNT(*)                                            AS total_requests,
    ROUND(AVG(CAST(rt.days_to_close AS FLOAT64)), 2)   AS avg_days_to_close,
    APPROX_QUANTILES(rt.days_to_close, 100)[OFFSET(50)] AS median_days_to_close,
    MIN(rt.days_to_close)                               AS min_days_to_close,
    MAX(rt.days_to_close)                               AS max_days_to_close
FROM response_times rt
INNER JOIN peak_months pm
    ON rt.year = pm.year AND rt.month = pm.month
GROUP BY pm.traffic_period
ORDER BY pm.traffic_period;