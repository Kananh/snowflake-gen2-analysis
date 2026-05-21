
-- ============================================================================
-- CONFIGURATION
-- ============================================================================
-- Set these variables before running

SET cutover_date = '2026-05-01';  -- UPDATE THIS DATE: first full UTC day of Gen2
SET warehouse_like = 'ANALYTICS_%';     -- UPDATE THIS PATTERN: your warehouse name prefix
-- SET warehouse_like = 'ALL';     -- Use this to include all warehouses
SET LOOKBACK_DAYS = 30;              -- Number of days to include in GEN1 period (default 90)

USE ROLE sysadmin;
USE DATABASE my_database;
USE SCHEMA sandbox;

-- ============================================================================
-- OPTION 1: FULL REFRESH (First run or complete rebuild)
-- ============================================================================
-- Use this for first-time creation or if additional columns for Gen1 period are needed
-- Drops and recreates entire table
--
-- ============================================================================
-- STAGING TABLE 1: QUERY HISTORY (Pre-filtered) - FULL REFRESH
-- ============================================================================

CREATE OR REPLACE TRANSIENT TABLE GEN2_STAGING_QUERY_HISTORY AS
SELECT
    session_id,
    query_id,
    query_text,
    query_parameterized_hash,
    CONVERT_TIMEZONE('UTC', start_time) AS start_time,
    CONVERT_TIMEZONE('UTC', end_time) AS end_time,
    CONVERT_TIMEZONE('UTC', current_timestamp()) as last_inserted_at,
    execution_time,
    warehouse_name,
    warehouse_size,
    execution_status,
    user_name,
    role_name,
    database_name,
    schema_name,
    query_type,
    total_elapsed_time,
    bytes_scanned,
    bytes_written,
    rows_produced,
    -- Time dimensions derived from UTC
    TO_DATE(CONVERT_TIMEZONE('UTC', start_time)) AS activity_date,
    DAYOFWEEKISO(CONVERT_TIMEZONE('UTC', start_time)) AS day_num,
    DAYNAME(CONVERT_TIMEZONE('UTC', start_time)) AS day_of_week,
    YEAROFWEEKISO(CONVERT_TIMEZONE('UTC', start_time)) AS year_iso,
    WEEKISO(CONVERT_TIMEZONE('UTC', start_time)) AS week_iso,
    DATE_TRUNC('week', CONVERT_TIMEZONE('UTC', start_time)) AS week_start_date,
    -- Queue time columns (milliseconds) — for detecting concurrency/provisioning issues
    queued_overload_time,       -- waiting because warehouse at max concurrency
    queued_provisioning_time,   -- waiting for warehouse to resume/provision
    queued_repair_time,         -- waiting for failed node repair
    -- extra columns
    error_code,
    error_message
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE warehouse_name LIKE (CASE WHEN $warehouse_like = 'ALL' THEN '%' ELSE $warehouse_like END)
    AND warehouse_name IS NOT NULL
    -- Keep raw start_time (TIMESTAMP_LTZ) for correct partition pruning
    -- to have clean weekly partitions, we load all data from the start of the week containing the cutover date
    AND start_time >= DATE_TRUNC('week', DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date)))
    AND start_time < date_trunc('day', convert_timezone('UTC', CURRENT_TIMESTAMP()))
ORDER BY activity_date ASC, query_parameterized_hash ASC
    ;

-- ============================================================================
-- STAGING TABLE 2: WAREHOUSE METERING HISTORY (Pre-filtered) - FULL REFRESH
-- ============================================================================

CREATE OR REPLACE TRANSIENT TABLE GEN2_STAGING_WAREHOUSE_METERING AS
SELECT
    CONVERT_TIMEZONE('UTC', start_time) AS start_time,
    CONVERT_TIMEZONE('UTC', end_time) AS end_time,
    CONVERT_TIMEZONE('UTC', current_timestamp()) as last_inserted_at,
    warehouse_id,
    warehouse_name,
    credits_used,
    credits_used_compute,
    credits_used_cloud_services,
    -- Time dimensions derived from UTC
    TO_DATE(CONVERT_TIMEZONE('UTC', start_time)) AS activity_date,
    YEAROFWEEKISO(CONVERT_TIMEZONE('UTC', start_time)) AS year_iso,
    WEEKISO(CONVERT_TIMEZONE('UTC', start_time)) AS week_iso,
    DAYOFWEEKISO(CONVERT_TIMEZONE('UTC', start_time)) AS day_num,
    DAYNAME(CONVERT_TIMEZONE('UTC', start_time)) AS day_of_week,
    DATE_TRUNC('week', CONVERT_TIMEZONE('UTC', start_time)) AS week_start_date
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE warehouse_name LIKE (CASE WHEN $warehouse_like = 'ALL' THEN '%' ELSE $warehouse_like END)
    AND warehouse_name IS NOT NULL
    -- Keep raw start_time (TIMESTAMP_LTZ) for correct partition pruning
    AND start_time >= DATE_TRUNC('week', DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date)))
    AND start_time < date_trunc('day', convert_timezone('UTC', CURRENT_TIMESTAMP()))
ORDER BY activity_date ASC
    ;



-- ============================================================================
-- OPTION 2: INCREMENTAL APPEND (Daily refresh - recommended for production)
-- ============================================================================
-- Use this for daily updates to append only new records
-- Much faster than full refresh (only processes new data since last run)
-- Skips GEN1 period data (already loaded in initial run)

-- ============================================================================
-- INCREMENTAL APPEND: QUERY HISTORY
-- ============================================================================

-- Disable caching to ensure we get the latest data from ACCOUNT_USAGE
ALTER SESSION SET USE_CACHED_RESULT = FALSE;
/* Get the latest timestamp currently in staging table */
set last_query_ts = (
    SELECT COALESCE(MAX(start_time), TO_TIMESTAMP_NTZ('1970-01-01'))
    FROM GEN2_STAGING_QUERY_HISTORY
);

/* Insert only new records since last update */
INSERT INTO GEN2_STAGING_QUERY_HISTORY
SELECT
    session_id,
    query_id,
    query_text,
    query_parameterized_hash,
    CONVERT_TIMEZONE('UTC', start_time) AS start_time,
    CONVERT_TIMEZONE('UTC', end_time) AS end_time,
    CONVERT_TIMEZONE('UTC', current_timestamp()) as last_inserted_at,
    execution_time,
    warehouse_name,
    warehouse_size,
    execution_status,
    user_name,
    role_name,
    database_name,
    schema_name,
    query_type,
    total_elapsed_time,
    bytes_scanned,
    bytes_written,
    rows_produced,
    -- Time dimensions derived from UTC
    TO_DATE(CONVERT_TIMEZONE('UTC', start_time)) AS activity_date,
    DAYOFWEEKISO(CONVERT_TIMEZONE('UTC', start_time)) AS day_num,
    DAYNAME(CONVERT_TIMEZONE('UTC', start_time)) AS day_of_week,
    YEAROFWEEKISO(CONVERT_TIMEZONE('UTC', start_time)) AS year_iso,
    WEEKISO(CONVERT_TIMEZONE('UTC', start_time)) AS week_iso,
    DATE_TRUNC('week', CONVERT_TIMEZONE('UTC', start_time)) AS week_start_date,
    queued_overload_time,
    queued_provisioning_time,
    queued_repair_time,
    error_code,
    error_message
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE warehouse_name LIKE (CASE WHEN $warehouse_like = 'ALL' THEN '%' ELSE $warehouse_like END)
    AND start_time > $last_query_ts  -- Only new records
    AND start_time < date_trunc('day', convert_timezone('UTC', CURRENT_TIMESTAMP()))
    AND warehouse_name IS NOT NULL
ORDER BY activity_date ASC, query_parameterized_hash ASC;

-- ============================================================================
-- INCREMENTAL APPEND: WAREHOUSE METERING
-- ============================================================================

/* Get the latest timestamp currently in staging table */
set last_metering_ts = (
    SELECT COALESCE(MAX(start_time), TO_TIMESTAMP_NTZ('1970-01-01'))
    FROM GEN2_STAGING_WAREHOUSE_METERING
);

-- Delete records for latest hour because metering data can be delayed and updated
-- for recent hours, unlike query history which is final on insert.
-- This ensures we don't have duplicates or partial data for the most recent hour.
DELETE FROM GEN2_STAGING_WAREHOUSE_METERING
   WHERE start_time >= $last_metering_ts;

/* Insert only new records since last update */
INSERT INTO GEN2_STAGING_WAREHOUSE_METERING
SELECT
    CONVERT_TIMEZONE('UTC', start_time) AS start_time,
    CONVERT_TIMEZONE('UTC', end_time) AS end_time,
    CONVERT_TIMEZONE('UTC', current_timestamp()) as last_inserted_at,
    warehouse_id,
    warehouse_name,
    credits_used,
    credits_used_compute,
    credits_used_cloud_services,
    -- Time dimensions derived from UTC
    TO_DATE(CONVERT_TIMEZONE('UTC', start_time)) AS activity_date,
    YEAROFWEEKISO(CONVERT_TIMEZONE('UTC', start_time)) AS year_iso,
    WEEKISO(CONVERT_TIMEZONE('UTC', start_time)) AS week_iso,
    DAYOFWEEKISO(CONVERT_TIMEZONE('UTC', start_time)) AS day_num,
    DAYNAME(CONVERT_TIMEZONE('UTC', start_time)) AS day_of_week,
    DATE_TRUNC('week', CONVERT_TIMEZONE('UTC', start_time)) AS week_start_date
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE warehouse_name LIKE (CASE WHEN $warehouse_like = 'ALL' THEN '%' ELSE $warehouse_like END)
    AND start_time >= $last_metering_ts  -- Only new records
    AND start_time < date_trunc('day', convert_timezone('UTC', CURRENT_TIMESTAMP()))
    AND warehouse_name IS NOT NULL
ORDER BY activity_date ASC;

-- Re-enable caching for downstream queries that don't require real-time data
ALTER SESSION SET USE_CACHED_RESULT = TRUE;


-- Validate coverage
SELECT
    'qh' AS source,
    CASE WHEN activity_date >= TO_DATE($cutover_date) THEN 'GEN2' ELSE 'GEN1' END AS period_type,
    COUNT(DISTINCT activity_date),
    MAX(activity_date)
FROM gen2_staging_query_history q
GROUP BY 2
UNION ALL
SELECT
    'wh' AS source,
    CASE WHEN activity_date >= TO_DATE($cutover_date) THEN 'GEN2' ELSE 'GEN1' END AS period_type,
    COUNT(DISTINCT activity_date),
    MAX(activity_date)
FROM gen2_staging_warehouse_metering w
GROUP BY 2
ORDER BY 1, 2
;
