-- ============================================================================
-- GEN2 MIGRATION MONITORING: Consolidated Statistical Comparison
-- ============================================================================
--
-- Purpose:
--   Single-file monitoring suite for Gen1 vs Gen2 warehouse comparison.
--   Produces a statistically defensible assessment for stakeholder decision-making.
--
-- Statistical methodology:
--   - Welch's t-test (not z-test) for unequal variances and small Gen2 samples
--   - Day-of-week stratification to control for cyclical workload patterns
--   - Matched query patterns (parameterized_hash) for apples-to-apples performance
--   - Direct credit comparison (WAREHOUSE_METERING_HISTORY already reflects Gen2 rates)
--   - Confidence intervals on effect sizes for decision-making
--   - Minimum 14 Gen2 days required before reliable assessment
--
-- Prerequisites:
--   Run gen2_staging_tables.sql to create:
--     - GEN2_STAGING_QUERY_HISTORY
--     - GEN2_STAGING_WAREHOUSE_METERING
--
-- Usage:
--   SET cutover_date = '2026-05-01';
--   SET warehouse_like = 'ANALYTICS_%';
--   Then run this file top-to-bottom.
--   Final output: SELECT * FROM VW_GEN2_MIGRATION_DECISION;
--
-- ============================================================================


-- ============================================================================
-- CONFIGURATION
-- ============================================================================

SET cutover_date = '2026-05-01';  -- NOTE: Set this to the first full UTC day after Gen2 was enabled.
SET LOOKBACK_DAYS = 30;   -- GEN1 window: min 30 days for reliable variance estimation (4+ per DOW)
SET LOOKAHEAD_DAYS = 30;  -- Gen2 window: grows as you collect more data
SET warehouse_like = 'ANALYTICS_%';
-- SET warehouse_like = 'ANALYTICS_PROD_ETL_WH';
-- NOTE: No credit multiplier needed. WAREHOUSE_METERING_HISTORY already reports
-- actual Gen2 credits consumed (e.g., Gen2 XS on AWS = 1.35 credits/hr vs Gen1 = 1).
-- The Gen2 premium is baked into the metering data. Raw credits = dollar cost comparison.

USE ROLE sysadmin;
USE DATABASE my_database;
USE SCHEMA sandbox;


-- ============================================================================
-- METRIC GLOSSARY
-- ============================================================================
--
-- CORE CONCEPTS
-- -------------
-- Pattern query:  A query whose query_parameterized_hash exists in BOTH the Gen1
--                 and Gen2 periods (via INTERSECT). This ensures apples-to-apples
--                 comparison — new or retired queries are excluded.
-- Result cache:   Queries with bytes_scanned = 0 are served from Snowflake's result
--                 cache (execution_time=0, no compute). These are excluded from all
--                 performance and cost-per-query metrics to avoid skewing results.
--
-- PERFORMANCE METRICS (from VW_GEN2_PERF_DAILY → VW_GEN2_PERF_SUMMARY)
-- ---------------------------------------------------------------------
-- speedup_pct:            (1 - Gen2/Gen1) × 100. Positive = Gen2 faster.
--                         Derived from Welch's t-test comparing AVG of daily medians.
--                         Each day contributes one observation: MEDIAN(execution_time/1000)
--                         across all non-cached pattern queries on that day.
-- speedup_ci_lower_90:    Lower bound of the 90% confidence interval on speedup_pct.
-- speedup_ci_upper_90:    Upper bound. If both bounds > 0, Gen2 is faster with 90% confidence.
-- perf_significance:      SIGNIFICANT (p<0.05), SIGNIFICANT (p<0.10), NOT SIGNIFICANT,
--                         or INSUFFICIENT DF (< 5 degrees of freedom).
-- tail_latency_check:     Compares AVG of daily P99 execution times. Flags WARNING (>2×),
--                         CAUTION (>1.5×), IMPROVED (≤0.8×), or OK.
--
-- COST METRICS (from VW_GEN2_COST_DAILY → VW_GEN2_COST_WELCH_TEST)
-- -----------------------------------------------------------------
-- Credits come from WAREHOUSE_METERING_HISTORY (already reflects Gen2 rates).
-- No multiplier is applied. Raw credits = dollar cost comparison.
--
-- cost_change_pct (assessment input, all-query scope):
--     (Gen2 - Gen1) / Gen1 × 100. Negative = Gen2 cheaper.
--     AVG of daily (total_warehouse_credits / all_non_cached_queries).
--     Numerator and denominator have CONSISTENT scope (both cover all warehouse activity).
--     Equal weight per DAY. Used for Welch's t-test and assessment.
--
-- pattern_cost_change_pct (context only, pattern-query scope):
--     Same formula but denominator is ONLY pattern queries.
--     CAUTION: contaminated estimand — numerator includes all warehouse credits but
--     denominator only counts matched patterns. Unmatched workload shifts can inflate
--     or deflate this metric even when matched queries themselves are unchanged.
--     Provided for diagnostics, NOT used for assessment.
--
-- weighted variants (SUM/SUM across the period):
--     Equal weight per QUERY (not per day). Reflects actual blended cost on the invoice.
--     Sensitive to volume shifts — high-volume days dominate.
--     WHEN avg and weighted DIVERGE (>10pp): daily query volumes are unstable.
--     See cost_metric_consistency column.
--
-- breakeven_status:       Whether Gen2 per-query cost ≤ Gen1 despite higher credit rate.
--     COST LOWER:                 all-query cost change ≤ 0% (Gen2 cheaper per query)
--     COST NEUTRAL:               all-query cost change ≤ 5% (within noise)
--     COST HIGHER:                all-query cost change 5-15%
--     COST SIGNIFICANTLY HIGHER:  all-query cost change > 15%
--
-- RELIABILITY METRICS (from VW_GEN2_RELIABILITY)
-- -----------------------------------------------
-- gen1_success_rate:      SUCCESS queries / total queries in Gen1 period.
-- gen2_success_rate:      Same for Gen2.
-- success_rate_diff_pp:   Gen2 rate - Gen1 rate in percentage points. Negative = worse.
-- reliability_status:     Welch's t-test on daily success rates (one obs per day).
--                         This naturally handles within-day query clustering that inflates
--                         query-level z-tests. DEGRADED requires BOTH statistical significance
--                         (p<0.05) AND practical significance (≥0.5pp drop or ≥25% failure
--                         rate increase).
--
-- ASSESSMENT (VW_GEN2_MIGRATION_DECISION)
-- ----------------------------------------
-- assessment:     INSUFFICIENT DATA | EARLY POSITIVE/NEGATIVE | STRONGLY FAVORABLE |
--                 FAVORABLE | UNFAVORABLE | MIXED | INCONCLUSIVE
-- This framework presents data signals — it does not dictate decisions.
-- Stakeholders weigh business context that the framework cannot see.
--
-- Performance: PATTERN scope (speedup_pct) — apples-to-apples on recurring queries.
-- Cost: ALL-QUERY scope (cost_change_pct) — numerator and denominator are consistent.
-- Reliability: ALL queries — failures matter regardless of pattern.
--
-- Key thresholds (all use 90% CI bounds, not point estimates):
--   STRONGLY FAVORABLE:  cost CI upper ≤ 0% AND speedup CI lower ≥ 35%
--   FAVORABLE:           cost CI upper ≤ 0% AND speedup ≥ -20% (cost neutral)
--   FAVORABLE:           cost CI upper ≤ 5% AND speedup CI lower ≥ 26%
--   FAVORABLE:           cost CI upper ≤ 15% AND speedup CI lower ≥ 35%
--   FAVORABLE:           tail latency IMPROVED + cost CI upper ≤ 10% + speedup ≥ 0%
--   UNFAVORABLE:         cost CI lower > 15% with speedup < 20%, or reliability DEGRADED
--   MIXED:               break-even + perf degraded >20%, minor reliability, tail latency, queue saturation,
--                        high-spend (>50 credits/day + >5%), or mixed signals
--   INCONCLUSIVE:        either median < 1s + < 1k queries/day
--   INSUFFICIENT DATA:   < 7 Gen2 days, or 7-13 days with ambiguous signals
--
-- ============================================================================

-- ============================================================================
-- SECTION 1: DATA VALIDATION
-- ============================================================================
-- Sanity checks before running the analysis. Run this first.

CREATE OR REPLACE VIEW VW_GEN2_DATA_VALIDATION AS
WITH query_coverage AS (
    SELECT
        CASE WHEN activity_date >= TO_DATE($cutover_date) THEN 'GEN2' ELSE 'GEN1' END AS period_type,
        COUNT(DISTINCT activity_date) AS days_covered,
        COUNT(DISTINCT warehouse_name) AS warehouse_count,
        COUNT(*) AS total_queries,
        MIN(activity_date) AS first_date,
        MAX(activity_date) AS last_date,
        SUM(CASE WHEN execution_status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_queries
    FROM GEN2_STAGING_QUERY_HISTORY
    WHERE TRUE
    -- AND activity_date >=  DATE_TRUNC('week', DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date)))
    AND activity_date >= DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date))
    AND activity_date <= DATEADD(day, $LOOKAHEAD_DAYS, TO_DATE($cutover_date))
    AND warehouse_name LIKE $warehouse_like
    GROUP BY 1
),
credit_coverage AS (
    SELECT
        CASE WHEN activity_date >= TO_DATE($cutover_date) THEN 'GEN2' ELSE 'GEN1' END AS period_type,
        COUNT(DISTINCT activity_date) AS days_covered,
        COUNT(DISTINCT warehouse_name) AS warehouse_count,
        ROUND(SUM(credits_used), 2) AS total_credits
    FROM GEN2_STAGING_WAREHOUSE_METERING
    WHERE TRUE
    -- AND activity_date >=  DATE_TRUNC('week', DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date)))
    AND activity_date >= DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date))
    AND activity_date <= DATEADD(day, +$LOOKAHEAD_DAYS, TO_DATE($cutover_date))
    AND warehouse_name LIKE $warehouse_like
    GROUP BY 1
),
patterns_intersect AS (
        -- queries on Gen1
        SELECT query_parameterized_hash AS pattern_hash
        FROM GEN2_STAGING_QUERY_HISTORY
        WHERE query_parameterized_hash IS NOT NULL
            -- AND activity_date >=  DATE_TRUNC('week', DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date)))
             AND activity_date >= DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date))
            AND activity_date < TO_DATE($cutover_date)
            AND warehouse_name LIKE $warehouse_like
        GROUP BY 1

        INTERSECT

        -- queries on Gen2
        SELECT query_parameterized_hash AS pattern_hash
        FROM GEN2_STAGING_QUERY_HISTORY
        WHERE query_parameterized_hash IS NOT NULL
            AND activity_date >= TO_DATE($cutover_date)
            AND activity_date <= DATEADD(day, +$LOOKAHEAD_DAYS, TO_DATE($cutover_date))
            AND warehouse_name LIKE $warehouse_like
        GROUP BY 1
),
common_patterns AS (
    SELECT COUNT(DISTINCT pattern_hash) AS common_patterns FROM patterns_intersect
),
-- Coverage: what % of total queries are in matched patterns + cache hit rate
pattern_coverage AS (
    SELECT
        CASE WHEN activity_date >= TO_DATE($cutover_date) THEN 'GEN2' ELSE 'GEN1' END AS period_type,
        COUNT(*) AS total_queries,
        SUM(CASE
            WHEN query_parameterized_hash IN (SELECT pattern_hash FROM patterns_intersect)
            THEN 1 ELSE 0 END) AS pattern_queries,
        -- Cache hit rate: queries served from result cache (bytes_scanned=0)
        -- Used to detect asymmetric caching between Gen1/Gen2 that could bias comparisons
        SUM(CASE WHEN bytes_scanned = 0 THEN 1 ELSE 0 END) AS cached_queries
    FROM GEN2_STAGING_QUERY_HISTORY
    WHERE TRUE
    -- AND activity_date >=  DATE_TRUNC('week', DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date)))
     AND activity_date >= DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date))
    AND activity_date <= DATEADD(day, +$LOOKAHEAD_DAYS, TO_DATE($cutover_date))
    AND warehouse_name LIKE $warehouse_like
    GROUP BY 1
)
SELECT
    q.period_type,
    q.days_covered AS query_days,
    q.warehouse_count,
    q.total_queries,
    q.successful_queries,
    ROUND(q.successful_queries * 100.0 / NULLIF(q.total_queries, 0), 2) AS success_rate_pct,
    q.first_date,
    q.last_date,
    c.total_credits,
    cp.common_patterns,
    ROUND(pc.pattern_queries * 100.0 / NULLIF(pc.total_queries, 0), 1) AS pattern_coverage_pct,
    -- Cache hit rate diagnostic: detect asymmetric caching between Gen1/Gen2.
    -- If rates differ significantly, performance and cost-per-query comparisons may be confounded.
    pc.cached_queries,
    ROUND(pc.cached_queries * 100.0 / NULLIF(pc.total_queries, 0), 1) AS cache_hit_rate_pct,
    CASE
        WHEN q.period_type = 'GEN2' AND q.days_covered < 7 THEN 'CRITICAL: < 7 days Gen2 data'
        WHEN q.period_type = 'GEN2' AND q.days_covered < 14 THEN 'WARNING: < 14 days Gen2 - preliminary only'
        WHEN q.period_type = 'GEN2' AND q.days_covered >= 14 THEN 'OK: Sufficient for decision'
        WHEN q.period_type = 'GEN1' AND q.days_covered < 14
            THEN 'CRITICAL: < 14 days GEN1 - unreliable variance, cannot detect < 40% effects'
        WHEN q.period_type = 'GEN1' AND q.days_covered < 30
            THEN 'WARNING: < 30 days GEN1 - reduced power, only ~4 samples per DOW. Increase LOOKBACK_DAYS to 30+'
        WHEN q.period_type = 'GEN1' AND q.days_covered >= 30 THEN 'OK: Sufficient GEN1'
        ELSE 'OK'
    END AS data_sufficiency,
    CASE
        WHEN q.period_type = 'GEN2' AND q.days_covered < 7
            THEN 'Less than 7 days of Gen2 data collected. Statistical tests will be unreliable and no reliable assessment can be made. Wait for more data.'
        WHEN q.period_type = 'GEN2' AND q.days_covered < 14
            THEN 'Between 7-13 days of Gen2 data. Preliminary trends visible but Welch t-test has low power (wide confidence intervals). Decisions at this stage are tentative.'
        WHEN q.period_type = 'GEN2' AND q.days_covered >= 14
            THEN 'At least 14 days of Gen2 data. Each day-of-week has 2+ samples. Confidence intervals are narrow enough for a defensible assessment.'
        WHEN q.period_type = 'GEN1' AND q.days_covered < 14
            THEN 'Less than 14 days of GEN1. Variance estimates are unreliable (< 2 samples per DOW). The t-test cannot detect effects smaller than ~40%. Increase LOOKBACK_DAYS.'
        WHEN q.period_type = 'GEN1' AND q.days_covered < 30
            THEN 'Between 14-29 days of GEN1. Only ~2-4 samples per day-of-week. Welch test has reduced statistical power. Increase LOOKBACK_DAYS to 30+ for reliable DOW-stratified analysis.'
        WHEN q.period_type = 'GEN1' AND q.days_covered >= 30
            THEN 'At least 30 days of GEN1 (4+ samples per DOW). Variance is well-estimated and DOW-stratified Welch tests are reliable.'
        ELSE 'Data coverage is adequate for analysis.'
    END AS data_sufficiency_explanation
FROM query_coverage q
LEFT JOIN credit_coverage c ON q.period_type = c.period_type
CROSS JOIN common_patterns cp
LEFT JOIN pattern_coverage pc ON q.period_type = pc.period_type
ORDER BY q.period_type DESC;


-- ============================================================================
-- SECTION 2: PERFORMANCE ANALYSIS
-- ============================================================================
-- Matched-pattern daily comparison using Welch's t-test.
-- Only compares query patterns that exist in BOTH Gen1 and Gen2 periods.

-- Step 2a: Daily performance metrics for matched patterns
-- NOTE: Excludes queries with NULL parameterized_hash (DDL, SHOW, ad-hoc)
--       to ensure apples-to-apples comparison of recurring query patterns.
CREATE OR REPLACE VIEW VW_GEN2_PERF_DAILY AS
WITH patterns_intersect AS (
        -- queries on Gen1
        SELECT query_parameterized_hash AS pattern_hash
        FROM GEN2_STAGING_QUERY_HISTORY
        WHERE query_parameterized_hash IS NOT NULL
            -- AND activity_date >=  DATE_TRUNC('week', DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date)))
            AND activity_date >= DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date))
            AND activity_date < TO_DATE($cutover_date)
            AND warehouse_name LIKE $warehouse_like
        GROUP BY 1

        INTERSECT

        -- queries on Gen2
        SELECT query_parameterized_hash AS pattern_hash
        FROM GEN2_STAGING_QUERY_HISTORY
        WHERE query_parameterized_hash IS NOT NULL
            AND activity_date >= TO_DATE($cutover_date)
            AND activity_date <= DATEADD(day, +$LOOKAHEAD_DAYS, TO_DATE($cutover_date))
            AND warehouse_name LIKE $warehouse_like
        GROUP BY 1
),
-- Pass 1: compute P95 threshold per day (SUCCESS only — failed queries have unpredictable
-- execution times that contaminate tail analysis)
daily_p95 AS (
    SELECT
        q.activity_date,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY q.execution_time / 1000) AS p95_threshold
    FROM GEN2_STAGING_QUERY_HISTORY q
    INNER JOIN patterns_intersect pi ON q.query_parameterized_hash = pi.pattern_hash
    WHERE true
        -- AND q.activity_date >=  DATE_TRUNC('week', DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date)))
        AND q.activity_date >= DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date))
        AND q.activity_date <= DATEADD(day, +$LOOKAHEAD_DAYS, TO_DATE($cutover_date))
        AND q.warehouse_name LIKE $warehouse_like
        AND q.bytes_scanned > 0
        AND q.execution_status = 'SUCCESS'
    GROUP BY q.activity_date
),
-- Success rate per day (computed separately because performance metrics below are filtered to SUCCESS)
daily_success_rate AS (
    SELECT
        q.activity_date,
        COUNT(*) AS total_pattern_queries,
        SUM(CASE WHEN q.execution_status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_queries,
        ROUND(successful_queries * 100.0 / NULLIF(total_pattern_queries, 0), 2) AS success_rate_pct
    FROM GEN2_STAGING_QUERY_HISTORY q
    INNER JOIN patterns_intersect pi ON q.query_parameterized_hash = pi.pattern_hash
    WHERE true
        -- AND q.activity_date >=  DATE_TRUNC('week', DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date)))
        AND q.activity_date >= DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date))
        AND q.activity_date <= DATEADD(day, +$LOOKAHEAD_DAYS, TO_DATE($cutover_date))
        AND q.warehouse_name LIKE $warehouse_like
        AND q.bytes_scanned > 0
    GROUP BY q.activity_date
)
-- Pass 2: performance metrics from SUCCESSFUL queries only + tail weight
-- Failed queries excluded: they have unpredictable execution times (instant failures
-- deflate medians, timeouts inflate P95/P99). Failure analysis is handled separately
-- by VW_GEN2_RELIABILITY and VW_GEN2_ERROR_ANALYSIS.
SELECT
    q.activity_date,
    CASE WHEN q.activity_date >= TO_DATE($cutover_date) THEN 'GEN2' ELSE 'GEN1' END AS period_type,
    q.day_num AS day_of_week_num,
    q.day_of_week,
    COUNT(*) AS query_count,
    -- Median is robust to outliers within a day
    ROUND(MEDIAN(q.execution_time / 1000), 4) AS median_exec_sec,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY q.execution_time / 1000), 4) AS p95_exec_sec,
    ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY q.execution_time / 1000), 4) AS p99_exec_sec,
    -- Tail analysis: queries at or above the P95 execution time threshold.
    -- p95_tail_count: number of queries with execution_time >= the day's P95 value.
    --   NOT guaranteed to be exactly 5% of query_count due to ties at the threshold
    --   and PERCENTILE_CONT interpolation. Compare with query_count to judge if tail
    --   weight comes from 1 outlier (noise) or many slow queries (systemic).
    -- p95_tail_weight_pct: what % of total execution time those tail queries consume.
    --   Baseline: ~10-15% is normal for typical query distributions (top values are by
    --   definition the largest, so they always hold >5% of total time). >30% = genuinely
    --   tail-heavy, worth investigating with gen2_pattern_diagnostic.sql.
    -- NOTE: On low-volume days (<20 queries), both tail metrics are unreliable.
    SUM(CASE WHEN q.execution_time / 1000 >= dp.p95_threshold THEN 1 ELSE 0 END) AS p95_tail_count,
    ROUND(SUM(CASE WHEN q.execution_time / 1000 >= dp.p95_threshold
                   THEN q.execution_time / 1000 ELSE 0 END)
          * 100.0 / NULLIF(SUM(q.execution_time / 1000), 0), 1) AS p95_tail_weight_pct,
    -- NOTE: Queue time metrics are computed separately in VW_GEN2_PERF_SUMMARY from ALL queries
    -- (not just matched patterns), because queue saturation is an infrastructure metric that
    -- affects all queries equally regardless of pattern matching or cache status.
    -- Success rate joined from separate CTE (includes failed queries in denominator)
    dsr.successful_queries,
    dsr.success_rate_pct
FROM GEN2_STAGING_QUERY_HISTORY q
INNER JOIN patterns_intersect pi ON q.query_parameterized_hash = pi.pattern_hash
INNER JOIN daily_p95 dp ON q.activity_date = dp.activity_date
INNER JOIN daily_success_rate dsr ON q.activity_date = dsr.activity_date
WHERE true
    -- AND q.activity_date >=  DATE_TRUNC('week', DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date)))
    AND q.activity_date >= DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date))
    AND q.activity_date <= DATEADD(day, +$LOOKAHEAD_DAYS, TO_DATE($cutover_date))
    AND q.warehouse_name LIKE $warehouse_like
    AND q.bytes_scanned > 0  -- Exclude result-cached queries (execution_time=0, no compute)
    AND q.execution_status = 'SUCCESS'  -- Performance metrics from successful queries only
GROUP BY q.activity_date, 2, q.day_num, q.day_of_week, dsr.successful_queries, dsr.success_rate_pct;


-- Step 2b: Welch's t-test on daily median execution times (day-of-week stratified)
-- *** EXPLORATORY ONLY — NOT USED FOR ASSESSMENT ***
-- With ~5 observations per DOW per period, these tests have very low statistical power
-- (~15% for medium effects). They are provided for pattern exploration — e.g., spotting
-- days where Gen2 performs differently — but "NOT SIGNIFICANT" at this sample size does
-- NOT mean "no difference." Use the overall pooled test (VW_GEN2_PERF_SUMMARY) for
-- the assessment. Not corrected for multiple comparisons (7 tests).
CREATE OR REPLACE VIEW VW_GEN2_PERF_WELCH_TEST AS
WITH dow_stats AS (
    -- Per day-of-week statistics for each period
    SELECT
        day_of_week_num,
        day_of_week,
        period_type,
        COUNT(*) AS n,
        AVG(median_exec_sec) AS mean_val,
        VARIANCE(median_exec_sec) AS var_val
    FROM VW_GEN2_PERF_DAILY
    GROUP BY day_of_week_num, day_of_week, period_type
    HAVING COUNT(*) >= 4  -- Need at least 4 observations per group for meaningful variance
),
dow_comparison AS (
    SELECT
        b.day_of_week_num,
        b.day_of_week,
        -- Sample sizes
        b.n AS n_gen1,
        g.n AS n_gen2,
        -- Means
        b.mean_val AS mean_gen1,
        g.mean_val AS mean_gen2,
        -- Variances
        b.var_val AS var_gen1,
        g.var_val AS var_gen2,
        -- Welch's t-statistic
        (g.mean_val - b.mean_val) /
            NULLIF(SQRT(b.var_val / b.n + g.var_val / g.n), 0) AS t_stat,
        -- Welch-Satterthwaite degrees of freedom
        POWER(b.var_val / b.n + g.var_val / g.n, 2) /
            NULLIF(
                POWER(b.var_val / b.n, 2) / (b.n - 1) +
                POWER(g.var_val / g.n, 2) / (g.n - 1),
                0
            ) AS welch_df,
        -- Effect size: percentage speedup (positive = Gen2 faster)
        ROUND((1 - g.mean_val / NULLIF(b.mean_val, 0)) * 100, 2) AS speedup_pct,
        -- Standard error of the difference
        SQRT(b.var_val / b.n + g.var_val / g.n) AS se_diff
    FROM dow_stats b
    INNER JOIN dow_stats g
        ON b.day_of_week_num = g.day_of_week_num
        AND b.period_type = 'GEN1'
        AND g.period_type = 'GEN2'
)
SELECT
    'EXPLORATORY — low power, not used for assessment' AS test_scope,
    day_of_week_num,
    day_of_week,
    n_gen1,
    n_gen2,
    ROUND(mean_gen1, 4) AS gen1_median_exec_sec,
    ROUND(mean_gen2, 4) AS gen2_median_exec_sec,
    speedup_pct,
    ROUND(t_stat, 3) AS t_statistic,
    ROUND(welch_df, 1) AS degrees_of_freedom,
    -- t-critical values by df range (two-sided alpha=0.10 for 90% CI)
    CASE
        WHEN welch_df < 5 THEN NULL  -- Too few df
        WHEN welch_df < 7 THEN 2.015
        WHEN welch_df < 11 THEN 1.860
        WHEN welch_df < 16 THEN 1.796
        WHEN welch_df < 21 THEN 1.746
        WHEN welch_df < 31 THEN 1.711
        WHEN welch_df < 61 THEN 1.684
        ELSE 1.645
    END AS t_critical_90,
    -- Confidence interval on speedup (90% CI)
    ROUND(speedup_pct - (CASE
        WHEN welch_df < 5 THEN NULL
        WHEN welch_df < 7 THEN 2.015
        WHEN welch_df < 11 THEN 1.860
        WHEN welch_df < 16 THEN 1.796
        WHEN welch_df < 21 THEN 1.746
        WHEN welch_df < 31 THEN 1.711
        WHEN welch_df < 61 THEN 1.684
        ELSE 1.645
    END) * se_diff / NULLIF(mean_gen1, 0) * 100, 2) AS speedup_ci_lower_90,
    ROUND(speedup_pct + (CASE
        WHEN welch_df < 5 THEN NULL
        WHEN welch_df < 7 THEN 2.015
        WHEN welch_df < 11 THEN 1.860
        WHEN welch_df < 16 THEN 1.796
        WHEN welch_df < 21 THEN 1.746
        WHEN welch_df < 31 THEN 1.711
        WHEN welch_df < 61 THEN 1.684
        ELSE 1.645
    END) * se_diff / NULLIF(mean_gen1, 0) * 100, 2) AS speedup_ci_upper_90,
    -- Significance
    CASE
        WHEN welch_df < 5 THEN 'INSUFFICIENT DF'
        WHEN ABS(t_stat) > CASE
            WHEN welch_df < 7 THEN 2.571
            WHEN welch_df < 11 THEN 2.306
            WHEN welch_df < 16 THEN 2.201
            WHEN welch_df < 21 THEN 2.120
            WHEN welch_df < 31 THEN 2.064
            WHEN welch_df < 61 THEN 2.021
            ELSE 1.960
        END THEN 'SIGNIFICANT (p<0.05)'
        WHEN ABS(t_stat) > CASE
            WHEN welch_df < 7 THEN 2.015
            WHEN welch_df < 11 THEN 1.860
            WHEN welch_df < 16 THEN 1.796
            WHEN welch_df < 21 THEN 1.746
            WHEN welch_df < 31 THEN 1.711
            WHEN welch_df < 61 THEN 1.684
            ELSE 1.645
        END THEN 'SIGNIFICANT (p<0.10)'
        ELSE 'NOT SIGNIFICANT'
    END AS significance,
    CASE
        WHEN welch_df < 5
            THEN 'Degrees of freedom < 5. Too few data points in one or both periods to compute a reliable t-test. The t-distribution has extremely heavy tails at low df, making confidence intervals meaninglessly wide. Collect more days of data before drawing conclusions for this day-of-week.'
        WHEN ABS(t_stat) > CASE
            WHEN welch_df < 7 THEN 2.571 WHEN welch_df < 11 THEN 2.306
            WHEN welch_df < 16 THEN 2.201 WHEN welch_df < 21 THEN 2.120
            WHEN welch_df < 31 THEN 2.064 WHEN welch_df < 61 THEN 2.021 ELSE 1.960
        END THEN 'The performance difference on this day-of-week is statistically significant at the 5% level (p < 0.05). There is less than a 5% probability this difference is due to random variation alone. Check speedup_pct to see direction and magnitude.'
        WHEN ABS(t_stat) > CASE
            WHEN welch_df < 7 THEN 2.015 WHEN welch_df < 11 THEN 1.860
            WHEN welch_df < 16 THEN 1.796 WHEN welch_df < 21 THEN 1.746
            WHEN welch_df < 31 THEN 1.711 WHEN welch_df < 61 THEN 1.684 ELSE 1.645
        END THEN 'The performance difference is marginally significant (p < 0.10 but >= 0.05). Suggestive of a real effect but not conclusive. With more Gen2 data days, this may become significant at p < 0.05 or may disappear.'
        ELSE 'The observed performance difference on this day-of-week could plausibly be due to normal day-to-day random variation. Cannot conclude Gen2 is meaningfully different from Gen1 for this DOW.'
    END AS significance_explanation
FROM dow_comparison
ORDER BY day_of_week_num;


-- Step 2b-2: Performance breakdown by day-of-week (descriptive, mirrors VW_GEN2_COST_BY_DOW)
-- Uses true median/percentiles across all queries per DOW (not average of daily medians)
CREATE OR REPLACE VIEW VW_GEN2_PERF_BY_DOW AS
WITH patterns_intersect AS (
    SELECT query_parameterized_hash AS pattern_hash
    FROM GEN2_STAGING_QUERY_HISTORY
    WHERE query_parameterized_hash IS NOT NULL
        -- AND activity_date >=  DATE_TRUNC('week', DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date)))
        AND activity_date >= DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date))
        AND activity_date < TO_DATE($cutover_date)
        AND warehouse_name LIKE $warehouse_like
    GROUP BY 1

    INTERSECT

    SELECT query_parameterized_hash AS pattern_hash
    FROM GEN2_STAGING_QUERY_HISTORY
    WHERE query_parameterized_hash IS NOT NULL
        AND activity_date >= TO_DATE($cutover_date)
        AND activity_date <= DATEADD(day, +$LOOKAHEAD_DAYS, TO_DATE($cutover_date))
        AND warehouse_name LIKE $warehouse_like
    GROUP BY 1
),
dow_pattern AS (
    SELECT
        q.day_num AS day_of_week_num,
        q.day_of_week,
        CASE WHEN q.activity_date >= TO_DATE($cutover_date) THEN 'GEN2' ELSE 'GEN1' END AS period_type,
        COUNT(*) AS total_queries,
        COUNT(DISTINCT q.activity_date) AS distinct_days,
        ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT q.activity_date), 0), 0) AS avg_daily_query_count,
        ROUND(MEDIAN(q.execution_time / 1000), 4) AS median_exec_sec,
        ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY q.execution_time / 1000), 4) AS p95_exec_sec,
        ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY q.execution_time / 1000), 4) AS p99_exec_sec,
        SUM(CASE WHEN q.execution_status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_queries,
        ROUND(successful_queries * 100.0 / NULLIF(total_queries, 0), 2) AS success_rate_pct
    FROM GEN2_STAGING_QUERY_HISTORY q
    INNER JOIN patterns_intersect pi ON q.query_parameterized_hash = pi.pattern_hash
    WHERE true
        -- AND q.activity_date >=  DATE_TRUNC('week', DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date)))
        AND q.activity_date >= DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date))
        AND q.activity_date <= DATEADD(day, +$LOOKAHEAD_DAYS, TO_DATE($cutover_date))
        AND warehouse_name LIKE $warehouse_like
        AND q.bytes_scanned > 0  -- Exclude result-cached queries (execution_time=0, no compute)
        AND q.execution_status = 'SUCCESS'  -- Consistent with VW_GEN2_PERF_DAILY — failed queries have erratic execution times
    GROUP BY q.day_num, q.day_of_week, 3
),
dow_all AS (
    SELECT
        q.day_num AS day_of_week_num,
        CASE WHEN q.activity_date >= TO_DATE($cutover_date) THEN 'GEN2' ELSE 'GEN1' END AS period_type,
        COUNT(*) AS total_queries,
        ROUND(MEDIAN(q.execution_time / 1000), 4) AS median_exec_sec,
        ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY q.execution_time / 1000), 4) AS p95_exec_sec,
        ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY q.execution_time / 1000), 4) AS p99_exec_sec
    FROM GEN2_STAGING_QUERY_HISTORY q
    WHERE true
    -- and q.activity_date >=  DATE_TRUNC('week', DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date)))
        AND q.activity_date >= DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date))
        AND q.activity_date <= DATEADD(day, +$LOOKAHEAD_DAYS, TO_DATE($cutover_date))
        AND q.warehouse_name LIKE $warehouse_like
        AND q.bytes_scanned > 0  -- Exclude result-cached queries (execution_time=0, no compute)
    GROUP BY q.day_num, 2
)
SELECT
    b.day_of_week_num,
    b.day_of_week,
    b.distinct_days AS gen1_days,
    g.distinct_days AS gen2_days,
    b.avg_daily_query_count AS gen1_avg_daily_queries,
    g.avg_daily_query_count AS gen2_avg_daily_queries,
    ROUND((g.avg_daily_query_count - b.avg_daily_query_count) * 100.0
        / NULLIF(b.avg_daily_query_count, 0), 2) AS avg_daily_queries_change_pct,
    -- All queries: median execution time (positive speedup = Gen2 faster)
    ab.median_exec_sec AS all_gen1_median_exec_sec,
    ag.median_exec_sec AS all_gen2_median_exec_sec,
    ROUND((1 - ag.median_exec_sec / NULLIF(ab.median_exec_sec, 0)) * 100, 2) AS all_median_exec_speedup_pct,
    -- Matched patterns: median execution time (positive speedup = Gen2 faster)
    b.median_exec_sec AS pattern_gen1_median_exec_sec,
    g.median_exec_sec AS pattern_gen2_median_exec_sec,
    ROUND((1 - g.median_exec_sec / NULLIF(b.median_exec_sec, 0)) * 100, 2) AS pattern_median_exec_speedup_pct,
    -- All queries: P95 execution time (positive speedup = Gen2 faster)
    ab.p95_exec_sec AS all_gen1_p95_exec_sec,
    ag.p95_exec_sec AS all_gen2_p95_exec_sec,
    ROUND((1 - ag.p95_exec_sec / NULLIF(ab.p95_exec_sec, 0)) * 100, 2) AS all_p95_exec_speedup_pct,
    -- Matched patterns: P95 execution time (positive speedup = Gen2 faster)
    b.p95_exec_sec AS pattern_gen1_p95_exec_sec,
    g.p95_exec_sec AS pattern_gen2_p95_exec_sec,
    ROUND((1 - g.p95_exec_sec / NULLIF(b.p95_exec_sec, 0)) * 100, 2) AS pattern_p95_exec_speedup_pct,
    -- All queries: P99 execution time (positive speedup = Gen2 faster)
    ab.p99_exec_sec AS all_gen1_p99_exec_sec,
    ag.p99_exec_sec AS all_gen2_p99_exec_sec,
    ROUND((1 - ag.p99_exec_sec / NULLIF(ab.p99_exec_sec, 0)) * 100, 2) AS all_p99_exec_speedup_pct,
    -- Matched patterns: P99 execution time (positive speedup = Gen2 faster)
    b.p99_exec_sec AS pattern_gen1_p99_exec_sec,
    g.p99_exec_sec AS pattern_gen2_p99_exec_sec,
    ROUND((1 - g.p99_exec_sec / NULLIF(b.p99_exec_sec, 0)) * 100, 2) AS pattern_p99_exec_speedup_pct,
    -- Success rate (matched patterns)
    b.success_rate_pct AS gen1_success_rate_pct,
    g.success_rate_pct AS gen2_success_rate_pct,
    ROUND(g.success_rate_pct - b.success_rate_pct, 2) AS success_rate_diff_pp
FROM dow_pattern b
INNER JOIN dow_pattern g
    ON b.day_of_week_num = g.day_of_week_num
    AND b.period_type = 'GEN1'
    AND g.period_type = 'GEN2'
INNER JOIN dow_all ab
    ON b.day_of_week_num = ab.day_of_week_num
    AND ab.period_type = 'GEN1'
INNER JOIN dow_all ag
    ON b.day_of_week_num = ag.day_of_week_num
    AND ag.period_type = 'GEN2'
ORDER BY b.day_of_week_num;


-- Step 2c: Overall performance summary (pooled Welch's t-test across all days)
CREATE OR REPLACE VIEW VW_GEN2_PERF_SUMMARY AS
WITH overall AS (
    -- Pool all daily observations (not stratified) for the main test
    SELECT
        period_type,
        COUNT(*) AS n,
        AVG(median_exec_sec) AS mean_val,
        VARIANCE(median_exec_sec) AS var_val
    FROM VW_GEN2_PERF_DAILY
    GROUP BY period_type
),
comparison AS (
    SELECT
        b.n AS n_gen1,
        g.n AS n_gen2,
        b.mean_val AS mean_gen1,
        g.mean_val AS mean_gen2,
        b.var_val AS var_gen1,
        g.var_val AS var_gen2,
        -- Welch's t (on the difference, for significance testing)
        (g.mean_val - b.mean_val) /
            NULLIF(SQRT(b.var_val / b.n + g.var_val / g.n), 0) AS t_stat,
        -- Welch-Satterthwaite df
        POWER(b.var_val / b.n + g.var_val / g.n, 2) /
            NULLIF(
                POWER(b.var_val / b.n, 2) / (b.n - 1) +
                POWER(g.var_val / g.n, 2) / (g.n - 1),
                0
            ) AS welch_df,
        -- Speedup point estimate
        (1 - g.mean_val / NULLIF(b.mean_val, 0)) * 100 AS speedup_pct,
        SQRT(b.var_val / b.n + g.var_val / g.n) AS se_diff
    FROM overall b, overall g
    WHERE b.period_type = 'GEN1' AND g.period_type = 'GEN2'
),
-- Fieller's theorem: exact CI on the ratio gen2/gen1, then transform to speedup.
-- Solves the quadratic: a*r² + b*r + c ≤ 0 where r = gen2_mean / gen1_mean.
-- This avoids the delta-method approximation that is ~2-3pp off for large effects (>30%).
-- The quadratic has two real roots when gen1_mean is sufficiently far from zero
-- (always true for execution times). See: Fieller (1954), Zerbe (1978).
fieller AS (
    SELECT
        c.*,
        -- t-critical value (90% CI, two-sided alpha=0.10)
        CASE
            WHEN c.welch_df < 5 THEN NULL
            WHEN c.welch_df < 7 THEN 2.015
            WHEN c.welch_df < 11 THEN 1.860
            WHEN c.welch_df < 16 THEN 1.796
            WHEN c.welch_df < 21 THEN 1.746
            WHEN c.welch_df < 31 THEN 1.711
            WHEN c.welch_df < 61 THEN 1.684
            ELSE 1.645
        END AS t_crit,
        -- Quadratic coefficients for Fieller's CI on ratio r = gen2/gen1
        -- a = gen1² - t² * var_gen1/n_gen1
        POWER(c.mean_gen1, 2) - POWER(CASE
            WHEN c.welch_df < 5 THEN NULL WHEN c.welch_df < 7 THEN 2.015
            WHEN c.welch_df < 11 THEN 1.860 WHEN c.welch_df < 16 THEN 1.796
            WHEN c.welch_df < 21 THEN 1.746 WHEN c.welch_df < 31 THEN 1.711
            WHEN c.welch_df < 61 THEN 1.684 ELSE 1.645
        END, 2) * c.var_gen1 / c.n_gen1 AS fieller_a,
        -- b = -2 * gen1 * gen2
        -2 * c.mean_gen1 * c.mean_gen2 AS fieller_b,
        -- c = gen2² - t² * var_gen2/n_gen2
        POWER(c.mean_gen2, 2) - POWER(CASE
            WHEN c.welch_df < 5 THEN NULL WHEN c.welch_df < 7 THEN 2.015
            WHEN c.welch_df < 11 THEN 1.860 WHEN c.welch_df < 16 THEN 1.796
            WHEN c.welch_df < 21 THEN 1.746 WHEN c.welch_df < 31 THEN 1.711
            WHEN c.welch_df < 61 THEN 1.684 ELSE 1.645
        END, 2) * c.var_gen2 / c.n_gen2 AS fieller_c
    FROM comparison c
),
p95_comparison AS (
    SELECT
        period_type,
        AVG(p95_exec_sec) AS mean_p95,
        AVG(p99_exec_sec) AS mean_p99
    FROM VW_GEN2_PERF_DAILY
    GROUP BY period_type
),
-- Queue time: computed from ALL queries on the warehouse (not just matched patterns).
-- Queue saturation is an infrastructure metric — cached queries, failed queries, and
-- non-pattern queries all experience queueing. Scoping to matched patterns would under-count.
queue_daily AS (
    SELECT
        activity_date,
        CASE WHEN activity_date >= TO_DATE($cutover_date) THEN 'GEN2' ELSE 'GEN1' END AS period_type,
        ROUND(MEDIAN(
            (COALESCE(queued_overload_time, 0) + COALESCE(queued_provisioning_time, 0)
            + COALESCE(queued_repair_time, 0)) / 1000), 4) AS median_queue_sec,
        ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY
            (COALESCE(queued_overload_time, 0) + COALESCE(queued_provisioning_time, 0)
            + COALESCE(queued_repair_time, 0)) / 1000), 4) AS p95_queue_sec
    FROM GEN2_STAGING_QUERY_HISTORY
    WHERE activity_date >= DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date))
        AND activity_date <= DATEADD(day, +$LOOKAHEAD_DAYS, TO_DATE($cutover_date))
        AND warehouse_name LIKE $warehouse_like
    GROUP BY activity_date, 2
),
queue_comparison AS (
    SELECT
        period_type,
        AVG(median_queue_sec) AS mean_median_queue,
        AVG(p95_queue_sec) AS mean_p95_queue
    FROM queue_daily
    GROUP BY period_type
)
SELECT
    'PERFORMANCE' AS metric_category,
    f.n_gen1 AS gen1_days,
    f.n_gen2 AS gen2_days,
    ROUND(f.mean_gen1, 4) AS gen1_daily_median_sec,
    ROUND(f.mean_gen2, 4) AS gen2_daily_median_sec,
    ROUND(f.speedup_pct, 2) AS speedup_pct,
    -- 90% CI on speedup using Fieller's theorem (exact for the ratio gen2/gen1).
    -- Solves the quadratic a*r² + b*r + c = 0 to get CI bounds on the ratio,
    -- then transforms via speedup = (1 - ratio) * 100.
    -- This avoids the delta-method approximation that is ~2-3pp off for large effects.
    -- The discriminant (b² - 4ac) is always positive for execution times since
    -- gen1_mean is many SEs away from zero. fieller_a > 0 is guaranteed.
    -- NOTE: Snowflake VARIANCE() returns sample variance (n-1 denominator) as required by Welch's test.
    CASE
        WHEN f.fieller_a IS NULL OR f.fieller_a <= 0 THEN NULL  -- Fieller's fails (gen1 too uncertain)
        WHEN (POWER(f.fieller_b, 2) - 4 * f.fieller_a * f.fieller_c) < 0 THEN NULL  -- No real roots
        ELSE ROUND((1 - (-f.fieller_b + SQRT(POWER(f.fieller_b, 2) - 4 * f.fieller_a * f.fieller_c))
            / (2 * f.fieller_a)) * 100, 2)
    END AS speedup_ci_lower_90,
    CASE
        WHEN f.fieller_a IS NULL OR f.fieller_a <= 0 THEN NULL
        WHEN (POWER(f.fieller_b, 2) - 4 * f.fieller_a * f.fieller_c) < 0 THEN NULL
        ELSE ROUND((1 - (-f.fieller_b - SQRT(POWER(f.fieller_b, 2) - 4 * f.fieller_a * f.fieller_c))
            / (2 * f.fieller_a)) * 100, 2)
    END AS speedup_ci_upper_90,
    ROUND(f.t_stat, 3) AS t_statistic,
    ROUND(f.welch_df, 1) AS degrees_of_freedom,
    CASE
        WHEN f.welch_df < 5 THEN 'INSUFFICIENT DF'
        WHEN ABS(f.t_stat) > CASE
            WHEN f.welch_df < 7 THEN 2.571
            WHEN f.welch_df < 11 THEN 2.306
            WHEN f.welch_df < 16 THEN 2.201
            WHEN f.welch_df < 21 THEN 2.120
            WHEN f.welch_df < 31 THEN 2.064
            WHEN f.welch_df < 61 THEN 2.021
            ELSE 1.960
        END THEN 'SIGNIFICANT (p<0.05)'
        WHEN ABS(f.t_stat) > CASE
            WHEN f.welch_df < 7 THEN 2.015
            WHEN f.welch_df < 11 THEN 1.860
            WHEN f.welch_df < 16 THEN 1.796
            WHEN f.welch_df < 21 THEN 1.746
            WHEN f.welch_df < 31 THEN 1.711
            WHEN f.welch_df < 61 THEN 1.684
            ELSE 1.645
        END THEN 'SIGNIFICANT (p<0.10)'
        ELSE 'NOT SIGNIFICANT'
    END AS significance,
    CASE
        WHEN f.welch_df < 5
            THEN 'Degrees of freedom < 5. Too few daily observations in one or both periods for reliable inference. The confidence interval cannot be computed. Wait for more Gen2 days or increase LOOKBACK_DAYS.'
        WHEN ABS(f.t_stat) > CASE
            WHEN f.welch_df < 7 THEN 2.571 WHEN f.welch_df < 11 THEN 2.306
            WHEN f.welch_df < 16 THEN 2.201 WHEN f.welch_df < 21 THEN 2.120
            WHEN f.welch_df < 31 THEN 2.064 WHEN f.welch_df < 61 THEN 2.021 ELSE 1.960
        END THEN 'The overall performance difference between Gen1 and Gen2 is statistically significant at p < 0.05. The speedup_pct and its 90% confidence interval [speedup_ci_lower_90, speedup_ci_upper_90] reflect a real effect unlikely due to chance.'
        WHEN ABS(f.t_stat) > CASE
            WHEN f.welch_df < 7 THEN 2.015 WHEN f.welch_df < 11 THEN 1.860
            WHEN f.welch_df < 16 THEN 1.796 WHEN f.welch_df < 21 THEN 1.746
            WHEN f.welch_df < 31 THEN 1.711 WHEN f.welch_df < 61 THEN 1.684 ELSE 1.645
        END THEN 'Marginally significant (p < 0.10). There is suggestive evidence of a performance difference, but it does not meet the conventional 5% threshold. More Gen2 data may clarify.'
        ELSE 'The observed performance difference is not statistically significant. Day-to-day variance in execution times is large enough that the Gen1-vs-Gen2 difference could be random noise. The 90% confidence interval on speedup spans zero — we cannot rule out that Gen2 is equal to or slower than Gen1.'
    END AS significance_explanation,
    -- Tail latency check
    -- NOTE: These are AVG of daily P95/P99 values, NOT the global P95/P99 across all queries.
    -- AVG(daily P99) approximates E[P99|day] — the expected worst-case on a typical day.
    -- The true unconditional P99 may differ depending on query volume distribution across days.
    -- The 2x threshold is calibrated to this averaged metric and is directionally reliable.
    ROUND(b_p95.mean_p95, 4) AS gen1_mean_p95,
    ROUND(g_p95.mean_p95, 4) AS gen2_mean_p95,
    ROUND(b_p95.mean_p99, 4) AS gen1_mean_p99,
    ROUND(g_p95.mean_p99, 4) AS gen2_mean_p99,
    CASE
        WHEN g_p95.mean_p99 > b_p95.mean_p99 * 2 THEN 'WARNING: P99 doubled'
        WHEN g_p95.mean_p99 > b_p95.mean_p99 * 1.5 THEN 'CAUTION: P99 increased 50%+'
        WHEN g_p95.mean_p99 <= b_p95.mean_p99 * 0.8 THEN 'IMPROVED: P99 reduced 20%+'
        ELSE 'OK'
    END AS tail_latency_check,
    CASE
        WHEN g_p95.mean_p99 > b_p95.mean_p99 * 2
            THEN 'The 99th percentile execution time has more than doubled on Gen2. This means the slowest 1% of queries are taking 2x+ longer. Even if median performance improved, worst-case latency has severely degraded. Investigate VW_GEN2_PATTERN_REGRESSIONS for the specific patterns causing tail bloat.'
        WHEN g_p95.mean_p99 > b_p95.mean_p99 * 1.5
            THEN 'The 99th percentile execution time increased by 50%+ on Gen2. Tail latency is worsening, which may impact user-facing SLAs or timeout-sensitive workflows. Review the worst-performing patterns in VW_GEN2_PATTERN_REGRESSIONS.'
        WHEN g_p95.mean_p99 <= b_p95.mean_p99 * 0.8
            THEN 'Tail latency (P99) has improved by 20%+ on Gen2. The slowest queries are completing faster, indicating Gen2 benefits extend beyond median performance to worst-case latency.'
        ELSE 'Tail latency (P99) is within acceptable bounds. Gen2 is comparable to Gen1 for the slowest queries (within +/-20%).'
    END AS tail_latency_explanation,
    -- Queue time: detects concurrency saturation or slow warehouse resume.
    -- Computed from ALL queries on the warehouse (not just matched patterns).
    -- Compares AVG of daily median and P95 queue times between periods.
    ROUND(b_q.mean_median_queue, 4) AS gen1_mean_median_queue_sec,
    ROUND(g_q.mean_median_queue, 4) AS gen2_mean_median_queue_sec,
    ROUND(b_q.mean_p95_queue, 4) AS gen1_mean_p95_queue_sec,
    ROUND(g_q.mean_p95_queue, 4) AS gen2_mean_p95_queue_sec,
    CASE
        WHEN b_q.mean_p95_queue < 0.1 AND g_q.mean_p95_queue < 0.1 THEN 'OK: Negligible queuing in both periods'
        WHEN g_q.mean_p95_queue > GREATEST(b_q.mean_p95_queue, 1) * 2 THEN 'WARNING: P95 queue time doubled'
        WHEN g_q.mean_p95_queue > GREATEST(b_q.mean_p95_queue, 1) * 1.5 THEN 'CAUTION: P95 queue time +50%'
        WHEN b_q.mean_p95_queue > 1 AND g_q.mean_p95_queue <= b_q.mean_p95_queue * 0.8 THEN 'IMPROVED: Queue time reduced 20%+'
        ELSE 'OK'
    END AS queue_saturation_check,
    CASE
        WHEN b_q.mean_p95_queue < 0.1 AND g_q.mean_p95_queue < 0.1
            THEN 'P95 queue time is < 0.1s in both periods. Warehouses are not experiencing concurrency saturation.'
        WHEN g_q.mean_p95_queue > GREATEST(b_q.mean_p95_queue, 1) * 2
            THEN 'The 95th percentile queue time has more than doubled on Gen2 ('
                || ROUND(b_q.mean_p95_queue, 2) || 's → ' || ROUND(g_q.mean_p95_queue, 2)
                || 's). Queries are waiting longer before execution begins. This may indicate concurrency saturation — Gen2 might need different scaling or auto-suspend settings.'
        WHEN g_q.mean_p95_queue > GREATEST(b_q.mean_p95_queue, 1) * 1.5
            THEN 'P95 queue time increased 50%+ on Gen2 ('
                || ROUND(b_q.mean_p95_queue, 2) || 's → ' || ROUND(g_q.mean_p95_queue, 2)
                || 's). Monitor for further degradation.'
        WHEN b_q.mean_p95_queue > 1 AND g_q.mean_p95_queue <= b_q.mean_p95_queue * 0.8
            THEN 'Queue time improved 20%+ on Gen2. Queries spend less time waiting, likely due to faster execution freeing concurrency slots.'
        ELSE 'Queue time is comparable between Gen1 and Gen2. No concurrency saturation detected.'
    END AS queue_saturation_explanation
FROM fieller f
CROSS JOIN (SELECT * FROM p95_comparison WHERE period_type = 'GEN1') b_p95
CROSS JOIN (SELECT * FROM p95_comparison WHERE period_type = 'GEN2') g_p95
CROSS JOIN (SELECT * FROM queue_comparison WHERE period_type = 'GEN1') b_q
CROSS JOIN (SELECT * FROM queue_comparison WHERE period_type = 'GEN2') g_q;


-- ============================================================================
-- SECTION 3: COST ANALYSIS
-- ============================================================================
-- Direct credit comparison using WAREHOUSE_METERING_HISTORY.
-- NO multiplier needed: Gen2 warehouses consume more credits per hour (e.g., 1.35x on AWS)
-- and WAREHOUSE_METERING_HISTORY already reports these ACTUAL Gen2 credit rates.
-- Since each credit costs the same dollar amount in your Snowflake contract,
-- comparing raw credits IS comparing dollar costs.

-- Step 3a: Daily cost metrics
-- Includes both all-query and matched-pattern query counts with credits-per-query for each.
-- Credits are warehouse-level (no pattern separation) from WAREHOUSE_METERING_HISTORY.
CREATE OR REPLACE VIEW VW_GEN2_COST_DAILY AS
WITH metering AS (
    SELECT
        activity_date,
        CASE WHEN activity_date >= TO_DATE($cutover_date) THEN 'GEN2' ELSE 'GEN1' END AS period_type,
        day_num AS day_of_week_num,
        day_of_week,
        ROUND(SUM(credits_used), 4) AS daily_credits
    FROM GEN2_STAGING_WAREHOUSE_METERING
    WHERE TRUE
        -- AND activity_date >=  DATE_TRUNC('week', DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date)))
        AND activity_date >= DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date))
        AND activity_date <= DATEADD(day, +$LOOKAHEAD_DAYS, TO_DATE($cutover_date))
        AND warehouse_name LIKE $warehouse_like
    GROUP BY activity_date, 2, day_num, day_of_week
),
-- All non-cached queries per day (excludes result cache hits for consistent cost comparison)
all_query_counts AS (
    SELECT
        activity_date,
        COUNT(*) AS all_queries
    FROM GEN2_STAGING_QUERY_HISTORY
    WHERE true
        -- and activity_date >=  DATE_TRUNC('week', DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date)))
        AND activity_date >= DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date))
        AND activity_date <= DATEADD(day, +$LOOKAHEAD_DAYS, TO_DATE($cutover_date))
        AND warehouse_name LIKE $warehouse_like
        AND bytes_scanned > 0  -- Exclude result-cached queries: they consume no warehouse credits
    GROUP BY activity_date
),
-- Matched/common non-cached pattern queries per day (from VW_GEN2_PERF_DAILY)
pattern_query_counts AS (
    SELECT
        activity_date,
        query_count AS pattern_queries
    FROM VW_GEN2_PERF_DAILY
)
SELECT
    m.activity_date,
    m.period_type,
    m.day_of_week_num,
    m.day_of_week,
    m.daily_credits,
    -- All non-cached queries (consistent with performance scope — cached queries consume no credits)
    COALESCE(aq.all_queries, 0) AS all_query_count,
    ROUND(m.daily_credits / NULLIF(aq.all_queries, 0), 6) AS all_credits_per_query,
    -- Matched/common non-cached pattern queries (from VW_GEN2_PERF_DAILY)
    -- NOTE: wh_credits_per_pattern_query = total warehouse credits / non-cached matched query count.
    -- This is an UPPER-BOUND proxy (numerator includes all warehouse activity, denominator
    -- only counts matched patterns). It overstates true per-matched-query cost when
    -- non-matched queries consume significant warehouse time.
    COALESCE(mq.pattern_queries, 0) AS pattern_query_count,
    ROUND(m.daily_credits / NULLIF(mq.pattern_queries, 0), 6) AS wh_credits_per_pattern_query
FROM metering m
LEFT JOIN all_query_counts aq ON m.activity_date = aq.activity_date
LEFT JOIN pattern_query_counts mq ON m.activity_date = mq.activity_date;


-- Step 3b: Welch's t-test on cost efficiency (credits per query)
--
-- This view outputs TWO cost-per-query metrics. They answer different questions:
--
--   avg_credits_per_query      = AVG of (daily_credits / daily_queries)
--     → Equal weight per DAY. Used for the Welch's t-test (requires independent,
--       equally-weighted observations). Answers: "Is the typical day's cost-per-query
--       different on Gen2?" Robust to volume changes between periods.
--
--   weighted_credits_per_query = SUM(credits) / SUM(queries)  across the entire period
--     → Equal weight per QUERY. The actual blended cost you'd see on the invoice.
--       Answers: "What did we actually pay per query?" Sensitive to volume shifts —
--       high-volume days dominate.
--
-- WHEN THEY DIVERGE:
--   Large divergence (>10%) signals unstable daily volumes. Example:
--     Day 1: 100 credits / 1000 queries = 0.10 cpq
--     Day 2:  50 credits /  100 queries = 0.50 cpq
--     avg_cpq = (0.10 + 0.50) / 2 = 0.30    (each day equal)
--     weighted_cpq = 150 / 1100    = 0.136   (each query equal)
--   The avg is inflated by the low-volume day. Neither is "wrong" — avg isolates
--   the rate from volume effects (needed for statistics), weighted reflects reality.
--
-- HOW TO USE:
--   - Assessment:  use avg (t-test with CI) — statistically rigorous
--   - Invoice forecasting: use weighted — reflects actual spend
--   - If both agree:       high confidence in the cost signal
--   - If they diverge:     investigate volume instability (workload changes, scheduling shifts)
--
CREATE OR REPLACE VIEW VW_GEN2_COST_WELCH_TEST AS
WITH -- All-query credits per query (primary: volume-normalized, all queries)
all_cpq_stats AS (
    SELECT
        period_type,
        COUNT(*) AS n,
        AVG(all_credits_per_query) AS mean_val,
        VARIANCE(all_credits_per_query) AS var_val
    FROM VW_GEN2_COST_DAILY
    WHERE all_credits_per_query IS NOT NULL
    GROUP BY period_type
),
all_cpq_comparison AS (
    SELECT
        b.n AS n_gen1, g.n AS n_gen2,
        b.mean_val AS mean_gen1, g.mean_val AS mean_gen2,
        b.var_val AS var_gen1, g.var_val AS var_gen2,
        (g.mean_val - b.mean_val) /
            NULLIF(SQRT(b.var_val / b.n + g.var_val / g.n), 0) AS t_stat,
        POWER(b.var_val / b.n + g.var_val / g.n, 2) /
            NULLIF(
                POWER(b.var_val / b.n, 2) / (b.n - 1) +
                POWER(g.var_val / g.n, 2) / (g.n - 1), 0
            ) AS welch_df,
        (g.mean_val - b.mean_val) * 100.0 / NULLIF(b.mean_val, 0) AS pct_change,
        SQRT(b.var_val / b.n + g.var_val / g.n) AS se_diff
    FROM all_cpq_stats b, all_cpq_stats g
    WHERE b.period_type = 'GEN1' AND g.period_type = 'GEN2'
),
-- Matched-pattern credits per query
pattern_cpq_stats AS (
    SELECT
        period_type,
        COUNT(*) AS n,
        AVG(wh_credits_per_pattern_query) AS mean_val,
        VARIANCE(wh_credits_per_pattern_query) AS var_val
    FROM VW_GEN2_COST_DAILY
    WHERE wh_credits_per_pattern_query IS NOT NULL
    GROUP BY period_type
),
pattern_cpq_comparison AS (
    SELECT
        b.n AS n_gen1, g.n AS n_gen2,
        b.mean_val AS mean_gen1, g.mean_val AS mean_gen2,
        b.var_val AS var_gen1, g.var_val AS var_gen2,
        (g.mean_val - b.mean_val) /
            NULLIF(SQRT(b.var_val / b.n + g.var_val / g.n), 0) AS t_stat,
        POWER(b.var_val / b.n + g.var_val / g.n, 2) /
            NULLIF(
                POWER(b.var_val / b.n, 2) / (b.n - 1) +
                POWER(g.var_val / g.n, 2) / (g.n - 1), 0
            ) AS welch_df,
        (g.mean_val - b.mean_val) * 100.0 / NULLIF(b.mean_val, 0) AS pct_change,
        SQRT(b.var_val / b.n + g.var_val / g.n) AS se_diff
    FROM pattern_cpq_stats b, pattern_cpq_stats g
    WHERE b.period_type = 'GEN1' AND g.period_type = 'GEN2'
),
-- Weighted ratios: SUM(credits) / SUM(queries) — actual blended cost per query
weighted_ratios AS (
    SELECT
        period_type,
        SUM(daily_credits) AS total_credits,
        SUM(all_query_count) AS total_all_queries,
        SUM(pattern_query_count) AS total_pattern_queries,
        ROUND(SUM(daily_credits) / NULLIF(SUM(all_query_count), 0), 6) AS weighted_all_credits_per_query,
        ROUND(SUM(daily_credits) / NULLIF(SUM(pattern_query_count), 0), 6) AS weighted_wh_credits_per_pattern_query
    FROM VW_GEN2_COST_DAILY
    GROUP BY period_type
),
-- Raw daily credits (total bill impact)
raw_stats AS (
    SELECT
        period_type,
        AVG(daily_credits) AS mean_daily_credits,
        AVG(all_query_count) AS mean_daily_all_queries,
        AVG(pattern_query_count) AS mean_daily_pattern_queries
    FROM VW_GEN2_COST_DAILY
    GROUP BY period_type
)
SELECT
    'COST' AS metric_category,
    c.n_gen1 AS gen1_days,
    c.n_gen2 AS gen2_days,

    -- === ALL QUERIES: AVG of daily ratios (for t-test) ===
    -- NOTE: The assessment in VW_GEN2_MIGRATION_DECISION uses the ALL-QUERY scope (c.*)
    -- for cost — see the design note at Section 7 for rationale.
    -- The matched-pattern scope (mc.*) is output as a diagnostic context column
    -- (pattern_cost_change_pct). The all-query t-test below includes unmatched/new
    -- queries and may diverge from matched results when workload composition changes.
    ROUND(c.mean_gen1, 6) AS gen1_all_avg_credits_per_query,
    ROUND(c.mean_gen2, 6) AS gen2_all_avg_credits_per_query,
    ROUND(c.pct_change, 2) AS all_avg_cpq_change_pct,
    -- 90% CI on per-query cost change (all queries)
    ROUND(c.pct_change - (CASE
        WHEN c.welch_df < 5 THEN NULL
        WHEN c.welch_df < 7 THEN 2.015
        WHEN c.welch_df < 11 THEN 1.860
        WHEN c.welch_df < 16 THEN 1.796
        WHEN c.welch_df < 21 THEN 1.746
        WHEN c.welch_df < 31 THEN 1.711
        WHEN c.welch_df < 61 THEN 1.684
        ELSE 1.645
    END) * c.se_diff / NULLIF(c.mean_gen1, 0) * 100, 2) AS all_avg_cpq_ci_lower_90,
    ROUND(c.pct_change + (CASE
        WHEN c.welch_df < 5 THEN NULL
        WHEN c.welch_df < 7 THEN 2.015
        WHEN c.welch_df < 11 THEN 1.860
        WHEN c.welch_df < 16 THEN 1.796
        WHEN c.welch_df < 21 THEN 1.746
        WHEN c.welch_df < 31 THEN 1.711
        WHEN c.welch_df < 61 THEN 1.684
        ELSE 1.645
    END) * c.se_diff / NULLIF(c.mean_gen1, 0) * 100, 2) AS all_avg_cpq_ci_upper_90,
    ROUND(c.t_stat, 3) AS t_statistic,
    ROUND(c.welch_df, 1) AS degrees_of_freedom,
    CASE
        WHEN c.welch_df < 5 THEN 'INSUFFICIENT DF'
        WHEN ABS(c.t_stat) > CASE
            WHEN c.welch_df < 7 THEN 2.571 WHEN c.welch_df < 11 THEN 2.306
            WHEN c.welch_df < 16 THEN 2.201 WHEN c.welch_df < 21 THEN 2.120
            WHEN c.welch_df < 31 THEN 2.064 WHEN c.welch_df < 61 THEN 2.021
            ELSE 1.960
        END THEN 'SIGNIFICANT (p<0.05)'
        WHEN ABS(c.t_stat) > CASE
            WHEN c.welch_df < 7 THEN 2.015 WHEN c.welch_df < 11 THEN 1.860
            WHEN c.welch_df < 16 THEN 1.796 WHEN c.welch_df < 21 THEN 1.746
            WHEN c.welch_df < 31 THEN 1.711 WHEN c.welch_df < 61 THEN 1.684
            ELSE 1.645
        END THEN 'SIGNIFICANT (p<0.10)'
        ELSE 'NOT SIGNIFICANT'
    END AS significance,
    CASE
        WHEN c.welch_df < 5
            THEN 'Degrees of freedom < 5. Too few daily observations for a reliable t-test on credits-per-query. Collect more days of data.'
        WHEN ABS(c.t_stat) > CASE
            WHEN c.welch_df < 7 THEN 2.571 WHEN c.welch_df < 11 THEN 2.306
            WHEN c.welch_df < 16 THEN 2.201 WHEN c.welch_df < 21 THEN 2.120
            WHEN c.welch_df < 31 THEN 2.064 WHEN c.welch_df < 61 THEN 2.021 ELSE 1.960
        END THEN 'The per-query credit cost difference is statistically significant at p < 0.05. This is volume-normalized, so it reflects true Gen2 cost efficiency independent of workload changes.'
        WHEN ABS(c.t_stat) > CASE
            WHEN c.welch_df < 7 THEN 2.015 WHEN c.welch_df < 11 THEN 1.860
            WHEN c.welch_df < 16 THEN 1.796 WHEN c.welch_df < 21 THEN 1.746
            WHEN c.welch_df < 31 THEN 1.711 WHEN c.welch_df < 61 THEN 1.684 ELSE 1.645
        END THEN 'Marginally significant per-query cost difference (p < 0.10). Suggestive but not conclusive. More data will clarify.'
        ELSE 'The per-query credit cost difference is not statistically significant. Day-to-day variation is large enough that the Gen1-vs-Gen2 difference could be noise.'
    END AS significance_explanation,

    -- === ALL QUERIES: Weighted ratio SUM(credits)/SUM(queries) — actual blended cost ===
    -- Unlike avg above (equal weight per day), weighted gives equal weight per query.
    -- This is what you'd see on the invoice. Compare with avg to detect volume instability.
    wrb.weighted_all_credits_per_query AS gen1_all_weighted_credits_per_query,
    wrg.weighted_all_credits_per_query AS gen2_all_weighted_credits_per_query,
    ROUND((wrg.weighted_all_credits_per_query - wrb.weighted_all_credits_per_query) * 100.0
        / NULLIF(wrb.weighted_all_credits_per_query, 0), 2) AS all_weighted_cpq_change_pct,

    -- === MATCHED PATTERNS: Welch's t-test on wh_credits_per_pattern_query ===
    -- NOTE: wh_credits_per_pattern_query is an upper-bound proxy (total warehouse credits /
    -- matched query count). Use alongside all-query metrics for a complete picture.
    ROUND(mc.mean_gen1, 6) AS gen1_avg_cost_per_pattern_query,
    ROUND(mc.mean_gen2, 6) AS gen2_avg_cost_per_pattern_query,
    ROUND(mc.pct_change, 2) AS avg_cost_per_pattern_query_change_pct,
    ROUND(mc.pct_change - (CASE
        WHEN mc.welch_df < 5 THEN NULL
        WHEN mc.welch_df < 7 THEN 2.015
        WHEN mc.welch_df < 11 THEN 1.860
        WHEN mc.welch_df < 16 THEN 1.796
        WHEN mc.welch_df < 21 THEN 1.746
        WHEN mc.welch_df < 31 THEN 1.711
        WHEN mc.welch_df < 61 THEN 1.684
        ELSE 1.645
    END) * mc.se_diff / NULLIF(mc.mean_gen1, 0) * 100, 2) AS avg_cost_per_pattern_query_ci_lower_90,
    ROUND(mc.pct_change + (CASE
        WHEN mc.welch_df < 5 THEN NULL
        WHEN mc.welch_df < 7 THEN 2.015
        WHEN mc.welch_df < 11 THEN 1.860
        WHEN mc.welch_df < 16 THEN 1.796
        WHEN mc.welch_df < 21 THEN 1.746
        WHEN mc.welch_df < 31 THEN 1.711
        WHEN mc.welch_df < 61 THEN 1.684
        ELSE 1.645
    END) * mc.se_diff / NULLIF(mc.mean_gen1, 0) * 100, 2) AS avg_cost_per_pattern_query_ci_upper_90,
    ROUND(mc.t_stat, 3) AS pattern_cost_t_statistic,
    ROUND(mc.welch_df, 1) AS pattern_cost_degrees_of_freedom,
    CASE
        WHEN mc.welch_df < 5 THEN 'INSUFFICIENT DF'
        WHEN ABS(mc.t_stat) > CASE
            WHEN mc.welch_df < 7 THEN 2.571 WHEN mc.welch_df < 11 THEN 2.306
            WHEN mc.welch_df < 16 THEN 2.201 WHEN mc.welch_df < 21 THEN 2.120
            WHEN mc.welch_df < 31 THEN 2.064 WHEN mc.welch_df < 61 THEN 2.021
            ELSE 1.960
        END THEN 'SIGNIFICANT (p<0.05)'
        WHEN ABS(mc.t_stat) > CASE
            WHEN mc.welch_df < 7 THEN 2.015 WHEN mc.welch_df < 11 THEN 1.860
            WHEN mc.welch_df < 16 THEN 1.796 WHEN mc.welch_df < 21 THEN 1.746
            WHEN mc.welch_df < 31 THEN 1.711 WHEN mc.welch_df < 61 THEN 1.684
            ELSE 1.645
        END THEN 'SIGNIFICANT (p<0.10)'
        ELSE 'NOT SIGNIFICANT'
    END AS pattern_cost_significance,
    -- === MATCHED PATTERNS: Weighted ratio SUM(credits)/SUM(queries) ===
    -- Same invoice-perspective metric, scoped to matched patterns only.
    wrb.weighted_wh_credits_per_pattern_query AS gen1_weighted_cost_per_pattern_query,
    wrg.weighted_wh_credits_per_pattern_query AS gen2_weighted_cost_per_pattern_query,
    ROUND((wrg.weighted_wh_credits_per_pattern_query - wrb.weighted_wh_credits_per_pattern_query) * 100.0
        / NULLIF(wrb.weighted_wh_credits_per_pattern_query, 0), 2) AS weighted_cost_per_pattern_query_change_pct,

    -- === Daily credits (total bill) and query volume context ===
    ROUND(rb.mean_daily_credits, 4) AS gen1_daily_credits,
    ROUND(rg.mean_daily_credits, 4) AS gen2_daily_credits,
    ROUND((rg.mean_daily_credits - rb.mean_daily_credits) * 100.0
        / NULLIF(rb.mean_daily_credits, 0), 2) AS total_credit_change_pct,
    ROUND(rb.mean_daily_all_queries, 0) AS gen1_daily_all_queries,
    ROUND(rg.mean_daily_all_queries, 0) AS gen2_daily_all_queries,
    ROUND((rg.mean_daily_all_queries - rb.mean_daily_all_queries) * 100.0
        / NULLIF(rb.mean_daily_all_queries, 0), 2) AS all_query_volume_change_pct,
    ROUND(rb.mean_daily_pattern_queries, 0) AS gen1_daily_pattern_queries,
    ROUND(rg.mean_daily_pattern_queries, 0) AS gen2_daily_pattern_queries,
    ROUND((rg.mean_daily_pattern_queries - rb.mean_daily_pattern_queries) * 100.0
        / NULLIF(rb.mean_daily_pattern_queries, 0), 2) AS pattern_query_volume_change_pct,

    -- Break-even on per-query cost (all-query scope — numerator and denominator are consistent)
    CASE
        WHEN c.pct_change <= 0
            THEN 'COST LOWER (Gen2 cheaper per query)'
        WHEN c.pct_change <= 5
            THEN 'COST NEUTRAL (per-query cost within 5%)'
        WHEN c.pct_change <= 15
            THEN 'COST HIGHER (per-query cost 5-15% above Gen1)'
        ELSE 'COST SIGNIFICANTLY HIGHER (per-query cost >15% above Gen1)'
    END AS breakeven_status,
    CASE
        WHEN c.pct_change <= 0
            THEN 'Gen2 costs equal or fewer credits per query than Gen1. Despite the higher per-hour credit rate (1.35x on AWS), Gen2 completes work fast enough to offset the premium. This is true cost efficiency.'
        WHEN c.pct_change <= 5
            THEN 'Gen2 per-query credit cost is within 5% of Gen1. Effectively cost-neutral. Monitor over subsequent weeks to confirm stability.'
        WHEN c.pct_change <= 15
            THEN 'Gen2 costs 5-15% more credits per query. The higher per-hour rate is not fully offset by faster execution. Check warehouse auto-suspend or minimum billing settings.'
        ELSE 'Gen2 costs >15% more credits per query. Each unit of work is significantly more expensive. Review warehouse sizing and auto-suspend settings.'
    END AS breakeven_explanation,

    -- === AVG vs WEIGHTED DIVERGENCE CHECK ===
    -- Large divergence signals unstable daily query volumes between periods.
    -- When avg and weighted agree, the cost signal is reliable regardless of method.
    -- When they diverge, investigate volume shifts before trusting either alone.
    ROUND(ABS(c.pct_change -
        (wrg.weighted_all_credits_per_query - wrb.weighted_all_credits_per_query) * 100.0
        / NULLIF(wrb.weighted_all_credits_per_query, 0)), 1) AS all_cpq_avg_vs_weighted_divergence_pp,
    ROUND(ABS(mc.pct_change -
        (wrg.weighted_wh_credits_per_pattern_query - wrb.weighted_wh_credits_per_pattern_query) * 100.0
        / NULLIF(wrb.weighted_wh_credits_per_pattern_query, 0)), 1) AS pattern_cpq_avg_vs_weighted_divergence_pp,
    CASE
        WHEN ABS(c.pct_change -
            (wrg.weighted_all_credits_per_query - wrb.weighted_all_credits_per_query) * 100.0
            / NULLIF(wrb.weighted_all_credits_per_query, 0)) > 10
            OR ABS(mc.pct_change -
            (wrg.weighted_wh_credits_per_pattern_query - wrb.weighted_wh_credits_per_pattern_query) * 100.0
            / NULLIF(wrb.weighted_wh_credits_per_pattern_query, 0)) > 10
            THEN 'WARNING: >10pp divergence — daily query volumes are unstable. The t-test (avg) isolates cost-rate changes; the weighted metric reflects actual spend. Check volume shifts before acting on either alone.'
        WHEN ABS(c.pct_change -
            (wrg.weighted_all_credits_per_query - wrb.weighted_all_credits_per_query) * 100.0
            / NULLIF(wrb.weighted_all_credits_per_query, 0)) > 5
            OR ABS(mc.pct_change -
            (wrg.weighted_wh_credits_per_pattern_query - wrb.weighted_wh_credits_per_pattern_query) * 100.0
            / NULLIF(wrb.weighted_wh_credits_per_pattern_query, 0)) > 5
            THEN 'NOTICE: 5-10pp divergence — moderate volume variability. Both metrics are usable but interpret with awareness of volume changes.'
        ELSE 'OK: avg and weighted metrics agree (<5pp). Cost signal is consistent regardless of method.'
    END AS cost_metric_consistency
FROM all_cpq_comparison c
CROSS JOIN pattern_cpq_comparison mc
CROSS JOIN (SELECT * FROM weighted_ratios WHERE period_type = 'GEN1') wrb
CROSS JOIN (SELECT * FROM weighted_ratios WHERE period_type = 'GEN2') wrg
CROSS JOIN (SELECT * FROM raw_stats WHERE period_type = 'GEN1') rb
CROSS JOIN (SELECT * FROM raw_stats WHERE period_type = 'GEN2') rg;


-- Step 3c: Day-of-week cost breakdown (detect weekend/weekday anomalies)
-- Credits are warehouse-level (all queries). Query counts shown for both all and matched patterns.
CREATE OR REPLACE VIEW VW_GEN2_COST_BY_DOW AS
WITH dow_stats AS (
    SELECT
        day_of_week_num,
        day_of_week,
        period_type,
        COUNT(*) AS n,
        ROUND(AVG(daily_credits), 4) AS avg_daily_credits,
        ROUND(AVG(all_query_count), 0) AS avg_all_queries,
        ROUND(AVG(pattern_query_count), 0) AS avg_pattern_queries,
        ROUND(AVG(all_credits_per_query), 6) AS avg_all_cpq,
        ROUND(AVG(wh_credits_per_pattern_query), 6) AS avg_pattern_cpq
    FROM VW_GEN2_COST_DAILY
    GROUP BY day_of_week_num, day_of_week, period_type
)
SELECT
    b.day_of_week_num,
    b.day_of_week,
    b.n AS gen1_samples,
    g.n AS gen2_samples,
    -- Raw daily credits (total cost — all warehouse activity)
    b.avg_daily_credits AS gen1_avg_daily_credits,
    g.avg_daily_credits AS gen2_avg_daily_credits,
    ROUND((g.avg_daily_credits - b.avg_daily_credits) * 100.0
        / NULLIF(b.avg_daily_credits, 0), 2) AS credit_change_pct,
    -- All queries: volume and credits per query
    b.avg_all_queries AS gen1_all_queries,
    g.avg_all_queries AS gen2_all_queries,
    ROUND((g.avg_all_queries - b.avg_all_queries) * 100.0
        / NULLIF(b.avg_all_queries, 0), 2) AS all_query_volume_change_pct,
    b.avg_all_cpq AS gen1_all_credits_per_query,
    g.avg_all_cpq AS gen2_all_credits_per_query,
    ROUND((g.avg_all_cpq - b.avg_all_cpq) * 100.0
        / NULLIF(b.avg_all_cpq, 0), 2) AS all_cpq_change_pct,
    -- Matched patterns: volume and credits per query
    b.avg_pattern_queries AS gen1_pattern_queries,
    g.avg_pattern_queries AS gen2_pattern_queries,
    ROUND((g.avg_pattern_queries - b.avg_pattern_queries) * 100.0
        / NULLIF(b.avg_pattern_queries, 0), 2) AS pattern_query_volume_change_pct,
    b.avg_pattern_cpq AS gen1_wh_credits_per_pattern_query,
    g.avg_pattern_cpq AS gen2_wh_credits_per_pattern_query,
    ROUND((g.avg_pattern_cpq - b.avg_pattern_cpq) * 100.0
        / NULLIF(b.avg_pattern_cpq, 0), 2) AS pattern_cpq_change_pct
FROM dow_stats b
INNER JOIN dow_stats g
    ON b.day_of_week_num = g.day_of_week_num
    AND b.period_type = 'GEN1'
    AND g.period_type = 'GEN2'
ORDER BY b.day_of_week_num;


-- ============================================================================
-- SECTION 4: RELIABILITY ANALYSIS
-- ============================================================================
-- Welch's t-test on DAILY success rates (one observation per day).
-- This unifies methodology across all three axes (performance, cost, reliability)
-- and naturally accounts for within-day query clustering.
--
-- Previous approach used a two-proportion z-test on individual queries, which
-- treats each query as independent. In practice, queries cluster by day, workflow,
-- and scheduler burst, inflating the z-statistic. The day-level Welch's t-test
-- eliminates this problem: n = number of days (not queries), so clustering within
-- a day is absorbed into the daily aggregate.
--
-- Practical significance thresholds are retained: DEGRADED requires BOTH statistical
-- significance (Welch t-test) AND a meaningful effect size (≥0.5pp drop or ≥25%
-- failure rate increase).
CREATE OR REPLACE VIEW VW_GEN2_RELIABILITY AS
WITH daily_rates AS (
    SELECT
        activity_date,
        CASE WHEN activity_date >= TO_DATE($cutover_date) THEN 'GEN2' ELSE 'GEN1' END AS period_type,
        COUNT(*) AS total_queries,
        SUM(CASE WHEN execution_status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
        ROUND(SUM(CASE WHEN execution_status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0
            / COUNT(*), 4) AS daily_success_rate_pct
    FROM GEN2_STAGING_QUERY_HISTORY
    WHERE TRUE
        AND activity_date >= DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date))
        AND activity_date <= DATEADD(day, +$LOOKAHEAD_DAYS, TO_DATE($cutover_date))
        AND warehouse_name LIKE $warehouse_like
    GROUP BY activity_date, 2
),
-- Aggregate totals for overall success rates (reporting context)
period_totals AS (
    SELECT
        period_type,
        SUM(total_queries) AS total_queries,
        SUM(success_count) AS total_success
    FROM daily_rates
    GROUP BY period_type
),
-- Welch's t-test on daily success rates
welch_stats AS (
    SELECT
        period_type,
        COUNT(*) AS n,
        AVG(daily_success_rate_pct) AS mean_val,
        VARIANCE(daily_success_rate_pct) AS var_val
    FROM daily_rates
    GROUP BY period_type
),
comparison AS (
    SELECT
        b.n AS n_gen1, g.n AS n_gen2,
        b.mean_val AS mean_gen1, g.mean_val AS mean_gen2,
        b.var_val AS var_gen1, g.var_val AS var_gen2,
        (g.mean_val - b.mean_val) /
            NULLIF(SQRT(b.var_val / b.n + g.var_val / g.n), 0) AS t_stat,
        POWER(b.var_val / b.n + g.var_val / g.n, 2) /
            NULLIF(
                POWER(b.var_val / b.n, 2) / (b.n - 1) +
                POWER(g.var_val / g.n, 2) / (g.n - 1), 0
            ) AS welch_df,
        (g.mean_val - b.mean_val) AS diff_pp,
        SQRT(b.var_val / b.n + g.var_val / g.n) AS se_diff
    FROM welch_stats b, welch_stats g
    WHERE b.period_type = 'GEN1' AND g.period_type = 'GEN2'
),
-- Overall success rates from aggregate totals
overall AS (
    SELECT
        tb.total_success * 100.0 / tb.total_queries AS p_gen1,
        tg.total_success * 100.0 / tg.total_queries AS p_gen2,
        tb.total_queries AS q_gen1,
        tg.total_queries AS q_gen2
    FROM (SELECT * FROM period_totals WHERE period_type = 'GEN1') tb,
         (SELECT * FROM period_totals WHERE period_type = 'GEN2') tg
)
SELECT
    'RELIABILITY' AS metric_category,
    -- Total query counts (context for scale)
    o.q_gen1 AS n_gen1,
    o.q_gen2 AS n_gen2,
    -- Overall success rates (from aggregate totals, not avg of daily rates)
    ROUND(o.p_gen1, 4) AS gen1_success_rate,
    ROUND(o.p_gen2, 4) AS gen2_success_rate,
    ROUND(o.p_gen2 - o.p_gen1, 4) AS success_rate_diff_pp,
    -- Failure rate metrics
    ROUND(100 - o.p_gen1, 4) AS gen1_failure_rate_pct,
    ROUND(100 - o.p_gen2, 4) AS gen2_failure_rate_pct,
    ROUND((100 - o.p_gen2) - (100 - o.p_gen1), 4) AS failure_rate_diff_pp,
    ROUND(((100 - o.p_gen2) - (100 - o.p_gen1)) / NULLIF(100 - o.p_gen1, 0) * 100, 1) AS failure_rate_change_pct,
    -- Welch t-test on daily success rates (replaces z-test on individual queries)
    c.n_gen1 AS gen1_days,
    c.n_gen2 AS gen2_days,
    ROUND(c.t_stat, 3) AS t_statistic,
    ROUND(c.welch_df, 1) AS degrees_of_freedom,
    -- Significance using same t-critical lookup as performance/cost views
    CASE
        WHEN c.welch_df < 5 THEN 'INSUFFICIENT DF'
        WHEN c.t_stat < -1 * CASE
            WHEN c.welch_df < 7 THEN 2.571 WHEN c.welch_df < 11 THEN 2.306
            WHEN c.welch_df < 16 THEN 2.201 WHEN c.welch_df < 21 THEN 2.120
            WHEN c.welch_df < 31 THEN 2.064 WHEN c.welch_df < 61 THEN 2.021
            ELSE 1.960
        END THEN 'SIGNIFICANT (p<0.05)'
        WHEN c.t_stat < -1 * CASE
            WHEN c.welch_df < 7 THEN 2.015 WHEN c.welch_df < 11 THEN 1.860
            WHEN c.welch_df < 16 THEN 1.796 WHEN c.welch_df < 21 THEN 1.746
            WHEN c.welch_df < 31 THEN 1.711 WHEN c.welch_df < 61 THEN 1.684
            ELSE 1.645
        END THEN 'SIGNIFICANT (p<0.10)'
        ELSE 'NOT SIGNIFICANT'
    END AS significance,
    -- Combined assessment: statistical significance + practical significance (effect size)
    -- DEGRADED requires BOTH stat significance AND meaningful effect (≥0.5pp or ≥25% failure increase)
    CASE
        WHEN c.welch_df < 5 THEN 'INSUFFICIENT DF'
        -- Significant at p<0.05 + large effect
        WHEN c.t_stat < -1 * CASE
                WHEN c.welch_df < 7 THEN 2.571 WHEN c.welch_df < 11 THEN 2.306
                WHEN c.welch_df < 16 THEN 2.201 WHEN c.welch_df < 21 THEN 2.120
                WHEN c.welch_df < 31 THEN 2.064 WHEN c.welch_df < 61 THEN 2.021 ELSE 1.960
            END
            AND (ABS(o.p_gen2 - o.p_gen1) >= 0.5
                 OR ABS(((100 - o.p_gen2) - (100 - o.p_gen1)) / NULLIF(100 - o.p_gen1, 0)) * 100 > 25)
            THEN 'DEGRADED (p<0.05, large effect)'
        -- Significant at p<0.05 + small effect
        WHEN c.t_stat < -1 * CASE
                WHEN c.welch_df < 7 THEN 2.571 WHEN c.welch_df < 11 THEN 2.306
                WHEN c.welch_df < 16 THEN 2.201 WHEN c.welch_df < 21 THEN 2.120
                WHEN c.welch_df < 31 THEN 2.064 WHEN c.welch_df < 61 THEN 2.021 ELSE 1.960
            END
            THEN 'MINOR DEGRADATION (p<0.05, small effect)'
        -- Significant at p<0.10
        WHEN c.t_stat < -1 * CASE
                WHEN c.welch_df < 7 THEN 2.015 WHEN c.welch_df < 11 THEN 1.860
                WHEN c.welch_df < 16 THEN 1.796 WHEN c.welch_df < 21 THEN 1.746
                WHEN c.welch_df < 31 THEN 1.711 WHEN c.welch_df < 61 THEN 1.684 ELSE 1.645
            END
            THEN 'MINOR DEGRADATION (p<0.10)'
        ELSE 'NO DEGRADATION'
    END AS reliability_status,
    CASE
        WHEN c.welch_df < 5
            THEN 'Degrees of freedom < 5. Too few days of data for a reliable t-test on daily success rates. Collect more data.'
        WHEN c.t_stat < -1 * CASE
                WHEN c.welch_df < 7 THEN 2.571 WHEN c.welch_df < 11 THEN 2.306
                WHEN c.welch_df < 16 THEN 2.201 WHEN c.welch_df < 21 THEN 2.120
                WHEN c.welch_df < 31 THEN 2.064 WHEN c.welch_df < 61 THEN 2.021 ELSE 1.960
            END
            AND (ABS(o.p_gen2 - o.p_gen1) >= 0.5
                 OR ABS(((100 - o.p_gen2) - (100 - o.p_gen1)) / NULLIF(100 - o.p_gen1, 0)) * 100 > 25)
            THEN 'DEGRADED: Gen2 daily success rate is significantly lower (Welch t-test, p<0.05) with a meaningful effect size. '
                 || 'Success rate: ' || ROUND(o.p_gen1, 2) || '% → ' || ROUND(o.p_gen2, 2) || '% '
                 || '(diff: ' || ROUND(o.p_gen2 - o.p_gen1, 2) || 'pp). '
                 || 'Failure rate change: ' || ROUND(((100 - o.p_gen2) - (100 - o.p_gen1)) / NULLIF(100 - o.p_gen1, 0) * 100, 1) || '%. '
                 || 'This is the strongest negative signal in the framework. See VW_GEN2_ERROR_ANALYSIS for error type breakdown.'
        WHEN c.t_stat < -1 * CASE
                WHEN c.welch_df < 7 THEN 2.571 WHEN c.welch_df < 11 THEN 2.306
                WHEN c.welch_df < 16 THEN 2.201 WHEN c.welch_df < 21 THEN 2.120
                WHEN c.welch_df < 31 THEN 2.064 WHEN c.welch_df < 61 THEN 2.021 ELSE 1.960
            END
            THEN 'MINOR DEGRADATION: Gen2 daily success rate is statistically lower (p<0.05) but the effect size is small. '
                 || 'Success rate: ' || ROUND(o.p_gen1, 2) || '% → ' || ROUND(o.p_gen2, 2) || '% '
                 || '(diff: ' || ROUND(o.p_gen2 - o.p_gen1, 2) || 'pp). '
                 || 'At scale across many warehouses, small degradations compound. See VW_GEN2_ERROR_ANALYSIS for error types.'
        WHEN c.t_stat < -1 * CASE
                WHEN c.welch_df < 7 THEN 2.015 WHEN c.welch_df < 11 THEN 1.860
                WHEN c.welch_df < 16 THEN 1.796 WHEN c.welch_df < 21 THEN 1.746
                WHEN c.welch_df < 31 THEN 1.711 WHEN c.welch_df < 61 THEN 1.684 ELSE 1.645
            END
            THEN 'MINOR DEGRADATION: Marginally significant (p<0.10). Suggestive of a reliability shift but not conclusive. '
                 || 'Success rate: ' || ROUND(o.p_gen1, 2) || '% → ' || ROUND(o.p_gen2, 2) || '%. Monitor closely.'
        ELSE 'NO DEGRADATION: No statistically significant difference in daily success rates. Gen2 reliability is comparable to or better than Gen1.'
    END AS reliability_explanation
FROM comparison c
CROSS JOIN overall o;


-- Error analysis: both NEW error types and INCREASED frequency of existing errors
-- FIXED: Now uses per-query rates (matching VW_GEN2_RELIABILITY) instead of per-day rates
CREATE OR REPLACE VIEW VW_GEN2_ERROR_ANALYSIS AS
WITH period_totals AS (
    SELECT
        'GEN1' AS period_type,
        COUNT(*) AS total_queries,
        COUNT(DISTINCT activity_date) AS total_days
    FROM GEN2_STAGING_QUERY_HISTORY
    WHERE activity_date < TO_DATE($cutover_date)
        -- AND activity_date >=  DATE_TRUNC('week', DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date)))
        AND activity_date >= DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date))
        AND warehouse_name LIKE $warehouse_like

    UNION ALL

    SELECT
        'GEN2' AS period_type,
        COUNT(*) AS total_queries,
        COUNT(DISTINCT activity_date) AS total_days
    FROM GEN2_STAGING_QUERY_HISTORY
    WHERE activity_date >= TO_DATE($cutover_date)
        AND activity_date <= DATEADD(day, +$LOOKAHEAD_DAYS, TO_DATE($cutover_date))
        AND warehouse_name LIKE $warehouse_like
),
gen1_errors AS (
    SELECT
        execution_status,
        COUNT(*) AS error_count,
        COUNT(DISTINCT activity_date) AS days_with_errors
    FROM GEN2_STAGING_QUERY_HISTORY
    WHERE activity_date < TO_DATE($cutover_date)
        -- AND activity_date >=  DATE_TRUNC('week', DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date)))
        AND activity_date >= DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date))
        AND execution_status != 'SUCCESS'
        AND warehouse_name LIKE $warehouse_like
    GROUP BY execution_status
),
gen2_errors AS (
    SELECT
        execution_status,
        COUNT(*) AS error_count,
        COUNT(DISTINCT activity_date) AS days_with_errors,
        MIN(activity_date) AS first_seen,
        MAX(activity_date) AS last_seen,
        ANY_VALUE(LEFT(query_text, 200)) AS sample_query
    FROM GEN2_STAGING_QUERY_HISTORY
    WHERE activity_date >= TO_DATE($cutover_date)
        AND activity_date <= DATEADD(day, +$LOOKAHEAD_DAYS, TO_DATE($cutover_date))
        AND execution_status != 'SUCCESS'
        AND warehouse_name LIKE $warehouse_like
    GROUP BY execution_status
)
SELECT
    g.execution_status,

    -- Absolute counts
    COALESCE(b.error_count, 0) AS gen1_error_count,
    g.error_count AS gen2_error_count,

    -- PER-QUERY rates (matches VW_GEN2_RELIABILITY denominator)
    ROUND(COALESCE(b.error_count, 0) * 1000.0 / bt.total_queries, 4) AS gen1_errors_per_1000_queries,
    ROUND(g.error_count * 1000.0 / gt.total_queries, 4) AS gen2_errors_per_1000_queries,
    ROUND((g.error_count * 1.0 / gt.total_queries - COALESCE(b.error_count, 0) * 1.0 / bt.total_queries)
        / NULLIF(COALESCE(b.error_count, 0) * 1.0 / bt.total_queries, 0) * 100, 1) AS per_query_rate_change_pct,

    -- DAILY rates (for temporal analysis - separate from per-query comparison)
    ROUND(COALESCE(b.error_count, 0) * 1.0 / bt.total_days, 2) AS gen1_errors_per_day,
    ROUND(g.error_count * 1.0 / gt.total_days, 2) AS gen2_errors_per_day,

    -- Context: query volume shows if workload intensity changed
    bt.total_queries AS gen1_total_queries,
    gt.total_queries AS gen2_total_queries,
    ROUND(gt.total_queries * 1.0 / gt.total_days, 1) AS gen2_queries_per_day,
    ROUND(bt.total_queries * 1.0 / bt.total_days, 1) AS gen1_queries_per_day,

    g.first_seen,
    g.last_seen,
    g.sample_query,

    -- Classification based on PER-QUERY rate (consistent with VW_GEN2_RELIABILITY)
    -- Thresholds lowered to align with statistical significance at large sample sizes
    CASE
        WHEN b.execution_status IS NULL THEN 'NEW IN GEN2'
        WHEN g.error_count * 1.0 / gt.total_queries > COALESCE(b.error_count, 0) * 1.0 / bt.total_queries * 2.0
            THEN 'RATE DOUBLED (per query)'
        WHEN g.error_count * 1.0 / gt.total_queries > COALESCE(b.error_count, 0) * 1.0 / bt.total_queries * 1.5
            THEN 'RATE INCREASED 50%+ (per query)'
        WHEN g.error_count * 1.0 / gt.total_queries > COALESCE(b.error_count, 0) * 1.0 / bt.total_queries * 1.2
            THEN 'RATE INCREASED 20%+ (per query)'
        WHEN g.error_count * 1.0 / gt.total_queries > COALESCE(b.error_count, 0) * 1.0 / bt.total_queries * 1.15
            THEN 'RATE INCREASED 15%+ (per query)'
        WHEN g.error_count * 1.0 / gt.total_queries > COALESCE(b.error_count, 0) * 1.0 / bt.total_queries * 1.10
            THEN 'RATE INCREASED 10%+ (per query)'
        WHEN g.error_count * 1.0 / gt.total_queries > COALESCE(b.error_count, 0) * 1.0 / bt.total_queries * 1.05
            THEN 'RATE INCREASED 5%+ (per query)'
        WHEN g.error_count * 1.0 / gt.total_queries < COALESCE(b.error_count, 0) * 1.0 / bt.total_queries * 0.95
            THEN 'RATE DECREASED 5%+ (per query)'
        ELSE 'STABLE (per query)'
    END AS error_trend,

    CASE
        WHEN b.execution_status IS NULL
            THEN 'NEW: This error type did not occur during the GEN1 period. It is new to Gen2 and may indicate a compatibility issue.'
        WHEN g.error_count * 1.0 / gt.total_queries > COALESCE(b.error_count, 0) * 1.0 / bt.total_queries * 2.0
            THEN 'DOUBLED: The per-query occurrence rate of this error has more than doubled on Gen2. This is likely contributing to the reliability degradation flagged in VW_GEN2_RELIABILITY.'
        WHEN g.error_count * 1.0 / gt.total_queries > COALESCE(b.error_count, 0) * 1.0 / bt.total_queries * 1.5
            THEN 'INCREASED 50%+: Per-query occurrence rate increased significantly. This is likely contributing to reliability degradation. Monitor as Gen2 stabilizes.'
        WHEN g.error_count * 1.0 / gt.total_queries > COALESCE(b.error_count, 0) * 1.0 / bt.total_queries * 1.2
            THEN 'INCREASED 20%+: Substantial increase in per-query error rate. With large query volumes, this is likely statistically significant and contributing to the reliability signal in VW_GEN2_RELIABILITY.'
        WHEN g.error_count * 1.0 / gt.total_queries > COALESCE(b.error_count, 0) * 1.0 / bt.total_queries * 1.15
            THEN 'INCREASED 15%+: Moderate increase in per-query error rate. With large sample sizes (>1M queries), this magnitude of change is typically statistically significant.'
        WHEN g.error_count * 1.0 / gt.total_queries > COALESCE(b.error_count, 0) * 1.0 / bt.total_queries * 1.10
            THEN 'INCREASED 10%+: Notable increase in per-query error rate. With large sample sizes, a 10% increase often reaches statistical significance and warrants investigation.'
        WHEN g.error_count * 1.0 / gt.total_queries > COALESCE(b.error_count, 0) * 1.0 / bt.total_queries * 1.05
            THEN 'INCREASED 5%+: Small but measurable increase in per-query error rate. With very large sample sizes (>10M queries), even this change can be statistically significant.'
        WHEN g.error_count * 1.0 / gt.total_queries < COALESCE(b.error_count, 0) * 1.0 / bt.total_queries * 0.95
            THEN 'DECREASED 5%+: This error type occurs less frequently per query on Gen2. Gen2 may have resolved an underlying issue.'
        ELSE 'STABLE: Error rate per query changed by less than ±5%. No Gen2-specific concern for this error type.'
    END AS error_trend_explanation

FROM gen2_errors g
LEFT JOIN gen1_errors b ON g.execution_status = b.execution_status
CROSS JOIN (SELECT * FROM period_totals WHERE period_type = 'GEN1') bt
CROSS JOIN (SELECT * FROM period_totals WHERE period_type = 'GEN2') gt
ORDER BY gen2_errors_per_1000_queries DESC;


-- ============================================================================
-- SECTION 5: PATTERN-LEVEL REGRESSION DETECTION
-- ============================================================================
-- Identifies the top query patterns that got slower or faster on Gen2.
-- Weighted by execution frequency for business impact ranking.

CREATE OR REPLACE VIEW VW_GEN2_PATTERN_REGRESSIONS AS
WITH gen1_patterns AS (
    SELECT
        query_parameterized_hash AS pattern_hash,
        COUNT(*) AS gen1_count,
        ROUND(MEDIAN(execution_time / 1000), 4) AS gen1_median_sec,
        ROUND(PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY execution_time / 1000), 4) AS gen1_05_sec,
        ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY execution_time / 1000), 4) AS gen1_p95_sec,
        ANY_VALUE(LEFT(query_text, 200)) AS sample_query,
        ANY_VALUE(warehouse_name) AS sample_warehouse
    FROM GEN2_STAGING_QUERY_HISTORY
    WHERE query_parameterized_hash IS NOT NULL
        -- AND activity_date >=  DATE_TRUNC('week', DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date)))
        AND activity_date >= DATEADD(day, -$LOOKBACK_DAYS, TO_DATE($cutover_date))
        AND activity_date < TO_DATE($cutover_date)
        AND warehouse_name LIKE $warehouse_like
        AND bytes_scanned > 0  -- Exclude result-cached queries (execution_time=0, no compute)
        -- exclude failed queries to focus on performance of successful executions (can analyze failures separately in VW_GEN2_ERROR_ANALYSIS)
         AND execution_status = 'SUCCESS'
    GROUP BY 1
),
gen2_patterns AS (
    SELECT
        query_parameterized_hash AS pattern_hash,
        COUNT(*) AS gen2_count,
        ROUND(MEDIAN(execution_time / 1000), 4) AS gen2_median_sec,
        ROUND(PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY execution_time / 1000), 4) AS gen2_05_sec,
        ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY execution_time / 1000), 4) AS gen2_p95_sec
    FROM GEN2_STAGING_QUERY_HISTORY
    WHERE query_parameterized_hash IS NOT NULL
        AND activity_date >= TO_DATE($cutover_date)
        AND activity_date <= DATEADD(day, +$LOOKAHEAD_DAYS, TO_DATE($cutover_date))
        AND warehouse_name LIKE $warehouse_like
        AND bytes_scanned > 0  -- Exclude result-cached queries (execution_time=0, no compute)
        -- exclude failed queries to focus on performance of successful executions (can analyze failures separately in VW_GEN2_ERROR_ANALYSIS)
         AND execution_status = 'SUCCESS'
    GROUP BY 1
)
SELECT
    b.pattern_hash,
    b.sample_query,
    b.sample_warehouse,
    b.gen1_count,
    g.gen2_count,
    b.gen1_median_sec,
    g.gen2_median_sec,
    ROUND((1 - g.gen2_median_sec / NULLIF(b.gen1_median_sec, 0)) * 100, 1) AS speedup_pct,
    b.gen1_p95_sec,
    g.gen2_p95_sec,
    b.gen1_05_sec,
    g.gen2_05_sec,
    ROUND((1 - g.gen2_p95_sec / NULLIF(b.gen1_p95_sec, 0)) * 100, 1) AS p95_speedup_pct,
    -- Impact score: frequency * absolute time change
    ROUND(g.gen2_count * ABS(g.gen2_median_sec - b.gen1_median_sec), 2) AS impact_score,
    CASE
        WHEN g.gen2_median_sec > b.gen1_median_sec * 1.20 THEN 'REGRESSION'
        WHEN g.gen2_median_sec < b.gen1_median_sec * 0.80 THEN 'IMPROVEMENT'
        ELSE 'STABLE'
    END AS status,
    CASE
        WHEN g.gen2_median_sec > b.gen1_median_sec * 1.20
            THEN 'This query pattern is >20% slower on Gen2 (median execution time). Higher impact_score means this regression affects more total compute time. Investigate query plan changes, cache behavior, or data scanning differences. Consider warehouse sizing adjustments.'
        WHEN g.gen2_median_sec < b.gen1_median_sec * 0.80
            THEN 'This query pattern is >20% faster on Gen2. Gen2 architecture is benefiting this workload. Higher impact_score means this improvement saves more total compute time across all executions.'
        ELSE 'This query pattern performs within +/-20% of its Gen1. The difference is within normal variation and does not indicate a meaningful change from the migration.'
    END AS status_explanation
FROM gen1_patterns b
INNER JOIN gen2_patterns g ON b.pattern_hash = g.pattern_hash
WHERE b.gen1_count >= 10 AND g.gen2_count >= 10  -- Only patterns with meaningful sample in both periods
ORDER BY impact_score DESC;


-- ============================================================================
-- SECTION 6: DAILY TREND (for visualization / dashboards)
-- ============================================================================
-- Simple daily time-series combining performance and cost.
-- Performance metrics (median, p95, success_rate) are from MATCHED/COMMON patterns only.
-- Cost metrics (credits, credits_per_query) are from ALL warehouse activity (no pattern separation).

CREATE OR REPLACE VIEW VW_GEN2_DAILY_TREND AS
WITH perf AS (
    SELECT
        activity_date,
        period_type,
        day_of_week,
        query_count,
        median_exec_sec,
        p95_exec_sec,
        success_rate_pct
    FROM VW_GEN2_PERF_DAILY
),
cost AS (
    SELECT
        activity_date,
        period_type,
        daily_credits,
        all_query_count,
        all_credits_per_query,
        pattern_query_count,
        wh_credits_per_pattern_query
    FROM VW_GEN2_COST_DAILY
)
SELECT
    c.activity_date,
    c.period_type,
    p.day_of_week,
    -- Performance: matched/common patterns only (excludes result-cached queries)
    -- NULL on days where all matched queries hit the result cache
    p.query_count AS perf_pattern_query_count,
    p.median_exec_sec AS pattern_median_exec_sec,
    p.p95_exec_sec AS pattern_p95_exec_sec,
    p.success_rate_pct AS pattern_success_rate_pct,
    -- Cost: all warehouse activity (no pattern-level separation in credits)
    c.all_query_count,
    c.daily_credits AS all_warehouse_daily_credits,
    c.all_credits_per_query AS all_warehouse_credits_per_query,
    -- Cost per pattern query (warehouse credits / pattern query count)
    c.pattern_query_count AS cost_pattern_query_count,
    c.wh_credits_per_pattern_query AS wh_credits_per_pattern_query
FROM cost c
LEFT JOIN perf p ON c.activity_date = p.activity_date AND c.period_type = p.period_type
ORDER BY c.activity_date;


-- ============================================================================
-- SECTION 7: ASSESSMENT
-- ============================================================================
-- Combines performance, cost, and reliability into a single assessment.
-- Assessment is based on confidence intervals on BOTH axes:
--   FAVORABLE: speedup CI LOWER bound must exceed threshold (worst-case speed still good)
--   FAVORABLE: cost CI UPPER bound must be within threshold (worst-case cost still acceptable)
--   UNFAVORABLE: cost CI LOWER bound must exceed threshold (best-case cost still too high)
-- This ensures assessments are robust to statistical uncertainty in both dimensions.
--
-- WHY COST USES ALL-QUERY SCOPE (not pattern-query scope):
--
-- Snowflake does not track credits per query. WAREHOUSE_METERING_HISTORY reports
-- total credits per warehouse per hour. Every "cost per query" metric is an
-- allocation model, not a measurement. Three approaches were considered:
--
--   Approach          | Numerator         | Denominator       | Weakness
--   --------------------|-------------------|-------------------|-----------------------------
--   All-query (chosen)| All credits       | All queries       | Workload composition changes
--                     |                   |                   | affect it, but numerator and denominator are
--                     |                   |                   | consistent (same scope).
--   Pattern-query     | All credits       | Pattern queries   | SCOPE MISMATCH: unmatched
--   (previous)        |                   |                   | workload inflates numerator without
--                     |                   |                   | matching denominator. Contaminated.
--   Proportional      | Credits × share   | Pattern queries   | Assumes cost ∝ exec time.
--   allocation        | of exec time      |                   | Ignores idle time, concurrency.
--
-- All-query is the least wrong for the assessment because:
--   1. The question is "should we migrate this warehouse?" — a warehouse-level question.
--   2. All credits / all queries answers "did the average unit of work get cheaper?"
--   3. Workload composition changes affect both numerator and denominator, so they partially cancel out.
--
-- The pattern-query metric is still output as context (pattern_cost_change_pct).
-- If coverage is >90% (check pattern_coverage_assessment), both metrics converge
-- and the choice of scope doesn't matter.
--

CREATE OR REPLACE VIEW VW_GEN2_MIGRATION_DECISION AS
WITH perf AS (SELECT * FROM VW_GEN2_PERF_SUMMARY),
cost AS (SELECT * FROM VW_GEN2_COST_WELCH_TEST),
reliability AS (SELECT * FROM VW_GEN2_RELIABILITY),
validation AS (
    SELECT
        MAX(CASE WHEN period_type = 'GEN2' THEN query_days END) AS gen2_days,
        MAX(CASE WHEN period_type = 'GEN1' THEN query_days END) AS gen1_days,
        MAX(CASE WHEN period_type = 'GEN1' THEN total_queries END) AS gen1_total_queries,
        MAX(CASE WHEN period_type = 'GEN2' THEN total_queries END) AS gen2_total_queries,
        MAX(CASE WHEN period_type = 'GEN1' THEN pattern_coverage_pct END) AS gen1_pattern_coverage_pct,
        MAX(CASE WHEN period_type = 'GEN2' THEN pattern_coverage_pct END) AS gen2_pattern_coverage_pct
    FROM VW_GEN2_DATA_VALIDATION
)
SELECT
    -- Configuration
    $warehouse_like AS domain_pattern,
    TO_DATE($cutover_date) AS cutover_date,

    -- Data sufficiency
    v.gen1_days,
    v.gen2_days,
    CASE
        WHEN v.gen2_days < 7 THEN 'CRITICAL: < 7 days'
        WHEN v.gen2_days < 14 THEN 'PRELIMINARY: < 14 days'
        WHEN v.gen2_days < 21 THEN 'ADEQUATE: 14-20 days'
        ELSE 'STRONG: 21+ days'
    END AS data_quality,
    CASE
        WHEN v.gen2_days < 7
            THEN 'Fewer than 7 Gen2 days collected. No day-of-week has more than 1 sample. Statistical tests are unreliable — the assessment will show INSUFFICIENT DATA. Keep collecting data.'
        WHEN v.gen2_days < 14
            THEN 'Between 7-13 Gen2 days. Most days-of-week have 1-2 samples. Welch t-test per-DOW is unreliable (df ~1-3). Overall test is preliminary. Assessments at this stage carry high uncertainty.'
        WHEN v.gen2_days < 21
            THEN 'Between 14-20 Gen2 days. Each DOW has 2-3 samples. The overall Welch t-test is usable and confidence intervals are meaningful. This is the minimum recommended for a reliable assessment.'
        ELSE 'At least 21 Gen2 days (3+ full weeks). The overall Welch t-test has strong power. High confidence in the assessment. DOW-stratified exploratory analysis requires 28+ days (4+ samples per DOW).'
    END AS data_quality_explanation,

    -- Pattern coverage: what % of total queries are matched patterns (present in both periods)?
    -- High coverage (>90%) means performance and cost metrics are measuring the same workload.
    -- Low coverage means significant unmatched activity exists — cost and performance scopes diverge.
    v.gen1_pattern_coverage_pct,
    v.gen2_pattern_coverage_pct,
    CASE
        WHEN LEAST(COALESCE(v.gen1_pattern_coverage_pct, 0), COALESCE(v.gen2_pattern_coverage_pct, 0)) >= 90
            THEN 'HIGH (>90%): Pattern and all-query metrics converge. Cost assessment is reliable.'
        WHEN LEAST(COALESCE(v.gen1_pattern_coverage_pct, 0), COALESCE(v.gen2_pattern_coverage_pct, 0)) >= 70
            THEN 'MODERATE (70-90%): Some unmatched activity. Cost assessment is directionally reliable but check pattern_cost_change_pct for divergence.'
        ELSE 'LOW (<70%): Significant unmatched workload. Cost assessment (all-query) may be influenced by non-pattern activity. Compare cost_change_pct vs pattern_cost_change_pct — if they diverge, investigate workload composition changes.'
    END AS pattern_coverage_assessment,

    -- Performance
    -- speedup_pct: (1 - Gen2/Gen1) × 100. Positive = Gen2 faster.
    -- Derived from Welch's t-test on daily median execution times (one observation per day).
    -- Each day's median is computed from raw execution_time (ms→sec) of matched patterns
    -- (query_parameterized_hash present in both periods, execution_status = 'SUCCESS',
    -- bytes_scanned > 0). Failed queries excluded — they have unpredictable execution times.
    -- The t-test compares AVG(daily_medians) between periods — equal weight per day.
    p.speedup_pct,
    p.speedup_ci_lower_90 AS speedup_lower_bound,
    p.speedup_ci_upper_90 AS speedup_upper_bound,
    p.significance AS perf_significance,
    -- tail_latency_check: compares mean of daily P99 execution times between periods.
    -- WARNING if Gen2 P99 > 2× Gen1, CAUTION if > 1.5×, IMPROVED if ≤ 0.8×.
    p.tail_latency_check,
    -- queue_saturation_check: compares mean of daily P95 queue times.
    -- Detects concurrency saturation or slow warehouse resume on Gen2.
    p.queue_saturation_check,

    -- Absolute execution time context: raw medians + diff for practical significance.
    -- Percentage speedup can mislead on sub-second queries where fixed overhead
    -- (compilation, scheduling, serialization ~200-500ms) dominates compute time.
    -- Both medians must be ≥ 1s for percentage speedup to be meaningful.
    p.gen1_daily_median_sec,
    p.gen2_daily_median_sec,
    ROUND(p.gen1_daily_median_sec - p.gen2_daily_median_sec, 4) AS median_diff_sec,
    -- Average daily query count (Gen2 period) — used for practical significance assessment.
    -- High-volume workloads (≥1000 queries/day) can compound small per-query savings into
    -- meaningful aggregate compute reduction, even when individual differences are < 2 seconds.
    ROUND(v.gen2_total_queries / NULLIF(v.gen2_days, 0), 0) AS gen2_avg_daily_queries,

    -- Cost: all-query scope (CI bounds drive the assessment)
    -- cost_change_pct: point estimate. cost_change_lower/upper_bound: 90% CI.
    -- Assessment uses CI bounds: FAVORABLE requires upper bound acceptable, UNFAVORABLE requires lower bound unacceptable.
    -- Derived from Welch's t-test on daily (total_warehouse_credits / all_non_cached_queries).
    -- Numerator and denominator have consistent scope (both cover all warehouse activity).
    -- The t-test compares AVG(daily_ratios) between periods — equal weight per day.
    cost.all_avg_cpq_change_pct AS cost_change_pct,
    cost.all_avg_cpq_ci_lower_90 AS cost_change_lower_bound,
    cost.all_avg_cpq_ci_upper_90 AS cost_change_upper_bound,
    cost.significance AS cost_significance,
    cost.breakeven_status,
    -- Pattern-query scope: diagnostic metric (not used for assessment).
    -- Numerator is ALL warehouse credits, denominator is ONLY pattern queries.
    -- Unmatched workload shifts can inflate/deflate this metric even when matched queries
    -- are unchanged. Compare with cost_change_pct — if they diverge, check pattern_coverage_assessment.
    cost.avg_cost_per_pattern_query_change_pct AS pattern_cost_change_pct,
    cost.avg_cost_per_pattern_query_ci_lower_90 AS pattern_cost_change_lower_bound,
    cost.avg_cost_per_pattern_query_ci_upper_90 AS pattern_cost_change_upper_bound,
    cost.pattern_cost_significance,

    -- Reliability
    -- success_rate: SUCCESS query count / total query count (all queries, both matched and unmatched).
    -- success_rate_diff_pp: Gen2 rate - Gen1 rate in percentage points. Negative = Gen2 worse.
    -- Tested via Welch's t-test on daily success rates (one observation per day).
    -- DEGRADED (large effect) → UNFAVORABLE. MINOR DEGRADATION (small effect) → MIXED.
    r.gen1_success_rate,
    r.gen2_success_rate,
    r.success_rate_diff_pp,
    r.reliability_status,

    -- ASSESSMENT: CI-based on both axes (all-query cost scope).
    -- This framework presents data signals — it does not dictate decisions.
    -- Stakeholders weigh business context that the framework cannot see.
    --
    -- Cascade: INSUFFICIENT DATA → UNFAVORABLE (reliability) → MIXED (minor reliability)
    --   → MIXED (tail latency degraded) → MIXED (queue saturation) → PRELIMINARY (7-13 days)
    --   → INCONCLUSIVE (sub-second workload, < 1k queries/day)
    --   → STRONGLY FAVORABLE → FAVORABLE (cost neutral, with perf guard ≥ -20%)
    --   → MIXED (cost neutral + perf degraded) → FAVORABLE (near break-even + speedup)
    --   → FAVORABLE (strong speedup offsets cost) → MIXED (high absolute cost)
    --   → FAVORABLE (tail latency improved + cost acceptable)
    --   → UNFAVORABLE (CI-based) → MIXED
    CASE
        -- Insufficient data
        WHEN v.gen2_days < 7
            THEN 'INSUFFICIENT DATA'
        -- Reliability degradation detected
        WHEN r.reliability_status LIKE 'DEGRADED%'
            THEN 'UNFAVORABLE - Reliability degraded'
        -- Minor reliability degradation — warrants investigation at scale
        WHEN r.reliability_status LIKE 'MINOR DEGRADATION%'
            THEN 'MIXED - Minor reliability concern'
        -- Tail latency degraded
        WHEN p.tail_latency_check LIKE 'WARNING%'
            THEN 'MIXED - Tail latency degraded'
        -- Queue saturation: queries waiting significantly longer before execution
        WHEN p.queue_saturation_check LIKE 'WARNING%'
            THEN 'MIXED - Queue saturation detected'
        -- Preliminary data (7-13 days) - early signals only
        WHEN v.gen2_days < 14
            THEN CASE
                WHEN cost.all_avg_cpq_change_pct < -10 AND p.speedup_pct > 0
                    THEN 'EARLY POSITIVE - Collect more data to confirm'
                WHEN cost.all_avg_cpq_change_pct > 20
                    THEN 'EARLY NEGATIVE - Cost spike detected'
                ELSE 'INSUFFICIENT DATA - Need 14+ days for reliable assessment'
            END
        -- Sufficient data (14+ days) - CI-based assessment
        -- FAVORABLE uses cost CI UPPER bound (worst plausible cost must still be acceptable)
        -- UNFAVORABLE uses cost CI LOWER bound (best plausible cost must still be unacceptable)
        --
        -- Practical significance guard: sub-second median execution time means
        -- percentage speedup is dominated by fixed overhead, not compute.
        -- High-volume workloads (≥1000 queries/day) are exempt.
        WHEN (p.gen1_daily_median_sec < 1 OR p.gen2_daily_median_sec < 1)
            AND COALESCE(v.gen2_total_queries / NULLIF(v.gen2_days, 0), 0) < 1000
            THEN 'INCONCLUSIVE - Sub-second workload'
        -- Worst-case cost at or below Gen1 AND strong speedup
        WHEN cost.all_avg_cpq_ci_upper_90 <= 0 AND p.speedup_ci_lower_90 >= 35
            THEN 'STRONGLY FAVORABLE'
        -- Worst-case cost at or below Gen1, no severe perf degradation
        WHEN cost.all_avg_cpq_ci_upper_90 <= 0 AND p.speedup_pct >= -20
            THEN 'FAVORABLE - Cost neutral'
        -- Cost breaks even but queries are significantly slower (>20%)
        WHEN cost.all_avg_cpq_ci_upper_90 <= 0
            THEN 'MIXED - Cost neutral but significantly slower'
        -- Worst-case cost within 5% and significantly faster
        WHEN cost.all_avg_cpq_ci_upper_90 <= 5 AND p.speedup_ci_lower_90 >= 26
            THEN 'FAVORABLE - Near break-even with strong speedup'
        -- Strong speedup (worst-case ≥35%) with moderate cost increase (worst-case ≤15%)
        WHEN cost.all_avg_cpq_ci_upper_90 <= 15 AND p.speedup_ci_lower_90 >= 35
            THEN 'FAVORABLE - Strong speedup offsets cost premium'
        -- High-spend guard-rail: absolute dollar impact warrants attention.
        -- Uses CI lower bound (best-case cost still >5%) for consistency with all other branches.
        WHEN cost.gen2_daily_credits > 50 AND cost.all_avg_cpq_ci_lower_90 > 5
            THEN 'MIXED - High absolute cost impact'
        -- Tail latency significantly improved with acceptable cost — the benefit
        -- is in the tail (P95/P99), not the median. Material even when median barely moves.
        -- 10% cost threshold is wider than break-even GO because dramatic tail improvement
        -- (P99 down ≥20%) is strong evidence Gen2 benefits this workload's worst cases.
        WHEN p.tail_latency_check LIKE 'IMPROVED%'
            AND cost.all_avg_cpq_ci_upper_90 <= 10
            AND p.speedup_pct >= 0
            THEN 'FAVORABLE - Tail latency improved'
        -- Best-case cost still >15% higher with no meaningful speedup
        WHEN cost.all_avg_cpq_ci_lower_90 > 15 AND p.speedup_pct < 20
            THEN 'UNFAVORABLE - Cost increase without sufficient speedup'
        -- Best-case cost still >10% higher (statistically confirmed) with weak speedup
        WHEN cost.all_avg_cpq_ci_lower_90 > 10 AND cost.significance LIKE 'SIGNIFICANT%'
            AND p.speedup_pct < 20
            THEN 'UNFAVORABLE - Statistically confirmed cost increase'
        -- Even optimistic speedup CI < 20% and best-case cost > 5%
        WHEN p.speedup_ci_upper_90 < 20 AND cost.all_avg_cpq_ci_lower_90 > 5
            THEN 'UNFAVORABLE - Neither speedup nor cost justify migration'
        -- Mixed signals
        ELSE 'MIXED - Review detailed metrics'
    END AS assessment,
    CASE
        WHEN v.gen2_days < 7
            THEN 'Fewer than 7 days of Gen2 data collected. Statistical tests are unreliable at this stage.'
        WHEN r.reliability_status LIKE 'DEGRADED%'
            THEN 'Gen2 shows a statistically significant increase in query failures. This is the strongest negative signal in the framework. See VW_GEN2_ERROR_ANALYSIS for new and increased error types.'
        WHEN r.reliability_status LIKE 'MINOR DEGRADATION%'
            THEN 'Gen2 shows a statistically significant but small-effect reliability degradation (' || r.reliability_status || '). At scale across many warehouses, small degradations compound. See VW_GEN2_ERROR_ANALYSIS for error type breakdown.'
        WHEN p.tail_latency_check LIKE 'WARNING%'
            THEN 'Median performance may be acceptable, but the worst-case (P99) latency has more than doubled. This could impact SLAs, timeout-sensitive jobs, or user experience for the slowest queries.'
        WHEN p.queue_saturation_check LIKE 'WARNING%'
            THEN 'P95 queue time has more than doubled on Gen2. Queries are spending significantly more time waiting before execution begins. This typically indicates concurrency saturation — the warehouse may need different scaling, multi-cluster configuration, or auto-suspend settings on Gen2.'
        WHEN v.gen2_days < 14 AND cost.all_avg_cpq_change_pct < -10 AND p.speedup_pct > 0
            THEN 'Early data (< 14 days) shows positive signals — per-query cost is down >10% and performance is up. More data is needed to confirm.'
        WHEN v.gen2_days < 14 AND cost.all_avg_cpq_change_pct > 20
            THEN 'Early data shows a per-query cost spike (>20%). This may stabilize with more data but warrants close monitoring.'
        WHEN v.gen2_days < 14
            THEN 'Have 7-13 days of data but signals are mixed or neutral. 14+ days are needed for confidence intervals narrow enough for a reliable assessment.'
        WHEN (p.gen1_daily_median_sec < 1 OR p.gen2_daily_median_sec < 1)
            AND COALESCE(v.gen2_total_queries / NULLIF(v.gen2_days, 0), 0) < 1000
            THEN 'Median execution time is sub-second in '
                || CASE WHEN p.gen1_daily_median_sec < 1 AND p.gen2_daily_median_sec < 1 THEN 'both periods'
                       WHEN p.gen1_daily_median_sec < 1 THEN 'Gen1' ELSE 'Gen2' END
                || ' (' || ROUND(p.gen1_daily_median_sec, 2) || 's Gen1 → ' || ROUND(p.gen2_daily_median_sec, 2) || 's Gen2). '
                || 'Sub-second queries are dominated by fixed overhead (compilation, scheduling, serialization ~200-500ms), '
                || 'not compute. The ' || ROUND(p.speedup_pct, 1) || '% speedup is likely noise. '
                || 'At ~' || ROUND(COALESCE(v.gen2_total_queries / NULLIF(v.gen2_days, 0), 0), 0) || ' queries/day, '
                || 'aggregate savings are negligible. Cost and reliability signals are more informative for this workload.'
        WHEN cost.all_avg_cpq_ci_upper_90 <= 0 AND p.speedup_ci_lower_90 >= 35
            THEN 'Even the worst-case (90% CI upper bound) per-query cost is at or below Gen1, and performance speedup lower bound exceeds 35%. Both axes are favorable even under pessimistic assumptions.'
        WHEN cost.all_avg_cpq_ci_upper_90 <= 0 AND p.speedup_pct >= -20
            THEN 'The 90% CI upper bound on per-query cost is at or below Gen1. Gen2 appears to complete work fast enough to offset its higher per-hour rate (1.35x on AWS). The data indicates cost-neutral or cost-saving conditions.'
        WHEN cost.all_avg_cpq_ci_upper_90 <= 0
            THEN 'Cost breaks even (90% CI upper bound ≤ Gen1) but queries are significantly slower (speedup < -20%). While the invoice is neutral, a >20% performance degradation could impact SLAs, timeout-sensitive jobs, and user experience. See VW_GEN2_PATTERN_REGRESSIONS for regressed patterns.'
        WHEN cost.all_avg_cpq_ci_upper_90 <= 5 AND p.speedup_ci_lower_90 >= 26
            THEN 'Worst-case per-query cost (90% CI upper bound) is within 5% of Gen1 and speedup is strong (lower bound of 90% CI ≥ 26%). Even under pessimistic assumptions, the cost premium is modest relative to the performance gain.'
        WHEN cost.all_avg_cpq_ci_upper_90 <= 15 AND p.speedup_ci_lower_90 >= 35
            THEN 'Worst-case speedup (90% CI lower bound) exceeds 35%, indicating Gen2 is substantially faster even under pessimistic assumptions. The worst-case cost increase (90% CI upper bound) is within 15%. At 35%+ speedup, the 1.35x/hr Gen2 premium is more than offset by reduced wall-clock time.'
        WHEN cost.gen2_daily_credits > 50 AND cost.all_avg_cpq_ci_lower_90 > 5
            THEN 'This domain consumes ~' || ROUND(cost.gen2_daily_credits, 0) || ' credits/day. Even the best-case cost (90% CI lower bound: ' || ROUND(cost.all_avg_cpq_ci_lower_90, 1) || '%) is >5% higher. At ' || ROUND(cost.all_avg_cpq_change_pct, 1) || '% point estimate, that translates to ~' || ROUND(cost.gen2_daily_credits * cost.all_avg_cpq_change_pct / 100, 1) || ' extra credits/day (~$' || ROUND(cost.gen2_daily_credits * cost.all_avg_cpq_change_pct / 100 * 3 * 365, 0) || '/year at $3/credit). The absolute dollar impact warrants attention.'
        WHEN p.tail_latency_check LIKE 'IMPROVED%'
            AND cost.all_avg_cpq_ci_upper_90 <= 10
            AND p.speedup_pct >= 0
            THEN 'Tail latency (P99) improved ≥20% on Gen2 while worst-case cost (90% CI upper bound) is within 10% of Gen1. The benefit is in the tail — the slowest queries are completing faster — even though median speedup (' || ROUND(p.speedup_pct, 1) || '%) is modest. Gen2 is doing comparable or more work for a similar cost.'
        WHEN cost.all_avg_cpq_ci_lower_90 > 15 AND p.speedup_pct < 20
            THEN 'Even the best-case (90% CI lower bound) per-query cost is >15% higher than Gen1, and speedup (<20%) is modest. The data rules out a cost-neutral outcome for this domain.'
        WHEN cost.all_avg_cpq_ci_lower_90 > 10 AND cost.significance LIKE 'SIGNIFICANT%' AND p.speedup_pct < 20
            THEN 'Even the best-case per-query cost (90% CI lower bound) is >10% higher (statistically significant), and performance speedup (<20%) is modest. The cost increase is confirmed with high confidence.'
        WHEN p.speedup_ci_upper_90 < 20 AND cost.all_avg_cpq_ci_lower_90 > 5
            THEN 'Even the optimistic end of the speedup 90% CI is below 20%, and even the best-case cost (90% CI lower bound) is >5% higher. The data does not support a favorable outcome for this domain.'
        ELSE 'The metrics give mixed signals — some favorable, some unfavorable, or confidence intervals are too wide to be conclusive. See VW_GEN2_PERF_SUMMARY and VW_GEN2_COST_WELCH_TEST for detailed data.'
    END AS assessment_explanation,

    -- Detailed reasoning
    CASE
        WHEN v.gen2_days < 7 THEN 'Need minimum 7 days of Gen2 data. Currently have ' || v.gen2_days || ' days.'
        WHEN v.gen2_days < 14 THEN 'Have ' || v.gen2_days || ' days. Recommend waiting for 14+ days for confident decision.'
        ELSE 'Based on ' || v.gen2_days || ' days of Gen2 data vs ' || v.gen1_days || ' days gen1. '
            || 'Speedup: ' || COALESCE(TO_VARCHAR(p.speedup_pct), 'N/A') || '% '
            || '[' || COALESCE(TO_VARCHAR(p.speedup_ci_lower_90), '?') || '%, '
            || COALESCE(TO_VARCHAR(p.speedup_ci_upper_90), '?') || '%] (90% CI). '
            || 'Median exec: ' || ROUND(p.gen1_daily_median_sec, 2) || 's → ' || ROUND(p.gen2_daily_median_sec, 2) || 's '
            || '(diff: ' || ROUND(p.gen1_daily_median_sec - p.gen2_daily_median_sec, 2) || 's). '
            || 'Cost change (all-query): ' || COALESCE(TO_VARCHAR(cost.all_avg_cpq_change_pct), 'N/A') || '% '
            || '[' || COALESCE(TO_VARCHAR(cost.all_avg_cpq_ci_lower_90), '?') || '%, '
            || COALESCE(TO_VARCHAR(cost.all_avg_cpq_ci_upper_90), '?') || '%] (90% CI). '
            || 'Pattern-only: ' || COALESCE(TO_VARCHAR(cost.avg_cost_per_pattern_query_change_pct), 'N/A') || '%. '
            || 'Break-even status: ' || cost.breakeven_status || '. '
            || 'Reliability: ' || r.reliability_status || '.'
            || CASE
                WHEN (p.gen1_daily_median_sec < 1 OR p.gen2_daily_median_sec < 1)
                    AND COALESCE(v.gen2_total_queries / NULLIF(v.gen2_days, 0), 0) >= 1000
                    THEN ' NOTE: Sub-second median execution time but high query volume (~'
                        || ROUND(v.gen2_total_queries / NULLIF(v.gen2_days, 0), 0)
                        || '/day) — small per-query savings compound into aggregate compute reduction. '
                        || 'Percentage speedup may still be sensitive to measurement noise.'
                WHEN p.gen1_daily_median_sec < 1 OR p.gen2_daily_median_sec < 1
                    THEN ' NOTE: Sub-second median execution time — percentage speedup may be dominated by fixed overhead noise.'
                ELSE ''
            END
            || CASE
                WHEN v.gen1_total_queries IS NOT NULL AND v.gen2_total_queries IS NOT NULL
                    AND v.gen1_days > 0 AND v.gen2_days > 0
                    AND (v.gen2_total_queries / NULLIF(v.gen2_days, 0))
                        > (v.gen1_total_queries / NULLIF(v.gen1_days, 0)) * 1.10
                    THEN ' NOTE: Daily query volume is up ~'
                        || ROUND(((v.gen2_total_queries / NULLIF(v.gen2_days, 0))
                            / NULLIF(v.gen1_total_queries / NULLIF(v.gen1_days, 0), 0) - 1) * 100, 0)
                        || '% on Gen2 ('
                        || ROUND(v.gen1_total_queries / NULLIF(v.gen1_days, 0), 0) || ' → '
                        || ROUND(v.gen2_total_queries / NULLIF(v.gen2_days, 0), 0)
                        || '/day) — Gen2 is handling more work, which dilutes per-query cost further.'
                ELSE ''
            END
            || CASE
                WHEN cost.gen2_daily_credits > 50 AND cost.all_avg_cpq_change_pct > 5
                    THEN ' NOTE: High-spend domain (~' || ROUND(cost.gen2_daily_credits, 0)
                        || ' credits/day) with ' || ROUND(cost.all_avg_cpq_change_pct, 1)
                        || '% cost increase = ~' || ROUND(cost.gen2_daily_credits * cost.all_avg_cpq_change_pct / 100, 1)
                        || ' extra credits/day (~$' || ROUND(cost.gen2_daily_credits * cost.all_avg_cpq_change_pct / 100 * 3 * 365, 0)
                        || '/year at $3/credit). Stakeholders should weigh this absolute dollar impact.'
                ELSE ''
            END
    END AS reasoning,

    -- Self-contained reference for how each metric is derived and how assessments are produced.
    -- This framework presents data signals — stakeholders make decisions.
    'METHODOLOGY: '
    || 'PERFORMANCE: speedup_pct = (1 - Gen2/Gen1) × 100 from Welch t-test on daily median execution times (matched patterns, execution_status=SUCCESS, bytes_scanned > 0). Positive = Gen2 faster. Failed queries excluded — they have unpredictable execution times. CI bounds = exact 90% confidence interval via Fieller''s theorem on the ratio gen2/gen1 (no delta-method approximation). '
    || 'COST: cost_change_pct = (Gen2 - Gen1) / Gen1 × 100 from Welch t-test on daily (total_warehouse_credits / all_non_cached_queries). Negative = Gen2 cheaper. Numerator and denominator have consistent scope. pattern_cost_change_pct (diagnostic) = same method but denominator is pattern queries only — unmatched workload shifts can distort it. Compare with cost_change_pct; if they diverge, check pattern_coverage_assessment. '
    || 'RELIABILITY: success_rate = SUCCESS queries / total queries (all queries, not just patterns). Tested via Welch t-test on daily success rates (one observation per day) — this naturally accounts for within-day query clustering that inflates query-level z-tests. DEGRADED requires statistical significance (p<0.05) AND practical significance (>=0.5pp drop or >=25% failure rate increase). MINOR DEGRADATION = stat. significant but small effect. '
    || 'ASSESSMENT LOGIC: CI-based on both axes. FAVORABLE requires cost CI UPPER bound acceptable (worst plausible cost still OK). UNFAVORABLE requires cost CI LOWER bound unacceptable (best plausible cost still bad). Performance degradation guard: FAVORABLE cost-neutral requires speedup >= -20%. Strong-speedup FAVORABLE: if speedup CI lower >= 35%, cost up to CI upper 15% is considered offset by reduced wall-clock time. Tail latency positive signal: if P99 improved >=20% (IMPROVED) AND cost CI upper <= 5% AND speedup >= 0%, the benefit is in the tail even if median is modest. Practical significance guard: if either period''s median execution time is < 1 second AND < 1000 queries/day, percentage speedup is unreliable (sub-second queries dominated by fixed overhead). High-volume workloads (>= 1000/day) are exempt. Cascade priority: INSUFFICIENT DATA → UNFAVORABLE (reliability DEGRADED) → MIXED (minor reliability) → MIXED (tail latency degraded P99 > 2x) → MIXED (queue saturation P95 queue > 2x) → preliminary 7-13 days → INCONCLUSIVE (sub-second + < 1k/day) → STRONGLY FAVORABLE (cost upper ≤ 0% + speedup lower ≥ 35%) → FAVORABLE cost neutral (cost upper ≤ 0% + speedup ≥ -20%) → MIXED cost neutral + slower (cost upper ≤ 0% + speedup < -20%) → FAVORABLE near-break-even (cost upper ≤ 5% + speedup lower ≥ 26%) → FAVORABLE strong-speedup (cost upper ≤ 15% + speedup lower ≥ 35%) → MIXED high-spend (>50 credits/day + CI lower >5%) → FAVORABLE tail-improved (P99 IMPROVED + cost upper ≤ 10% + speedup ≥ 0%) → UNFAVORABLE (CI-based) → MIXED. '
    || 'SHARED: All t-tests use AVG of daily values (equal weight per day, one observation per day). Matched patterns = query_parameterized_hash values present in BOTH periods (INTERSECT). Result-cached queries (bytes_scanned = 0) excluded from performance and cost-per-query metrics.'
    AS metric_methodology,

    CURRENT_TIMESTAMP() AS last_updated

FROM perf p
CROSS JOIN cost
CROSS JOIN reliability r
CROSS JOIN validation v;


-- ============================================================================
-- USAGE
-- ============================================================================

/*
QUICK START:
  1. Run gen2_staging_tables.sql (full refresh first time, incremental after)
  2. SET cutover_date = '2026-05-01';
     SET warehouse_like = 'ANALYTICS_%';
  3. Run this file to create all views
  4. Query the decision:

    SELECT * FROM VW_GEN2_MIGRATION_DECISION;

DETAILED ANALYSIS:

  -- Data quality check
  SELECT * FROM VW_GEN2_DATA_VALIDATION ;

  -- Overall performance summary
  SELECT * FROM VW_GEN2_PERF_SUMMARY;

  -- Cost analysis
  SELECT * FROM VW_GEN2_COST_WELCH_TEST;

  -- Performance by day-of-week
  SELECT * FROM VW_GEN2_PERF_WELCH_TEST;
  -- Cost by day-of-week
  SELECT * FROM VW_GEN2_COST_BY_DOW;

  -- Performance by day-of-week (matched patterns)
  SELECT * FROM VW_GEN2_PERF_BY_DOW;

  -- Reliability
  SELECT * FROM VW_GEN2_RELIABILITY;

  -- New error patterns
  SELECT * FROM VW_GEN2_ERROR_ANALYSIS;

  -- Top regressions and improvements
  SELECT * FROM VW_GEN2_PATTERN_REGRESSIONS WHERE status = 'REGRESSION' ORDER BY impact_score DESC LIMIT 20;
  SELECT * FROM VW_GEN2_PATTERN_REGRESSIONS WHERE status = 'IMPROVEMENT' ORDER BY impact_score DESC LIMIT 20;


  -- Daily trend for dashboard charts
  SELECT * FROM VW_GEN2_DAILY_TREND order by activity_date desc; -- complimentary view
  select * from VW_GEN2_PERF_DAILY order by activity_date desc;
  select * from VW_GEN2_COST_DAILY order by activity_date desc;

VIEWS CREATED (in dependency order):
  1. VW_GEN2_DATA_VALIDATION     - Sanity checks and data sufficiency
  2. VW_GEN2_PERF_DAILY          - Daily performance metrics (matched patterns)
  3. VW_GEN2_PERF_WELCH_TEST     - Welch's t-test by day-of-week
  4. VW_GEN2_PERF_SUMMARY        - Overall performance with CI
  5. VW_GEN2_COST_DAILY          - Daily credit cost (Gen2 rates already in metering)
  6. VW_GEN2_COST_WELCH_TEST     - Cost Welch's t-test with break-even check
  7. VW_GEN2_COST_BY_DOW         - Cost breakdown by day-of-week
  8. VW_GEN2_RELIABILITY         - Success rate Welch's t-test on daily rates
  9. VW_GEN2_ERROR_ANALYSIS    - Error analysis: new types + increased frequency
  10. VW_GEN2_PATTERN_REGRESSIONS - Per-pattern regression detection
  11. VW_GEN2_DAILY_TREND         - Combined daily trend for dashboards
  12. VW_GEN2_MIGRATION_DECISION  - Final assessment for stakeholder decision
  13. VW_GEN2_PERF_BY_DOW       - Performance by day-of-week (matched patterns)

STATISTICAL METHODOLOGY:
  - Performance: Welch's t-test on daily median execution times (matched patterns)
  - Cost: Welch's t-test on daily credits-per-query (volume-normalized efficiency)
  - Reliability: Welch's t-test on daily success rates (handles query clustering)
  - Decision: Based on 90% confidence interval lower bound and credit cost comparison
  - Break-even: Gen2 credits-per-query <= Gen1 (Gen2 efficient enough to offset higher rate)

WHY WELCH'S T-TEST:
  - Used for ALL three axes: performance (daily medians), cost (daily credits-per-query),
    and reliability (daily success rates). Unified methodology.
  - Handles unequal sample sizes (Gen1 ~35 days vs Gen2 growing) and variances correctly
  - Critical values are looked up by Welch-Satterthwaite degrees of freedom
  - For reliability: day-level aggregation naturally handles within-day query clustering
    that inflates query-level z-tests. n = days (not queries).

ASSESSMENT RULES (credit-cost based, CI bounds):
  STRONGLY FAVORABLE:  Cost CI upper <= 0% AND speedup CI lower >= 35%
  FAVORABLE:           Cost CI upper <= 0% AND speedup >= -20% (cost neutral)
  FAVORABLE:           Cost CI upper <= 5% AND speedup CI lower >= 26%
  FAVORABLE:           Cost CI upper <= 15% AND speedup CI lower >= 35%
  FAVORABLE:           Tail latency IMPROVED + cost CI upper <= 10% + speedup >= 0%
  UNFAVORABLE:         Cost CI lower > 15% with speedup < 20%, OR reliability DEGRADED
  UNFAVORABLE:         Cost CI lower > 10% (stat sig) with speedup < 20%
  UNFAVORABLE:         Speedup CI upper < 20% AND cost CI lower > 5%
  MIXED:               Queue saturation, tail latency, minor reliability, high-spend, mixed signals
  INCONCLUSIVE:        Either median < 1s + < 1k queries/day
  INSUFFICIENT DATA:   < 14 days Gen2 data

NOTE ON COST STATUS (breakeven_status):
  Execution time speedup does NOT directly equal credit reduction.
  Credits depend on warehouse runtime (auto-suspend, minimum billing, idle time).
  WAREHOUSE_METERING_HISTORY already reports ACTUAL Gen2 credit rates
  (e.g., Gen2 XS on AWS = 1.35 credits/hr vs Gen1 = 1 credit/hr).
  No multiplier is applied — comparing raw credits IS comparing dollar costs
  since each credit has the same dollar price in your Snowflake contract.
  COST LOWER = Gen2 total credits per query ≤ Gen1 (speedup offsets the 1.35x rate).
  COST NEUTRAL = within 5% (effectively the same).
  COST HIGHER = Gen2 is faster but not enough to offset the higher credit rate.

NOTE ON SESSION VARIABLES:
  $warehouse_like, $cutover_date, $LOOKBACK_DAYS, $LOOKAHEAD_DAYS are baked into
  views at CREATE time. Re-run this file if configuration changes.

NOTE ON CREDIT PRICING (WHY NO MULTIPLIER):
  Gen2 warehouses consume more credits per hour (e.g., 1.35x on AWS, 1.25x on Azure).
  WAREHOUSE_METERING_HISTORY already reports these higher Gen2 credit rates.
  Since each credit has the same dollar price regardless of Gen1/Gen2, comparing
  raw credits IS comparing dollar costs. No multiplier is needed — applying one
  would double-count the Gen2 premium.

NOTE ON RESULT CACHE EXCLUSION:
  Result-cached queries (bytes_scanned = 0) have execution_time=0 and consume no
  warehouse credits. They are excluded from all views that feed Gen1-vs-Gen2
  statistical comparisons or the assessment, using the filter: bytes_scanned > 0.

  Views that EXCLUDE cached queries (bytes_scanned > 0):
  - VW_GEN2_PERF_DAILY, VW_GEN2_PERF_BY_DOW, VW_GEN2_PERF_WEEKLY,
    VW_GEN2_PATTERN_REGRESSIONS: performance metrics (MEDIAN/P95/P99)
  - VW_GEN2_COST_DAILY, VW_GEN2_COST_WEEKLY: query count denominators
    (credits / non-cached queries = cost per unit of compute work)

  Views that INCLUDE all queries (including cached):
  - VW_GEN2_DATA_VALIDATION: total query counts and cache_hit_rate_pct for coverage
  - VW_GEN2_RELIABILITY: success/failure rates (cached success is real success)
  - VW_GEN2_ERROR_ANALYSIS: error counts and rates

  Rationale: credits (numerator) only reflect warehouse compute — cached queries
  consume zero credits. Denominators must match this scope. If cache hit rates
  differ between Gen1 and Gen2, including cached queries in denominators creates
  asymmetric deflation that confounds the cost comparison.

  VW_GEN2_DATA_VALIDATION exposes cache_hit_rate_pct per period to make any
  asymmetric caching visible.

NOTE ON DOW-LEVEL TESTS:
  Day-of-week stratified tests (VW_GEN2_PERF_WELCH_TEST) are exploratory and
  not corrected for multiple comparisons. With < 21 days Gen2, per-DOW tests
  have low power (n=2-3 per DOW). Use the overall test for decisions.
*/


