# Snowflake Gen2 Warehouse Analysis — Statistical Framework

A SQL-based monitoring framework for evaluating Snowflake Generation 2 (Gen2) warehouse migrations using statistical methods. Produces a defensible, data-driven assessment of cost, performance, and reliability.

## The Problem

Snowflake Gen2 warehouses promise better query performance but consume **more credits per hour** (1.35x on AWS, 1.25x on Azure). The critical question isn't "is Gen2 faster?" but rather:

> **Does Gen2 complete work fast enough to offset its higher per-hour credit rate, resulting in equal or lower cost per query?**

This framework answers that question with statistical confidence intervals — not guesswork.

## What It Does

The framework creates 12 SQL views that analyze three axes of comparison between your Gen1 (baseline) and Gen2 periods:

| Axis | Method | What it measures |
|------|--------|------------------|
| **Performance** | Welch's t-test on daily median execution times | Are queries faster on Gen2? |
| **Cost** | Welch's t-test on daily credits-per-query | Is each unit of work cheaper? |
| **Reliability** | Welch's t-test on daily success rates | Are failure rates unchanged? |

The final view (`VW_GEN2_MIGRATION_DECISION`) synthesizes all three into a single assessment with explanations.

## Key Design Decisions

- **No credit multiplier needed**: `WAREHOUSE_METERING_HISTORY` already reports actual Gen2 credit consumption.
- **Welch's t-test** (not pooled t-test): handles unequal variances and sample sizes between Gen1/Gen2 periods.
- **Day-of-week stratification**: controls for cyclical workload patterns (weekday vs weekend).
- **Matched query patterns** (`patterns_intersect`): only compares queries present in both periods for apples-to-apples performance measurement.
- **Confidence interval-based assessment**: uses CI bounds (not point estimates) — FAVORABLE requires worst-case cost still acceptable, UNFAVORABLE requires best-case cost still too high.
- **Result cache exclusion** (`bytes_scanned > 0`): cached queries consume zero credits and show 0ms execution — including them confounds comparisons.
- **Fieller's theorem** for performance CI: exact confidence intervals on the speed ratio, avoiding delta-method approximation errors on large effects.

## Quick Start

### 0. Switch warehouse(s) to Gen2

Before you can measure anything, your target warehouses need to be running on Gen2. Switch them as close as possible to the start of a UTC day — this gives you clean daily boundaries for the Gen1/Gen2 comparison.

Switching between generations is straightforward in Snowflake:

```sql
-- Switch a single warehouse to Gen2
ALTER WAREHOUSE my_warehouse SET WAREHOUSE_TYPE = 'STANDARD' GENERATION = '2';

-- Switch back to Gen1 if needed
ALTER WAREHOUSE my_warehouse SET WAREHOUSE_TYPE = 'STANDARD' GENERATION = '1';
```

See [Snowflake documentation: Gen2 warehouse examples](https://docs.snowflake.com/en/user-guide/warehouses-gen2#examples-using-generation-clause-recommended-approach) for the recommended approach.

Let the warehouse(s) run on Gen2 for at least 14 days (30+ days preferred) before running the analysis. The framework needs sufficient Gen2 data to produce reliable confidence intervals.

### 1. Set Configuration

```sql
SET cutover_date = '2025-05-01';      -- First full UTC day of Gen2 (the day AFTER you switched)
SET warehouse_like = 'ANALYTICS_%';   -- Your warehouse name pattern
SET LOOKBACK_DAYS = 30;               -- Gen1 baseline window: 30 days
SET LOOKAHEAD_DAYS = 29;              -- Gen2 observation window; 1 day (Cutover date) + 29 days


USE ROLE sysadmin;
USE DATABASE my_database;
USE SCHEMA sandbox;
```

### 2. Create Staging Tables

```sql
-- Run sql/gen2_staging_tables.sql (full refresh first time, incremental daily after)
```

### 3. Create Monitoring Views

```sql
-- Run sql/gen2_migration_monitoring.sql (creates all 12 views)
```

### 4. Get the Assessment

```sql
SELECT * FROM VW_GEN2_MIGRATION_DECISION;
```

## Assessment Output

The framework produces assessments like:

| Assessment | Meaning |
|-----------|---------|
| `STRONGLY FAVORABLE` | Cost CI upper ≤ 0% AND speedup CI lower ≥ 35% |
| `FAVORABLE - Cost neutral` | Worst-case cost at or below Gen1, performance acceptable |
| `FAVORABLE - Strong speedup offsets cost` | Speed gain justifies moderate cost increase |
| `UNFAVORABLE - Cost increase without sufficient speedup` | Gen2 is more expensive without compensating performance |
| `MIXED - Review detailed metrics` | Signals conflict; human judgment needed |
| `INSUFFICIENT DATA` | < 14 days Gen2 data; wait for more |

## Views Created (Dependency Order)

1. `VW_GEN2_DATA_VALIDATION` — Sanity checks and data sufficiency
2. `VW_GEN2_PERF_DAILY` — Daily performance metrics (matched patterns)
3. `VW_GEN2_PERF_WELCH_TEST` — Exploratory: Welch's t-test by day-of-week
4. `VW_GEN2_PERF_BY_DOW` — Performance breakdown by day-of-week
5. `VW_GEN2_PERF_SUMMARY` — Overall performance with Fieller CI
6. `VW_GEN2_COST_DAILY` — Daily credit cost metrics
7. `VW_GEN2_COST_WELCH_TEST` — Cost Welch's t-test with break-even
8. `VW_GEN2_COST_BY_DOW` — Cost breakdown by day-of-week
9. `VW_GEN2_RELIABILITY` — Success rate Welch's t-test
10. `VW_GEN2_ERROR_ANALYSIS` — New/increased error types
11. `VW_GEN2_PATTERN_REGRESSIONS` — Per-pattern regression detection
12. `VW_GEN2_DAILY_TREND` — Combined daily trend for dashboards
13. `VW_GEN2_MIGRATION_DECISION` — Final stakeholder assessment

## Statistical Methodology

### Why Welch's t-test?

All three axes use the same test: Welch's t-test on **daily aggregates** (one observation per day). This:
- Handles unequal sample sizes (Gen1 ~90 days vs Gen2 growing from 7+)
- Handles unequal variances (Gen2 may be more/less variable than Gen1)
- For reliability: naturally accounts for within-day query clustering that inflates query-level z-tests

### Why Confidence Intervals (not p-values alone)?

A statistically significant result tells you the effect is real. A confidence interval tells you **how big** it is. The framework uses 90% CIs on both speedup and cost to make decisions:
- **FAVORABLE**: worst-case (CI bound against you) must still be acceptable
- **UNFAVORABLE**: best-case (CI bound in your favor) must still be unacceptable

### Practical Significance Guards

- Sub-second queries (< 1s median) with low volume (< 1,000/day): percentage speedup is unreliable due to fixed overhead
- High-volume workloads (≥ 1,000/day): exempt from sub-second guard because small per-query savings compound
- Effect size for reliability: DEGRADED requires ≥ 0.5pp success rate drop OR ≥ 25% failure rate increase

## Credit Pricing — The Key Insight

Gen2 warehouses consume more credits per hour:

| Cloud | Gen1 XS | Gen2 XS | Multiplier |
|-------|---------|---------|------------|
| AWS | 1.00 cr/hr | 1.35 cr/hr | 1.35x |
| Azure | 1.00 cr/hr | 1.25 cr/hr | 1.25x |

But `WAREHOUSE_METERING_HISTORY` **already reports actual Gen2 rates**. So:
- Gen2 XS running for 1 hour → `credits_used = 1.35` (already inflated)
- Each credit costs the same dollar amount regardless of Gen1/Gen2
- Comparing raw credits from metering history IS comparing dollar costs
- **No multiplier needed** — applying one double-counts the premium

Break-even means: Gen2 finishes work fast enough that despite consuming 1.35 credits/hour, total credits per query ≤ Gen1.

## Requirements

- Snowflake account with `ACCOUNT_USAGE` access (typically requires `SYSADMIN` or `ACCOUNTADMIN`)
- At least 14 days of Gen2 data for reliable assessment (7 minimum for preliminary signals)
- At least 30 days Gen1 baseline recommended (for stable variance estimation)

## Limitations

- This framework presents data signals — **it does not dictate decisions**. Stakeholders weigh business context the framework cannot see.
- Credits are warehouse-level (Snowflake does not track per-query credits). All "cost per query" metrics are allocation models.
- Day-of-week stratified tests are exploratory with low statistical power (< 21 days Gen2). Use the overall pooled test for decisions.
- The framework assumes workload composition is roughly stable between periods. Large shifts in query mix can confound cost comparisons (the `pattern_coverage_assessment` column flags this).

## Disclaimer

This framework is provided **"as is"**, without warranty of any kind, express or implied. The SQL code, statistical methodology, and any assessments produced are for **informational and educational purposes only** and do not constitute professional, financial, or technical advice.

**No liability for decisions**: The author(s) accept no responsibility or liability for any loss, damage, cost, or expense incurred as a result of decisions made based on the output of this framework. Migration decisions involve business context, contractual terms, workload characteristics, and risk tolerances that this framework cannot evaluate.

**No guarantee of accuracy**: Statistical outputs depend on data quality, configuration correctness, workload stability, and assumptions that may not hold in your environment. Results should be independently validated before acting on them.

**Not affiliated with Snowflake**: This is an independent, community-developed tool. It is not endorsed by, affiliated with, or supported by Snowflake Inc. Snowflake's Gen2 pricing, credit mechanics, and warehouse behavior may change without notice, potentially invalidating assumptions in this framework.

**Your responsibility**: You are solely responsible for verifying that this framework is appropriate for your use case, that configuration parameters are correct, and that any migration decisions account for your organization's specific requirements and risk profile.

