-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Reliability Tables
-- View: SLA Breaches
-- =============================================================================
-- Visualization: Table (sortable, with conditional formatting)
-- Title: "Tables Breaching 2-Day SLA"
-- Purpose: Identify and prioritize tables with SLA issues
--
-- Columns:
--   - region, database, table
--   - late_partitions_7d, late_partitions_30d
--   - avg_latency_days, max_latency_days
--   - worst_partition (partition_date with highest latency)
--
-- Sort: By late_partitions_7d DESC (prioritize recent issues)
--
-- Conditional Formatting:
--   - late_partitions_7d: red if >5, yellow if >0
--   - max_latency_days: red if >5 days
-- =============================================================================

-- Find common date range where both tables have data
WITH max_dates AS (
  SELECT 
    (SELECT MAX(date) FROM product_analytics_staging.fct_partition_landings WHERE date BETWEEN :date.min AND :date.max) AS landings_max,
    (SELECT MAX(date) FROM product_analytics_staging.dim_tables WHERE date BETWEEN :date.min AND :date.max) AS tables_max
),
common_max AS (
  SELECT LEAST(landings_max, tables_max) AS max_date
  FROM max_dates
),
-- Get firmwarevdp-owned tables from dim_tables (with region/database/table names)
owned_tables AS (
  SELECT DISTINCT
    t.date,
    t.table_id,
    t.region,
    t.database,
    t.`table`
  FROM product_analytics_staging.dim_tables t
  WHERE t.date BETWEEN DATE_SUB((SELECT max_date FROM common_max), 29) AND (SELECT max_date FROM common_max)
    AND t.team = :team
),
-- Get partition-level details for SLA breaches
late_landings AS (
  SELECT 
    o.region,
    o.database,
    o.`table`,
    f.partition_date,
    f.date AS landing_date,
    f.landing_latency_days,
    -- Categorize by recency
    CASE 
      WHEN f.date >= DATE_SUB((SELECT max_date FROM common_max), 6) THEN 'last_7d'
      ELSE 'last_30d'
    END AS recency
  FROM owned_tables o
  JOIN product_analytics_staging.fct_partition_landings f
    USING (date, table_id)
  WHERE NOT f.meets_2d_sla
),
aggregated AS (
  SELECT
    region,
    database,
    `table`,
    COUNT(CASE WHEN recency = 'last_7d' THEN 1 END) AS late_partitions_7d,
    COUNT(*) AS late_partitions_30d,
    ROUND(AVG(landing_latency_days), 1) AS avg_latency_days,
    MAX(landing_latency_days) AS max_latency_days,
    -- Get the worst partition
    MAX_BY(partition_date, landing_latency_days) AS worst_partition
  FROM late_landings
  GROUP BY region, database, `table`
)
SELECT 
  region,
  database,
  `table`,
  late_partitions_7d,
  late_partitions_30d,
  avg_latency_days,
  max_latency_days,
  worst_partition
FROM aggregated
WHERE late_partitions_30d > 0
ORDER BY late_partitions_7d DESC, late_partitions_30d DESC, max_latency_days DESC
LIMIT CAST(CASE WHEN :limit <= 0 THEN 999999 ELSE :limit END AS INT)
