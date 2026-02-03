-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Reliability Overview
-- View: Worst Offenders
-- =============================================================================
-- Visualization: Table
-- Title: "Tables with Poorest Reliability"
-- Purpose: Identify tables that consistently underperform on reliability metrics
--          over the selected time span. Prioritize remediation efforts.
--
-- Table Columns:
--   - database, table, avg_pct_meeting_2d_sla, days_stale, avg_latency_days
--
-- Use Case: Find tables needing attention, track problematic patterns
-- =============================================================================

-- Aggregate reliability metrics per table over the entire date range
WITH date_bounds AS (
  SELECT
    GREATEST(
      (SELECT MIN(date) FROM product_analytics_staging.agg_landing_reliability_rolling WHERE date BETWEEN :date.min AND :date.max),
      (SELECT MIN(date) FROM product_analytics_staging.dim_tables WHERE date BETWEEN :date.min AND :date.max)
    ) AS min_date,
    LEAST(
      (SELECT MAX(date) FROM product_analytics_staging.agg_landing_reliability_rolling WHERE date BETWEEN :date.min AND :date.max),
      (SELECT MAX(date) FROM product_analytics_staging.dim_tables WHERE date BETWEEN :date.min AND :date.max)
    ) AS max_date
),

-- Get firmwarevdp-owned tables with their attributes
owned_tables AS (
  SELECT
    t.date,
    t.table_id,
    t.region,
    t.database,
    t.`table`
  FROM product_analytics_staging.dim_tables t
  WHERE t.date BETWEEN (SELECT min_date FROM date_bounds) AND (SELECT max_date FROM date_bounds)
    AND t.team = :team
),

-- Aggregate reliability metrics per table
table_reliability AS (
  SELECT
    o.database,
    o.`table`,
    o.region,
    
    -- Count metrics
    COUNT(DISTINCT o.date) AS days_tracked,
    SUM(CASE WHEN r.freshness_status = 'STALE' THEN 1 ELSE 0 END) AS days_stale,
    SUM(CASE WHEN r.freshness_status = 'WARNING' THEN 1 ELSE 0 END) AS days_warning,
    SUM(CASE WHEN r.freshness_status = 'FRESH' THEN 1 ELSE 0 END) AS days_fresh,
    
    -- SLA compliance (average over period, as 0-1 for UI percent formatting)
    ROUND(AVG(r.monthly.pct_meeting_1d_sla), 3) AS avg_pct_meeting_1d_sla,
    ROUND(AVG(r.monthly.pct_meeting_2d_sla), 3) AS avg_pct_meeting_2d_sla,
    
    -- Latency metrics
    ROUND(AVG(r.monthly.avg_latency_days), 2) AS avg_latency_days,
    MAX(r.monthly.max_latency_days) AS max_latency_days,
    
    -- Volume
    SUM(r.monthly.partitions_landed) AS total_partitions_landed
    
  FROM owned_tables o
  JOIN product_analytics_staging.agg_landing_reliability_rolling r
    USING (date, table_id)
  GROUP BY o.database, o.`table`, o.region
),

-- Calculate a "reliability score" for ranking (lower = worse)
scored AS (
  SELECT
    *,
    -- Score: penalize stale days, low SLA compliance, high latency (0-100 scale)
    (avg_pct_meeting_2d_sla * 100 * 0.5) + 
    ((1.0 - (days_stale * 1.0 / NULLIF(days_tracked, 0))) * 100 * 0.3) +
    ((1.0 - LEAST(avg_latency_days / 5.0, 1.0)) * 100 * 0.2) AS reliability_score
  FROM table_reliability
  WHERE days_tracked > 0
)

SELECT
  database,
  `table`,
  region,
  
  -- Freshness breakdown
  days_tracked,
  days_stale,
  days_warning,
  days_fresh,
  ROUND(days_stale * 1.0 / NULLIF(days_tracked, 0), 3) AS pct_days_stale,
  
  -- SLA compliance
  avg_pct_meeting_1d_sla,
  avg_pct_meeting_2d_sla,
  
  -- Latency
  avg_latency_days,
  max_latency_days,
  
  -- Overall score (0-100, lower = worse)
  ROUND(reliability_score, 1) AS reliability_score,
  
  -- Quick navigation links
  CONCAT('https://datahub.internal.samsara.com/dataset/urn:li:dataset:(urn:li:dataPlatform:databricks,', database, '.', `table`, ',PROD)/Schema') AS datahub_url,
  CONCAT('https://dagster.internal.samsara.com/assets/', region, '/', database, '/', `table`) AS dagster_url

FROM scored
WHERE days_stale > 0 OR avg_pct_meeting_2d_sla < 0.95 OR avg_latency_days > 1
ORDER BY reliability_score ASC, days_stale DESC
LIMIT CAST(CASE WHEN :limit <= 0 THEN 999999 ELSE :limit END AS INT)

