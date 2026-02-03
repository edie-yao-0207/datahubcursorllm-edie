-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Reliability Overview
-- View: Reliability Trend
-- =============================================================================
-- Visualization: Line Chart (multi-series)
-- Title: "Data Reliability Over Time"
-- Purpose: Track reliability metrics across the selected date range to identify
--          trends, improvements, or degradations in data freshness.
--
-- X-axis: date
-- Y-axis: percentage / count
-- Series: pct_fresh, pct_meeting_2d_sla, avg_latency_days
--
-- Use Case: See how reliability changes day-over-day, catch degradation early
-- =============================================================================

-- Daily reliability metrics for firmwarevdp-owned tables
-- Use date bounds to ensure joins work across both tables
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

-- Get firmwarevdp-owned tables for each date
owned_tables AS (
  SELECT
    t.date,
    t.table_id
  FROM product_analytics_staging.dim_tables t
  WHERE t.date BETWEEN (SELECT min_date FROM date_bounds) AND (SELECT max_date FROM date_bounds)
    AND t.team = :team
)

SELECT 
  CAST(r.date AS DATE) AS date,
  
  -- Table health counts
  COUNT(*) AS total_tables,
  SUM(CASE WHEN r.freshness_status = 'FRESH' THEN 1 ELSE 0 END) AS fresh_tables,
  SUM(CASE WHEN r.freshness_status = 'WARNING' THEN 1 ELSE 0 END) AS warning_tables,
  SUM(CASE WHEN r.freshness_status = 'STALE' THEN 1 ELSE 0 END) AS stale_tables,
  
  -- Percentages (0-1 range for UI percent formatting)
  ROUND(SUM(CASE WHEN r.freshness_status = 'FRESH' THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) AS pct_fresh,
  ROUND(SUM(CASE WHEN r.freshness_status = 'STALE' THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) AS pct_stale,
  
  -- SLA compliance (30-day rolling averages, 0-1 range)
  ROUND(AVG(r.monthly.pct_meeting_1d_sla), 4) AS pct_meeting_1d_sla,
  ROUND(AVG(r.monthly.pct_meeting_2d_sla), 4) AS pct_meeting_2d_sla,
  
  -- Latency metrics
  ROUND(AVG(r.monthly.avg_latency_days), 2) AS avg_latency_days,
  MAX(r.monthly.max_latency_days) AS max_latency_days,
  
  -- Volume
  SUM(r.monthly.partitions_landed) AS partitions_landed_30d

FROM owned_tables o
JOIN product_analytics_staging.agg_landing_reliability_rolling r
  USING (date, table_id)
GROUP BY r.date
ORDER BY r.date DESC

