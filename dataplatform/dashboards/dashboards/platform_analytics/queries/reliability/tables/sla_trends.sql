-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Reliability Trends
-- View: SLA Compliance & Latency Over Time
-- =============================================================================
-- Visualization: Line Chart (multi-series, dual-axis)
-- Title: "Partition Landing Reliability Over Time"
-- Purpose: Deep dive into SLA compliance and latency trends
--
-- X-Axis: date
-- Y-Axis (left): pct_meeting_2d_sla, pct_meeting_1d_sla (0-100%)
-- Y-Axis (right): avg_latency_days
--
-- Series:
--   - monthly_pct_meeting_2d_sla (primary line, thicker)
--   - monthly_pct_meeting_1d_sla (secondary line)
--   - monthly_avg_latency (right axis)
--   - daily_* variants available for more granular view
--
-- Annotations:
--   - Horizontal line at 95% on left Y-axis (label: "2-Day SLA Target")
--   - Horizontal line at 80% on left Y-axis (label: "1-Day SLA Target")  
--   - Horizontal line at 1.0 on right Y-axis (label: "Latency Target")
--   - Optional: Shaded band 90-100% as "Healthy Zone" on SLA axis
-- =============================================================================

-- Find common date range where both tables have data
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
-- Get firmwarevdp-owned tables from dim_tables
owned_tables AS (
  SELECT DISTINCT
    t.date,
    t.table_id
  FROM product_analytics_staging.dim_tables t
  WHERE t.date BETWEEN (SELECT min_date FROM date_bounds) AND (SELECT max_date FROM date_bounds)
    AND t.team = :team
)
-- Aggregate across firmwarevdp-owned tables per day
SELECT 
  CAST(r.date AS DATE) AS date,
  
  -- Daily metrics (more volatile, useful for spotting incidents)
  SUM(r.daily.partitions_landed) AS daily_partitions_landed,
  ROUND(AVG(r.daily.avg_latency_days), 2) AS daily_avg_latency,
  ROUND(AVG(r.daily.pct_meeting_1d_sla), 4) AS daily_pct_meeting_1d_sla,
  ROUND(AVG(r.daily.pct_meeting_2d_sla), 4) AS daily_pct_meeting_2d_sla,
  
  -- 30-day rolling metrics (smoother trend line, 0-1 range for UI percent formatting)
  SUM(r.monthly.partitions_landed) AS monthly_partitions_landed,
  ROUND(AVG(r.monthly.avg_latency_days), 2) AS monthly_avg_latency,
  ROUND(AVG(r.monthly.pct_meeting_1d_sla), 4) AS monthly_pct_meeting_1d_sla,
  ROUND(AVG(r.monthly.pct_meeting_2d_sla), 4) AS monthly_pct_meeting_2d_sla,
  
  -- Table health distribution (for stacked area chart)
  SUM(CASE WHEN r.freshness_status = 'FRESH' THEN 1 ELSE 0 END) AS fresh_tables,
  SUM(CASE WHEN r.freshness_status = 'WARNING' THEN 1 ELSE 0 END) AS warning_tables,
  SUM(CASE WHEN r.freshness_status = 'STALE' THEN 1 ELSE 0 END) AS stale_tables

FROM owned_tables o
JOIN product_analytics_staging.agg_landing_reliability_rolling r
  USING (date, table_id)
GROUP BY r.date
ORDER BY r.date DESC
