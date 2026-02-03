-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Reliability Overview
-- View: Reliability Snapshot
-- =============================================================================
-- Visualization: Scorecards (4-6 cards)
-- Title: "Data Reliability At-a-Glance"
-- Purpose: Quick health check - are our tables fresh and landing on time?
--
-- Scorecards:
--   1. "Tables Tracked" - total_tables (count)
--   2. "Fresh" - fresh_tables (count, color: green)
--   3. "Warning" - warning_tables (count, color: yellow)
--   4. "Stale" - stale_tables (count, color: red)
--   5. "2-Day SLA %" - pct_meeting_2d_sla (%, target: >95%)
--   6. "Avg Latency" - avg_latency_days (days, target: <1.0)
--
-- Annotations:
--   - Color code scorecards based on status (green/yellow/red)
--   - Show delta vs previous period where applicable
-- =============================================================================

-- Latest day snapshot for scorecards (filtered to firmwarevdp-owned tables)
-- Use the MINIMUM of max dates across tables to ensure joins work
WITH max_dates AS (
  SELECT 
    (SELECT MAX(date) FROM product_analytics_staging.agg_landing_reliability_rolling WHERE date BETWEEN :date.min AND :date.max) AS reliability_max,
    (SELECT MAX(date) FROM product_analytics_staging.dim_tables WHERE date BETWEEN :date.min AND :date.max) AS tables_max
),
common_date AS (
  SELECT LEAST(reliability_max, tables_max) AS max_date
  FROM max_dates
),
-- Get firmwarevdp-owned tables from dim_tables
owned_tables AS (
  SELECT DISTINCT
    t.date,
    t.table_id
  FROM product_analytics_staging.dim_tables t
  WHERE t.date = (SELECT max_date FROM common_date)
    AND t.team = :team
)
SELECT 
  CAST(r.date AS DATE) AS date,
  
  -- Table health counts (freshness_status stored as plain text)
  COUNT(*) AS total_tables,
  SUM(CASE WHEN r.freshness_status = 'FRESH' THEN 1 ELSE 0 END) AS fresh_tables,
  SUM(CASE WHEN r.freshness_status = 'WARNING' THEN 1 ELSE 0 END) AS warning_tables,
  SUM(CASE WHEN r.freshness_status = 'STALE' THEN 1 ELSE 0 END) AS stale_tables,
  
  -- Percentages (0-1 range for UI percent formatting)
  ROUND(SUM(CASE WHEN r.freshness_status = 'FRESH' THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) AS pct_fresh,
  ROUND(SUM(CASE WHEN r.freshness_status = 'STALE' THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) AS pct_stale,
  
  -- SLA compliance (30-day rolling, 0-1 range)
  ROUND(AVG(r.monthly.pct_meeting_1d_sla), 4) AS pct_meeting_1d_sla,
  ROUND(AVG(r.monthly.pct_meeting_2d_sla), 4) AS pct_meeting_2d_sla,
  
  -- Latency (30-day rolling)
  ROUND(AVG(r.monthly.avg_latency_days), 2) AS avg_latency_days,
  MAX(r.monthly.max_latency_days) AS max_latency_days,
  
  -- Volume
  SUM(r.monthly.partitions_landed) AS partitions_landed_30d

FROM owned_tables o
JOIN product_analytics_staging.agg_landing_reliability_rolling r
  USING (date, table_id)
GROUP BY r.date
