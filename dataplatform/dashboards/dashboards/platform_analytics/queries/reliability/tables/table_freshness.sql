-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Reliability Tables
-- View: Table Freshness Status
-- =============================================================================
-- Visualization: Table (sortable, with conditional formatting)
-- Title: "Table Freshness Status"
-- Purpose: Per-table view of freshness and SLA compliance
--
-- Columns:
--   - region, database, table
--   - freshness_status (🟢/🟡/🔴)
--   - last_partition_landed, days_since_last_landing
--   - pct_meeting_1d_sla, pct_meeting_2d_sla
--   - avg_latency_days, max_latency_days
--
-- Filters:
--   - freshness_status dropdown (Fresh, Warning, Stale, All)
--   - region dropdown
--
-- Conditional Formatting:
--   - freshness_status column colored by status
--   - pct_meeting_2d_sla: green >95%, yellow 80-95%, red <80%
-- =============================================================================

-- Get most recent date's metrics for firmwarevdp-owned tables
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
-- Get firmwarevdp-owned tables from dim_tables (with region/database/table names)
owned_tables AS (
  SELECT DISTINCT
    t.date,
    t.table_id,
    t.region,
    t.database,
    t.`table`
  FROM product_analytics_staging.dim_tables t
  WHERE t.date = (SELECT max_date FROM common_date)
    AND t.team = :team
)
SELECT 
  o.region,
  o.database,
  o.`table`,
  -- Add emoji indicators for display (stored as plain text)
  CASE r.freshness_status
    WHEN 'FRESH' THEN '🟢 FRESH'
    WHEN 'WARNING' THEN '🟡 WARNING'
    WHEN 'STALE' THEN '🔴 STALE'
    ELSE r.freshness_status
  END AS freshness_status,
  r.last_partition_landed,
  r.days_since_last_landing,
  
  -- 30-day SLA compliance (0-1 range for UI percent formatting)
  ROUND(r.monthly.pct_meeting_1d_sla, 4) AS pct_meeting_1d_sla,
  ROUND(r.monthly.pct_meeting_2d_sla, 4) AS pct_meeting_2d_sla,
  
  -- Latency metrics
  ROUND(r.monthly.avg_latency_days, 2) AS avg_latency_days,
  r.monthly.max_latency_days,
  
  -- Volume
  r.monthly.partitions_landed AS partitions_landed_30d

FROM owned_tables o
JOIN product_analytics_staging.agg_landing_reliability_rolling r
  USING (date, table_id)
ORDER BY 
  CASE r.freshness_status 
    WHEN 'STALE' THEN 1 
    WHEN 'WARNING' THEN 2 
    ELSE 3 
  END,
  r.days_since_last_landing DESC,
  o.database, 
  o.`table`
