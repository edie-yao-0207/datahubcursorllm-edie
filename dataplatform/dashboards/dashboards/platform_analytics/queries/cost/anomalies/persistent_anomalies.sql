-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Cost Anomalies
-- View: Persistent Anomalies
-- =============================================================================
-- Visualization: Table (sortable)
-- Title: "Services with Persistent Anomalies (3+ Days)"
-- Description: Services showing sustained cost anomalies indicating systemic issues 
--              rather than one-time spikes. Prioritize by days_anomalous.
--
-- Columns:
--   - service, region, team
--   - streak_start, streak_end, days_anomalous
--   - status, avg_cost, max_z_score
-- Sort: days_anomalous DESC, avg_cost DESC
-- =============================================================================

WITH anomaly_days AS (
  SELECT 
    h.service,
    h.region,
    h.team,
    a.date,
    a.daily.status,
    a.daily.cost,
    a.daily.z_score,
    -- Detect gaps: if previous date is not yesterday, start new streak
    CASE WHEN LAG(a.date) OVER (PARTITION BY h.service, h.region, h.team ORDER BY a.date) 
              = DATE_SUB(a.date, 1)
         THEN 0 ELSE 1 END AS is_new_streak
  FROM product_analytics_staging.fct_table_cost_anomalies a
  JOIN product_analytics_staging.dim_cost_hierarchies h
    USING (date, cost_hierarchy_id)
  WHERE a.date BETWEEN :date.min AND :date.max
    AND h.team = :team
    AND h.service IS NOT NULL  -- Service-level groupings only
    AND a.daily.status IN ('critical_increase', 'anomalous_increase', 'warning')
),
anomaly_streaks AS (
  SELECT 
    *,
    SUM(is_new_streak) OVER (PARTITION BY service, region, team ORDER BY date) AS streak_group
  FROM anomaly_days
)
SELECT 
  service,
  region,
  team,
  CAST(MIN(date) AS DATE) AS streak_start,
  CAST(MAX(date) AS DATE) AS streak_end,
  COUNT(*) AS days_anomalous,  -- Actual count of anomaly days
  MAX(status) AS status,
  ROUND(AVG(cost), 2) AS avg_cost,
  ROUND(MAX(z_score), 2) AS max_z_score
FROM anomaly_streaks
GROUP BY service, region, team, streak_group
HAVING COUNT(*) >= 3  -- 3+ consecutive anomaly days
ORDER BY days_anomalous DESC, avg_cost DESC
LIMIT CAST(CASE WHEN :limit <= 0 THEN 999999 ELSE :limit END AS INT)
