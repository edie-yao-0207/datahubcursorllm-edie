-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Cost Alerts
-- View: Active Alerts Summary
-- =============================================================================
-- Visualization: Table
-- Title: "Active Cost Alerts by Type"
-- Description: Current alerts grouped by type and severity.
--              critical_service_increase is highest priority.
-- =============================================================================

SELECT
  alert_type,
  time_horizon,
  COUNT(*) AS alert_count,
  SUM(cost) AS total_cost_affected,
  ROUND(AVG(z_score), 2) AS avg_z_score,
  ROUND(AVG(percent_change), 1) AS avg_percent_change
FROM product_analytics_staging.dim_cost_alerts
WHERE date BETWEEN :date.min AND :date.max
  AND team = :team
GROUP BY alert_type, time_horizon
ORDER BY 
  CASE alert_type 
    WHEN 'critical_service_increase' THEN 1
    WHEN 'critical_team_increase' THEN 2
    WHEN 'critical_decrease' THEN 3
    WHEN 'multi_horizon_anomaly' THEN 4
    WHEN 'anomalous_weekly_spike' THEN 5
    WHEN 'overall_daily_spike' THEN 6
    ELSE 7
  END,
  time_horizon

