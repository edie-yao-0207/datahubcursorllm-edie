-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Cost Alerts
-- View: Alert Details
-- =============================================================================
-- Visualization: Table
-- Title: "Cost Alert Details"
-- Description: Detailed view of all current alerts with full context.
--              z_score > 3 indicates extreme deviation.
-- =============================================================================

SELECT
  CAST(date AS DATE) AS date,
  alert_type,
  time_horizon,
  hierarchy_label,
  grouping_columns,
  region,
  team,
  product_group,
  dataplatform_feature,
  service,
  ROUND(cost, 2) AS cost,
  ROUND(baseline_mean, 2) AS baseline_mean,
  ROUND(z_score, 2) AS z_score,
  ROUND(percent_change, 1) AS percent_change_pct,
  status
FROM product_analytics_staging.dim_cost_alerts
WHERE date BETWEEN :date.min AND :date.max
  AND team = :team
ORDER BY 
  date DESC,
  CASE alert_type 
    WHEN 'critical_service_increase' THEN 1
    WHEN 'critical_team_increase' THEN 2
    WHEN 'critical_decrease' THEN 3
    ELSE 4
  END,
  ABS(z_score) DESC
LIMIT CAST(CASE WHEN :limit <= 0 THEN 999999 ELSE :limit END AS INT)

