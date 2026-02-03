-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Cost Alerts
-- View: Recurring Alerts
-- =============================================================================
-- Visualization: Table
-- Title: "Recurring Cost Alerts (Chronic Issues)"
-- Description: Services with alerts on multiple days. High recurrence indicates
--              persistent problems needing systematic investigation.
-- =============================================================================

WITH alert_history AS (
  SELECT
    service,
    region,
    team,
    product_group,
    dataplatform_feature,
    alert_type,
    COUNT(DISTINCT date) AS recurrence_days,
    MIN(date) AS first_alert_date,
    MAX(date) AS last_alert_date,
    AVG(cost) AS avg_cost,
    AVG(z_score) AS avg_z_score
  FROM product_analytics_staging.dim_cost_alerts
  WHERE date BETWEEN :date.min AND :date.max
    AND team = :team
  GROUP BY service, region, team, product_group, dataplatform_feature, alert_type
  HAVING COUNT(DISTINCT date) >= 2
)

SELECT
  service,
  region,
  team,
  product_group,
  dataplatform_feature,
  alert_type,
  recurrence_days,
  CAST(first_alert_date AS DATE) AS first_alert_date,
  CAST(last_alert_date AS DATE) AS last_alert_date,
  DATEDIFF(last_alert_date, first_alert_date) AS alert_span_days,
  ROUND(avg_cost, 2) AS avg_cost,
  ROUND(avg_z_score, 2) AS avg_z_score
FROM alert_history
ORDER BY recurrence_days DESC, avg_z_score DESC
LIMIT CAST(CASE WHEN :limit <= 0 THEN 999999 ELSE :limit END AS INT)

