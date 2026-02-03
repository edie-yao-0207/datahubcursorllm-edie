-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Cost Anomalies
-- View: Critical Service Increases
-- =============================================================================
-- Visualization: Table (sortable, with conditional formatting)
-- Title: "Services with Critical Cost Spikes (>5σ)"
-- Description: Top 50 services with statistically significant cost increases 
--              requiring immediate investigation. Sort by z-score to prioritize.
--
-- Columns:
--   - date, region, team, product_group, dataplatform_feature, service
--   - current_cost, expected_cost, cost_increase (USD)
--   - cost_z_score, percent_change (0-1 scale, format as %)
-- Conditional Formatting: Highlight rows with z_score > 7 in red
-- =============================================================================

SELECT 
  CAST(a.date AS DATE) AS date,
  h.region,
  h.team,
  h.product_group,
  h.dataplatform_feature,
  h.service,
  a.daily.cost AS current_cost,
  a.daily.z_score AS cost_z_score,
  a.daily.percent_change AS percent_change,
  a.daily_baseline.mean AS expected_cost,
  a.daily.cost - a.daily_baseline.mean AS cost_increase
FROM product_analytics_staging.fct_table_cost_anomalies a
JOIN product_analytics_staging.dim_cost_hierarchies h
  USING (date, cost_hierarchy_id)
WHERE a.date BETWEEN :date.min AND :date.max
  AND h.team = :team
  AND h.service IS NOT NULL  -- Service-level groupings only
  AND a.daily.status = 'critical_increase'
ORDER BY a.daily.z_score DESC
LIMIT CAST(CASE WHEN :limit <= 0 THEN 999999 ELSE :limit END AS INT)
