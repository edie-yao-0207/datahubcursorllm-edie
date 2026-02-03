-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Usage Overview
-- View: Usage Growth (2 charts)
-- =============================================================================
-- 
-- CHART 1: Active Users Growth
-- Visualization: Line Chart (dual-axis)
-- Title: "FirmwareVDP Monthly Active Users Growth"
-- Description: Track MAU (monthly active users) and MoM growth rate.
--
-- CHART 2: Query Volume Growth  
-- Visualization: Line Chart (dual-axis)
-- Title: "FirmwareVDP Monthly Query Volume Growth"
--
-- Annotations:
--   - Horizontal line at 0% on growth axes (label: "Zero Growth", style: dashed)
--   - Horizontal line at 10% on growth axes (label: "Healthy Growth Target", color: green)
--   - Horizontal line at -10% on growth axes (label: "Concern Threshold", color: yellow)
-- =============================================================================

SELECT 
  CAST(u.date AS DATE) AS date,
  u.monthly.unique_users AS monthly_active_users,
  u.monthly.delta_unique_users AS user_change,
  u.monthly.delta_unique_users / NULLIF(u.monthly.unique_users - u.monthly.delta_unique_users, 0) AS user_growth_pct,
  u.monthly.total_queries AS monthly_queries,
  (u.monthly.total_queries - LAG(u.monthly.total_queries) OVER (ORDER BY u.date)) 
    / NULLIF(LAG(u.monthly.total_queries) OVER (ORDER BY u.date), 0) AS query_growth_pct
FROM product_analytics_staging.agg_table_usage_rolling u
JOIN product_analytics_staging.dim_cost_hierarchies h
  USING (date, cost_hierarchy_id)
WHERE h.grouping_columns = 'team'
  AND h.team = :team
  AND u.date BETWEEN :date.min AND :date.max
ORDER BY u.date DESC
