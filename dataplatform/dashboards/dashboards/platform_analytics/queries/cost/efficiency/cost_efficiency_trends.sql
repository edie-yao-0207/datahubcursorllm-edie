-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Cost Efficiency
-- View: Efficiency Degradation
-- =============================================================================
-- Visualization: Table (sortable, with conditional formatting)
-- Title: "Services with Degrading Efficiency"
-- Description: Services where cost is increasing while usage is decreasing (🔴) 
--              or high cost with very few users (🟡). Top optimization candidates.
--
-- Columns:
--   - date, service, team
--   - weekly_cost, cost_change
--   - weekly_users, user_change
--   - cost_per_1k_queries
--   - efficiency_status (🔴 INEFFICIENT, 🟡 UNDERUTILIZED, 🟢 OK)
-- =============================================================================

-- Join using hierarchy_id for simplified joining
SELECT 
  CAST(c.date AS DATE) AS date,
  h.service,
  h.team,
  c.weekly.cost AS weekly_cost,
  c.weekly.delta AS cost_change,
  u.weekly.unique_users AS weekly_users,
  u.weekly.delta_unique_users AS user_change,
  (c.weekly.cost / NULLIF(u.weekly.total_queries, 0)) * 1000 AS cost_per_1k_queries,
  CASE 
    WHEN c.weekly.delta > 0 AND u.weekly.delta_unique_users < 0 THEN '🔴 INEFFICIENT'
    WHEN c.weekly.cost > 100 AND u.weekly.unique_users < 5 THEN '🟡 UNDERUTILIZED'
    ELSE '🟢 OK'
  END AS efficiency_status
FROM product_analytics_staging.agg_costs_rolling c
JOIN product_analytics_staging.agg_table_usage_rolling u
  USING (date, cost_hierarchy_id)
JOIN product_analytics_staging.dim_cost_hierarchies h
  USING (date, cost_hierarchy_id)
WHERE h.service IS NOT NULL  -- Service-level groupings only
  AND h.team = :team
  AND c.date BETWEEN :date.min AND :date.max
  AND (
    (c.weekly.delta > 0 AND u.weekly.delta_unique_users < 0)
    OR (c.weekly.cost > 100 AND u.weekly.unique_users < 5)
  )
ORDER BY c.date DESC, c.weekly.cost DESC
LIMIT CAST(CASE WHEN :limit <= 0 THEN 999999 ELSE :limit END AS INT)
