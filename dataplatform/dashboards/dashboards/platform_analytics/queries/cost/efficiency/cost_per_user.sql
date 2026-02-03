-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Cost Efficiency
-- View: Cost per User
-- =============================================================================
-- Visualization: Line Chart (dual-axis)
-- Title: "Cost Efficiency: $ per User & $ per 1K Queries"
-- Description: Track unit economics over time. Monitor cost efficiency relative
--              to user count and query volume.
--
-- X-Axis: date
-- Y-Axis (left): cost_per_user (USD)
-- Y-Axis (right): cost_per_1k_queries (USD)
--
-- Annotations:
--   - Horizontal line at $100 on left Y-axis (label: "High Cost Threshold", color: red)
--   - Horizontal line at $50 on left Y-axis (label: "Target Cost/User", color: green)
--   - Horizontal line at $1.00 on right Y-axis (label: "Target Cost/1K Queries", color: green)
--   - Optional: Shaded band $50-$100 as "Acceptable Range" on left axis
-- =============================================================================

SELECT 
  CAST(c.date AS DATE) AS date,
  c.monthly.cost AS total_monthly_cost,
  u.monthly.unique_users AS active_users,
  c.monthly.cost / NULLIF(u.monthly.unique_users, 0) AS cost_per_user,
  u.monthly.total_queries AS total_queries,
  (c.monthly.cost / NULLIF(u.monthly.total_queries, 0)) * 1000 AS cost_per_1k_queries,
  CASE 
    WHEN c.monthly.cost / NULLIF(u.monthly.unique_users, 0) > 100 THEN '🔴 HIGH'
    WHEN c.monthly.cost / NULLIF(u.monthly.unique_users, 0) > 50 THEN '🟡 MEDIUM'
    ELSE '🟢 GOOD'
  END AS efficiency_status
FROM product_analytics_staging.agg_costs_rolling c
JOIN product_analytics_staging.agg_table_usage_rolling u
  USING (date, cost_hierarchy_id)
JOIN product_analytics_staging.dim_cost_hierarchies h
  USING (date, cost_hierarchy_id)
WHERE h.grouping_columns = 'team'
  AND h.team = :team
  AND c.date BETWEEN :date.min AND :date.max
ORDER BY c.date DESC
