-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Usage Overview
-- View: Usage Summary (2 charts)
-- =============================================================================
-- 
-- CHART 1: Active Users Summary
-- Visualization: Combination Chart (Bar + Line)
-- Title: "FirmwareVDP Active Users: Daily, Weekly, Monthly"
-- Description: Track DAU, WAU, and MAU trends to understand engagement patterns.
--
-- X-Axis: date
-- Y-Axis: user_count
-- Series:
--   - daily_users (bar, light blue)
--   - weekly_active_users (line, blue, medium weight)
--   - monthly_active_users (line, dark blue, heavy weight)
--
-- CHART 2: Query Volume Summary
-- Visualization: Combination Chart (Bar + Line)
-- Title: "FirmwareVDP Query Volume: Daily, Weekly, Monthly"
-- =============================================================================

SELECT 
  CAST(u.date AS DATE) AS date,
  u.daily.unique_users AS daily_users,
  u.daily.total_queries AS daily_queries,
  u.daily.unique_tables AS tables_accessed_today,
  u.weekly.unique_users AS weekly_active_users,
  u.weekly.total_queries AS weekly_queries,
  u.monthly.unique_users AS monthly_active_users,
  u.monthly.total_queries AS monthly_queries,
  u.monthly.unique_tables AS unique_tables_accessed_monthly,
  u.monthly.avg_queries_per_user AS avg_queries_per_user,
  u.monthly.avg_queries_per_table AS avg_queries_per_table
FROM product_analytics_staging.agg_table_usage_rolling u
JOIN product_analytics_staging.dim_cost_hierarchies h
  USING (date, cost_hierarchy_id)
WHERE h.grouping_columns = 'team'
  AND h.team = :team
  AND u.date BETWEEN :date.min AND :date.max
ORDER BY u.date DESC
