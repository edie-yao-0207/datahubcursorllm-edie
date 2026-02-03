-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: User Analytics - Platform Trends
-- View: Platform Usage Trends
-- =============================================================================
-- Visualization: Multi-line chart
-- Title: "Platform Usage Trends Over Time"
-- Description: Shows how platform adoption evolves - are more users using both
--              SQL and dashboards? Is overall engagement growing?
--
-- Line Chart:
--   X-Axis: date
--   Y-Axis: user_count or pct
--   Lines: total_active_users, sql_only_users, dashboard_only_users, both_users
--
-- Annotations:
--   - Growing "both_users" indicates successful cross-platform adoption
--   - Watch for declining trends that may indicate churn
-- =============================================================================

WITH overall_metrics AS (
  SELECT 
    a.date,
    a.monthly.users.total AS total_active_users,
    a.monthly.users.sql_users,
    a.monthly.users.dashboard_users,
    a.monthly.users.sql_only AS sql_only_users,
    a.monthly.users.dashboard_only AS dashboard_only_users,
    a.monthly.users.both AS both_users,
    a.monthly.sql.queries AS total_sql_queries,
    a.monthly.dashboard.events AS total_dashboard_events,
    a.monthly.combined.total_interactions,
    a.monthly.combined.delta_users AS delta_active_users
  FROM product_analytics_staging.agg_platform_usage_rolling a
  JOIN product_analytics_staging.dim_user_hierarchies h
    USING (date, user_hierarchy_id)
  WHERE h.grouping_columns = 'overall'
    AND a.date BETWEEN :date.min AND :date.max
)

SELECT 
  CAST(date AS DATE) AS date,
  -- User counts
  total_active_users,
  sql_only_users,
  dashboard_only_users,
  both_users,
  
  -- User percentages (0-1 range for UI percent formatting)
  ROUND(sql_only_users * 1.0 / NULLIF(total_active_users, 0), 4) AS pct_sql_only,
  ROUND(dashboard_only_users * 1.0 / NULLIF(total_active_users, 0), 4) AS pct_dashboard_only,
  ROUND(both_users * 1.0 / NULLIF(total_active_users, 0), 4) AS pct_both,
  
  -- Activity metrics
  total_sql_queries,
  total_dashboard_events,
  total_interactions,
  
  -- Per-user averages
  ROUND(total_sql_queries * 1.0 / NULLIF(sql_users, 0), 1) AS avg_queries_per_sql_user,
  ROUND(total_dashboard_events * 1.0 / NULLIF(dashboard_users, 0), 1) AS avg_events_per_dashboard_user,
  ROUND(total_interactions * 1.0 / NULLIF(total_active_users, 0), 1) AS avg_interactions_per_user,
  
  -- Growth
  delta_active_users,
  
  -- 7-day moving averages
  AVG(total_active_users) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS active_users_7d_avg,
  AVG(sql_users) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sql_users_7d_avg,
  AVG(dashboard_users) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS dashboard_users_7d_avg,
  AVG(both_users) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS both_users_7d_avg

FROM overall_metrics
ORDER BY date
