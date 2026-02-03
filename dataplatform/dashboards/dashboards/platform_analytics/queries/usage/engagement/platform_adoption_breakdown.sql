-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: User Analytics - Platform Adoption
-- View: Platform Adoption Breakdown
-- =============================================================================
-- Visualization: Stacked bar chart + pie chart
-- Title: "Platform Adoption by Department/Job Family"
-- Description: Shows how users engage with the platform - SQL only, dashboards only,
--              or both. Helps identify adoption gaps.
--
-- Stacked Bar Chart:
--   X-Axis: department or job_family
--   Y-Axis: user_count (stacked by platform_usage_type)
--   Colors: SQL_ONLY (blue), DASHBOARD_ONLY (purple), BOTH (green)
--
-- Annotations:
--   - "BOTH" users are power users who leverage multiple platform capabilities
--   - High SQL_ONLY may indicate dashboard training opportunities
--   - High DASHBOARD_ONLY may indicate SQL skill development opportunities
-- =============================================================================

WITH latest_metrics AS (
  SELECT 
    h.department,
    h.job_family,
    h.job_family_group,
    h.grouping_columns,
    a.monthly.users.total AS total_active_users,
    a.monthly.users.sql_users,
    a.monthly.users.dashboard_users,
    a.monthly.users.sql_only AS sql_only_users,
    a.monthly.users.dashboard_only AS dashboard_only_users,
    a.monthly.users.both AS both_users,
    a.monthly.combined.total_interactions,
    a.monthly.sql.queries AS total_sql_queries,
    a.monthly.dashboard.events AS total_dashboard_events
  FROM product_analytics_staging.agg_platform_usage_rolling a
  JOIN product_analytics_staging.dim_user_hierarchies h
    USING (date, user_hierarchy_id)
  WHERE a.date = (SELECT MAX(date) FROM product_analytics_staging.agg_platform_usage_rolling WHERE date BETWEEN :date.min AND :date.max)
    AND a.monthly.users.total > 0
)

-- By Department
SELECT 
  'department' AS grouping_level,
  department AS group_name,
  job_family_group,
  total_active_users,
  sql_only_users,
  dashboard_only_users,
  both_users,
  -- Percentages (0-1 range for UI percent formatting)
  ROUND(sql_only_users * 1.0 / NULLIF(total_active_users, 0), 4) AS pct_sql_only,
  ROUND(dashboard_only_users * 1.0 / NULLIF(total_active_users, 0), 4) AS pct_dashboard_only,
  ROUND(both_users * 1.0 / NULLIF(total_active_users, 0), 4) AS pct_both,
  -- Activity metrics
  total_sql_queries,
  total_dashboard_events,
  total_interactions,
  ROUND(total_sql_queries * 1.0 / NULLIF(sql_users, 0), 1) AS avg_queries_per_sql_user,
  ROUND(total_dashboard_events * 1.0 / NULLIF(dashboard_users, 0), 1) AS avg_events_per_dashboard_user
FROM latest_metrics
WHERE grouping_columns = 'job_family_group.job_family.department'
  AND department IS NOT NULL

UNION ALL

-- By Job Family
SELECT 
  'job_family' AS grouping_level,
  job_family AS group_name,
  job_family_group,
  total_active_users,
  sql_only_users,
  dashboard_only_users,
  both_users,
  ROUND(sql_only_users * 1.0 / NULLIF(total_active_users, 0), 4) AS pct_sql_only,
  ROUND(dashboard_only_users * 1.0 / NULLIF(total_active_users, 0), 4) AS pct_dashboard_only,
  ROUND(both_users * 1.0 / NULLIF(total_active_users, 0), 4) AS pct_both,
  total_sql_queries,
  total_dashboard_events,
  total_interactions,
  ROUND(total_sql_queries * 1.0 / NULLIF(sql_users, 0), 1) AS avg_queries_per_sql_user,
  ROUND(total_dashboard_events * 1.0 / NULLIF(dashboard_users, 0), 1) AS avg_events_per_dashboard_user
FROM latest_metrics
WHERE grouping_columns = 'job_family_group.job_family'
  AND job_family IS NOT NULL

UNION ALL

-- By Job Family Group (highest level)
SELECT 
  'job_family_group' AS grouping_level,
  job_family_group AS group_name,
  job_family_group,
  total_active_users,
  sql_only_users,
  dashboard_only_users,
  both_users,
  ROUND(sql_only_users * 1.0 / NULLIF(total_active_users, 0), 4) AS pct_sql_only,
  ROUND(dashboard_only_users * 1.0 / NULLIF(total_active_users, 0), 4) AS pct_dashboard_only,
  ROUND(both_users * 1.0 / NULLIF(total_active_users, 0), 4) AS pct_both,
  total_sql_queries,
  total_dashboard_events,
  total_interactions,
  ROUND(total_sql_queries * 1.0 / NULLIF(sql_users, 0), 1) AS avg_queries_per_sql_user,
  ROUND(total_dashboard_events * 1.0 / NULLIF(dashboard_users, 0), 1) AS avg_events_per_dashboard_user
FROM latest_metrics
WHERE grouping_columns = 'job_family_group'
  AND job_family_group IS NOT NULL

ORDER BY grouping_level, total_active_users DESC
