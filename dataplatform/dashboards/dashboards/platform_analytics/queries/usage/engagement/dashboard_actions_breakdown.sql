-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Usage - Engagement
-- View: Dashboard Actions Breakdown
-- =============================================================================
-- Visualization: Bar Chart (stacked) + Table
-- Title: "Dashboard Events by Action Type"
-- Description: Understand what users are doing with dashboards. Views vs edits
--              vs exports helps gauge engagement depth and identify use patterns.
--
-- Chart: Stacked bar chart, x=action_name, y=count, color=dashboard_title
-- Table: action_name, count, percent_of_total
--
-- Action types from Databricks audit logs:
--   - getPublishedDashboard (view published)
--   - getDashboard (view draft)
--   - updateDashboard (edit)
--   - cloneDashboard (copy)
--   - runQuery (execute query in dashboard)
--   - etc.
-- =============================================================================

WITH dashboard_metadata AS (
  SELECT
    dashboard_id,
    dashboard_title
  FROM (VALUES
    ('01f0a83a866f1de78da43e4d779640b4', 'Trace Explorer'),
    ('01f02d133db1173a8ba26f8bba1cdc35', 'Coverage'),
    ('01efd424550c107aae9d8c7edcebff18', 'Data Dashboard'),
    ('01efbcc3b2181e5688194ba2d0f7100c', 'Release Qualifier'),
    ('01f0d1bfc7271cb9939bccb49eb3d88e', 'Signal Promotion Service V2'),
    ('01f0bb572dd2117e8e477d9b5e05755c', 'DCE Program Metrics'),
    ('01f0d106fa4613bfbe3d376d3e89ecf2', 'Cost Dashboard V2'),
    ('01f0c03521bb16419b4e8f0cce0ff67d', 'Automotive On-Call Dashboard')
  ) AS t(dashboard_id, dashboard_title)
),

action_counts AS (
  SELECT
    e.action_name,
    m.dashboard_title,
    COUNT(*) AS event_count,
    COUNT(DISTINCT e.user_email) AS unique_users
  FROM auditlog.fct_databricks_dashboard_events e
  JOIN dashboard_metadata m
    USING (dashboard_id)
  WHERE e.date BETWEEN :date.min AND :date.max
  GROUP BY e.action_name, m.dashboard_title
)

SELECT
  action_name,
  dashboard_title,
  event_count,
  unique_users,
  -- Calculate percent of total for this dashboard (0-1 range for UI percent formatting)
  ROUND(event_count * 1.0 / SUM(event_count) OVER (PARTITION BY dashboard_title), 4) AS percent_of_dashboard,
  -- Calculate percent of total across all dashboards
  ROUND(event_count * 1.0 / SUM(event_count) OVER (), 4) AS percent_of_total
FROM action_counts
ORDER BY event_count DESC

