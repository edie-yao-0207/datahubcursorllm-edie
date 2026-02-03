-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Usage - Engagement
-- View: FirmwareVDP Dashboard Usage Summary
-- =============================================================================
-- Visualization: Scorecard + Bar Chart
-- Title: "Dashboard Engagement Summary"
-- Description: Aggregate metrics for FirmwareVDP dashboard usage.
--              Track total views, unique users, and engagement trends.
--
-- Scorecards: total_events_30d, unique_users_30d, most_active_dashboard
-- Chart: Horizontal bar of total_events by dashboard_title
--
-- Annotations (for bar chart):
--   - Vertical line at 100 events (label: "Good Adoption", color: green)
--   - Vertical line at 50 events (label: "Minimum Viable Usage", color: yellow)
-- =============================================================================

WITH dashboard_metadata AS (
  SELECT
    dashboard_id,
    dashboard_title,
    dashboard_url
  FROM (VALUES
    ('01f0a83a866f1de78da43e4d779640b4', 'Trace Explorer', 'https://samsara-dev-us-west-2.cloud.databricks.com/dashboardsv3/01f0a83a866f1de78da43e4d779640b4/published?o=5924096274798303'),
    ('01f02d133db1173a8ba26f8bba1cdc35', 'Coverage', 'https://samsara-dev-us-west-2.cloud.databricks.com/dashboardsv3/01f02d133db1173a8ba26f8bba1cdc35/published?o=5924096274798303'),
    ('01efd424550c107aae9d8c7edcebff18', 'Data Dashboard', 'https://samsara-dev-us-west-2.cloud.databricks.com/dashboardsv3/01efd424550c107aae9d8c7edcebff18/published?o=5924096274798303'),
    ('01efbcc3b2181e5688194ba2d0f7100c', 'Release Qualifier', 'https://samsara-dev-us-west-2.cloud.databricks.com/dashboardsv3/01efbcc3b2181e5688194ba2d0f7100c/published?o=5924096274798303'),
    ('01f0d1bfc7271cb9939bccb49eb3d88e', 'Signal Promotion Service V2', 'https://samsara-dev-us-west-2.cloud.databricks.com/dashboardsv3/01f0d1bfc7271cb9939bccb49eb3d88e/published?o=5924096274798303'),
    ('01f0bb572dd2117e8e477d9b5e05755c', 'DCE Program Metrics', 'https://samsara-dev-us-west-2.cloud.databricks.com/dashboardsv3/01f0bb572dd2117e8e477d9b5e05755c/published?o=5924096274798303'),
    ('01f0d106fa4613bfbe3d376d3e89ecf2', 'Cost Dashboard V2', 'https://samsara-dev-us-west-2.cloud.databricks.com/dashboardsv3/01f0d106fa4613bfbe3d376d3e89ecf2/published?o=5924096274798303'),
    ('01f0c03521bb16419b4e8f0cce0ff67d', 'Automotive On-Call Dashboard', 'https://samsara-dev-us-west-2.cloud.databricks.com/dashboardsv3/01f0c03521bb16419b4e8f0cce0ff67d/published?o=5924096274798303')
  ) AS t(dashboard_id, dashboard_title, dashboard_url)
)

SELECT
  m.dashboard_title,
  m.dashboard_url,
  COUNT(*) AS total_events,
  COUNT(DISTINCT e.user_email) AS unique_users,
  COUNT(DISTINCT e.date) AS active_days,
  -- Engagement ratio: events per user
  ROUND(COUNT(*) / NULLIF(COUNT(DISTINCT e.user_email), 0), 1) AS events_per_user,
  -- Most recent activity
  CAST(MAX(e.date) AS DATE) AS last_activity_date
FROM auditlog.fct_databricks_dashboard_events e
JOIN dashboard_metadata m
  USING (dashboard_id)
WHERE e.date BETWEEN :date.min AND :date.max
GROUP BY m.dashboard_title, m.dashboard_url
ORDER BY total_events DESC
