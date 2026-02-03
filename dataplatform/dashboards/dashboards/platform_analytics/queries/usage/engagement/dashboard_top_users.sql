-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Usage - Engagement
-- View: Top Dashboard Users
-- =============================================================================
-- Visualization: Table
-- Title: "Top Users by Dashboard"
-- Description: Ranked list of most active users per dashboard. Identifies power
--              users and helps understand adoption patterns.
--
-- Table Columns:
--   - dashboard_title, user_email, event_count, rank, dashboard_url
--
-- Use Case: Identify champions, find users to interview for feedback,
--           spot potential training needs for low-engagement users
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
),

user_counts AS (
  SELECT
    e.dashboard_id,
    e.user_email,
    COUNT(*) AS event_count,
    COUNT(DISTINCT e.date) AS active_days,
    CAST(MAX(e.date) AS DATE) AS last_active
  FROM auditlog.fct_databricks_dashboard_events e
  WHERE e.date BETWEEN :date.min AND :date.max
    AND e.dashboard_id IN (
      '01f0a83a866f1de78da43e4d779640b4',
      '01f02d133db1173a8ba26f8bba1cdc35',
      '01efd424550c107aae9d8c7edcebff18',
      '01efbcc3b2181e5688194ba2d0f7100c',
      '01f0d1bfc7271cb9939bccb49eb3d88e',
      '01f0bb572dd2117e8e477d9b5e05755c',
      '01f0d106fa4613bfbe3d376d3e89ecf2',
      '01f0c03521bb16419b4e8f0cce0ff67d'
    )
  GROUP BY e.dashboard_id, e.user_email
),

ranked AS (
  SELECT
    *,
    RANK() OVER (PARTITION BY dashboard_id ORDER BY event_count DESC) AS user_rank
  FROM user_counts
)

SELECT
  m.dashboard_title,
  r.user_email,
  r.event_count,
  r.active_days,
  r.last_active,
  r.user_rank,
  m.dashboard_url
FROM ranked r
JOIN dashboard_metadata m
  USING (dashboard_id)
WHERE r.user_rank <= 10  -- Top 10 users per dashboard
ORDER BY m.dashboard_title, r.user_rank

