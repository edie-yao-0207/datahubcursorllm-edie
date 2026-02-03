-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Usage - Engagement
-- View: Overall Top Dashboard Users
-- =============================================================================
-- Visualization: Table
-- Title: "Top Users Across All Dashboards"
-- Description: Ranked list of most active users across all tracked dashboards.
--              Identifies power users who engage with multiple dashboards.
--
-- Table Columns:
--   - user_email, total_events, dashboards_used, active_days, most_used_dashboard, rank
--
-- Use Case: Find data champions, potential trainers, users for feedback
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

-- Per-user-per-dashboard event counts
user_dashboard_counts AS (
  SELECT
    e.user_email,
    e.dashboard_id,
    m.dashboard_title,
    COUNT(*) AS events_per_dashboard
  FROM auditlog.fct_databricks_dashboard_events e
  JOIN dashboard_metadata m
    USING (dashboard_id)
  WHERE e.date BETWEEN :date.min AND :date.max
  GROUP BY e.user_email, e.dashboard_id, m.dashboard_title
),

-- Active days per user (from original events, not a constant)
user_active_days AS (
  SELECT
    e.user_email,
    COUNT(DISTINCT e.date) AS active_days
  FROM auditlog.fct_databricks_dashboard_events e
  JOIN dashboard_metadata m
    USING (dashboard_id)
  WHERE e.date BETWEEN :date.min AND :date.max
  GROUP BY e.user_email
),

-- Most-used dashboard per user (using ROW_NUMBER to pick top)
user_most_used AS (
  SELECT
    user_email,
    dashboard_title AS most_used_dashboard
  FROM (
    SELECT
      user_email,
      dashboard_title,
      ROW_NUMBER() OVER (PARTITION BY user_email ORDER BY events_per_dashboard DESC) AS rn
    FROM user_dashboard_counts
  )
  WHERE rn = 1
),

-- Aggregate to user level (GROUP BY only user_email)
user_totals AS (
  SELECT
    user_email,
    SUM(events_per_dashboard) AS total_events,
    COUNT(DISTINCT dashboard_id) AS dashboards_used
  FROM user_dashboard_counts
  GROUP BY user_email
),

-- Combine all user metrics
user_summary AS (
  SELECT
    t.user_email,
    t.total_events,
    t.dashboards_used,
    a.active_days,
    m.most_used_dashboard
  FROM user_totals t
  JOIN user_active_days a USING (user_email)
  JOIN user_most_used m USING (user_email)
),

ranked AS (
  SELECT
    *,
    RANK() OVER (ORDER BY total_events DESC) AS user_rank
  FROM user_summary
)

SELECT
  user_email,
  total_events,
  dashboards_used,
  active_days,
  most_used_dashboard,
  user_rank
FROM ranked
WHERE user_rank <= 25  -- Top 25 users overall
ORDER BY user_rank

