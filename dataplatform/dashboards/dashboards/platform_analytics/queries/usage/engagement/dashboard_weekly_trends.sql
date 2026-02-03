-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Usage - Engagement
-- View: Dashboard Weekly Trends
-- =============================================================================
-- Visualization: Line Chart (multi-series)
-- Title: "Weekly Dashboard Activity"
-- Description: Track dashboard engagement trends by week. Useful for spotting
--              adoption patterns, seasonal usage, and the impact of updates.
--
-- X-axis: week
-- Y-axis: events / unique_users (dual axis or separate charts)
-- Series: dashboard_title (color by)
--
-- Annotations:
--   - Mark dashboard release dates or major updates
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
)

SELECT
  CAST(DATE_TRUNC('week', e.date) AS DATE) AS week,
  m.dashboard_title,
  COUNT(*) AS events,
  COUNT(DISTINCT e.user_email) AS unique_users,
  COUNT(DISTINCT e.date) AS active_days,
  -- Week-over-week comparison
  ROUND(COUNT(*) / 7.0, 1) AS avg_daily_events
FROM auditlog.fct_databricks_dashboard_events e
JOIN dashboard_metadata m
  USING (dashboard_id)
WHERE e.date BETWEEN :date.min AND :date.max
GROUP BY 1, 2
ORDER BY week DESC, events DESC

