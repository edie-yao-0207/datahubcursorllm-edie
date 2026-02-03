-- =============================================================================
-- Dashboard: Signal Promotion Metrics
-- Tab: Velocity
-- View: Throughput by User
-- =============================================================================
-- Visualization: Table
-- Description: Top users by promotion volume in the 30-day window
-- =============================================================================

WITH latest_date AS (
  SELECT MAX(date) AS max_date
  FROM product_analytics_staging.agg_signal_promotion_daily_metrics
  WHERE date BETWEEN :date.min AND :date.max
    AND grouping_columns = 'user'
),

users_latest AS (
  SELECT *
  FROM datamodel_platform.dim_users
  WHERE date = (SELECT MAX(date) FROM datamodel_platform.dim_users)
)

SELECT
  u.name AS user_name,
  u.email AS user_email,
  m.user_id,
  m.monthly.total_trials AS total_trials,
  m.monthly.graduations AS graduations,
  m.monthly.graduation_rate AS graduation_rate,
  m.monthly.beta_rollbacks AS beta_rollbacks,
  m.monthly.alpha_rollbacks AS alpha_rollbacks,
  -- Daily unique counts (rolling window unique counts not available)
  m.daily.unique_signals AS unique_signals,
  m.daily.unique_populations AS unique_populations,
  ROUND(m.monthly.avg_time_to_graduation_seconds / 86400.0, 1) AS avg_days_to_graduation
FROM product_analytics_staging.agg_signal_promotion_daily_metrics m
LEFT JOIN users_latest u ON u.user_id = m.user_id
WHERE m.date = (SELECT max_date FROM latest_date)
  AND m.grouping_columns = 'user'
  AND m.user_id IS NOT NULL
ORDER BY m.monthly.total_trials DESC
LIMIT 50

