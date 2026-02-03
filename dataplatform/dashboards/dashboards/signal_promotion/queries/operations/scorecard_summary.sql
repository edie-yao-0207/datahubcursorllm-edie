-- =============================================================================
-- Dashboard: Signal Promotion Metrics
-- Tab: Operations
-- View: Scorecard Summary
-- =============================================================================
-- Visualization: Counter cards
-- Description: Key operational metrics for the current period
-- =============================================================================

WITH latest AS (
  SELECT MAX(date) AS max_date
  FROM product_analytics_staging.agg_signal_promotion_daily_metrics
  WHERE date BETWEEN :date.min AND :date.max
    AND grouping_columns = 'overall'
),

stuck_count AS (
  SELECT COUNT(*) AS stuck_promotions
  FROM product_analytics_staging.fct_signal_promotion_latest_state
  WHERE promotion_outcome = 'IN_PROGRESS'
    AND current.stage IN (2, 3, 4)
    AND time_in_current_stage_seconds > 7 * 86400
)

SELECT
  m.monthly.total_trials AS monthly_trials,
  m.monthly.graduations AS monthly_graduations,
  m.monthly.graduation_rate AS graduation_rate,
  m.monthly.beta_rollbacks AS beta_rollbacks,
  m.monthly.alpha_rollbacks AS alpha_rollbacks,
  m.daily.unique_signals AS daily_unique_signals,
  ROUND(m.monthly.avg_time_to_graduation_seconds / 86400.0, 1) AS avg_days_to_graduation,
  s.stuck_promotions
FROM product_analytics_staging.agg_signal_promotion_daily_metrics m
CROSS JOIN stuck_count s
WHERE m.date = (SELECT max_date FROM latest)
  AND m.grouping_columns = 'overall'

