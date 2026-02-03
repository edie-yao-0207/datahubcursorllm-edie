-- =============================================================================
-- Dashboard: Signal Promotion Metrics
-- Tab: Overview
-- View: Metrics Snapshot
-- =============================================================================
-- Visualization: Counter cards
-- Description: Current snapshot of key promotion metrics
-- =============================================================================

WITH latest_date AS (
  SELECT MAX(date) AS max_date
  FROM product_analytics_staging.agg_signal_promotion_daily_metrics
  WHERE date BETWEEN :date.min AND :date.max
    AND grouping_columns = 'overall'
)

SELECT
  m.monthly.total_trials AS monthly_total_trials,
  m.monthly.graduations AS monthly_graduations,
  m.monthly.graduation_rate AS monthly_graduation_rate,
  m.monthly.beta_rollback_rate AS monthly_beta_rollback_rate,
  m.monthly.alpha_rollback_rate AS monthly_alpha_rollback_rate,
  -- Daily unique counts (rolling window unique counts not available)
  m.daily.unique_signals AS daily_unique_signals,
  m.daily.unique_populations AS daily_unique_populations,
  ROUND(m.monthly.avg_time_to_graduation_seconds / 86400.0, 1) AS avg_time_to_graduation_days,
  m.quarterly.total_trials AS quarterly_total_trials,
  m.quarterly.graduation_rate AS quarterly_graduation_rate,
  m.yearly.total_trials AS yearly_total_trials,
  m.yearly.graduation_rate AS yearly_graduation_rate
FROM product_analytics_staging.agg_signal_promotion_daily_metrics m
WHERE m.date = (SELECT max_date FROM latest_date)
  AND m.grouping_columns = 'overall'

