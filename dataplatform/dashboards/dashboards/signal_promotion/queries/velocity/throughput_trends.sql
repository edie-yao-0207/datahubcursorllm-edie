-- =============================================================================
-- Dashboard: Signal Promotion Metrics
-- Tab: Overview / Velocity
-- View: Throughput Trends
-- =============================================================================
-- Visualization: Line Chart
-- Description: Total promotion trials over time at different rolling windows
-- =============================================================================

SELECT
  CAST(m.date AS DATE) AS date,
  m.daily.total_trials AS daily_trials,
  m.weekly.total_trials AS weekly_trials,
  m.monthly.total_trials AS monthly_trials,
  m.quarterly.total_trials AS quarterly_trials,
  m.yearly.total_trials AS yearly_trials
FROM product_analytics_staging.agg_signal_promotion_daily_metrics m
WHERE m.date BETWEEN :date.min AND :date.max
  AND m.grouping_columns = 'overall'
ORDER BY m.date DESC

