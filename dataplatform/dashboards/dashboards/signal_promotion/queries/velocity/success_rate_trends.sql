-- =============================================================================
-- Dashboard: Signal Promotion Metrics
-- Tab: Velocity
-- View: Success Rate Trends
-- =============================================================================
-- Visualization: Line Chart
-- Description: Success rate over time at different rolling windows
-- =============================================================================

SELECT
  CAST(m.date AS DATE) AS date,
  m.daily.graduation_rate AS daily_graduation_rate,
  m.weekly.graduation_rate AS weekly_graduation_rate,
  m.monthly.graduation_rate AS monthly_graduation_rate,
  m.quarterly.graduation_rate AS quarterly_graduation_rate,
  m.yearly.graduation_rate AS yearly_graduation_rate
FROM product_analytics_staging.agg_signal_promotion_daily_metrics m
WHERE m.date BETWEEN :date.min AND :date.max
  AND m.grouping_columns = 'overall'
ORDER BY m.date DESC

