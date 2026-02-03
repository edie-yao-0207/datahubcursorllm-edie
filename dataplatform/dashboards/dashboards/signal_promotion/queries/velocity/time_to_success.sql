-- =============================================================================
-- Dashboard: Signal Promotion Metrics
-- Tab: Velocity
-- View: Time to Graduation
-- =============================================================================
-- Visualization: Line Chart
-- Description: Average time to reach graduation (BETA) in days
-- =============================================================================

SELECT
  CAST(m.date AS DATE) AS date,
  ROUND(m.daily.avg_time_to_graduation_seconds / 86400.0, 1) AS daily_avg_days,
  ROUND(m.weekly.avg_time_to_graduation_seconds / 86400.0, 1) AS weekly_avg_days,
  ROUND(m.monthly.avg_time_to_graduation_seconds / 86400.0, 1) AS monthly_avg_days,
  ROUND(m.quarterly.avg_time_to_graduation_seconds / 86400.0, 1) AS quarterly_avg_days,
  ROUND(m.yearly.avg_time_to_graduation_seconds / 86400.0, 1) AS yearly_avg_days
FROM product_analytics_staging.agg_signal_promotion_daily_metrics m
WHERE m.date BETWEEN :date.min AND :date.max
  AND m.grouping_columns = 'overall'
ORDER BY m.date DESC

