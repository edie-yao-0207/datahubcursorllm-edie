-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: Diagnostic Breadth
-- View: Signal Growth Over Time
-- =============================================================================
-- Visualization: Line Chart
-- Description: Track distinct promoted signals over time (MoM, QoQ growth)
-- =============================================================================

SELECT
  CAST(date AS DATE) AS date,
  daily.unique_signals AS daily_unique_signals,
  monthly.total_trials AS monthly_trials,
  monthly.graduations AS monthly_graduations
FROM product_analytics_staging.agg_signal_promotion_daily_metrics
WHERE date BETWEEN :date.min AND :date.max
  AND grouping_columns = 'overall'
ORDER BY date
