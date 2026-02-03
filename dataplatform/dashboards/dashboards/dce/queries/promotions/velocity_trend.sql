-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: Workflow Velocity
-- View: Promotion Throughput Trend
-- =============================================================================
-- Visualization: Line Chart
-- Description: Trials and graduations over time
-- =============================================================================

SELECT
  CAST(date AS DATE) AS date,
  monthly.total_trials AS monthly_trials,
  monthly.graduations AS monthly_graduations
FROM product_analytics_staging.agg_signal_promotion_daily_metrics
WHERE date BETWEEN :date.min AND :date.max
  AND grouping_columns = 'overall'
ORDER BY date
