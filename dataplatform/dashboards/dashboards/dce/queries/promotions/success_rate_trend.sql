-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: Workflow Velocity
-- View: Graduation Rate Trend
-- =============================================================================
-- Visualization: Line Chart
-- Description: Success rate over time
-- =============================================================================

SELECT
  CAST(date AS DATE) AS date,
  monthly.graduation_rate AS monthly_graduation_rate
FROM product_analytics_staging.agg_signal_promotion_daily_metrics
WHERE date BETWEEN :date.min AND :date.max
  AND grouping_columns = 'overall'
ORDER BY date
