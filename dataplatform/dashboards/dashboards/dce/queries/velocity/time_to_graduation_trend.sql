-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: Workflow Velocity
-- View: Time to Graduation Trend
-- =============================================================================
-- Visualization: Line Chart
-- Description: Average time from request to graduation over time
-- =============================================================================

SELECT
  CAST(date AS DATE) AS date,
  ROUND(monthly.avg_time_to_graduation_seconds / 86400.0, 1) AS avg_days_to_graduation,
  ROUND(monthly.avg_time_in_current_stage_seconds / 86400.0, 1) AS avg_days_in_stage
FROM product_analytics_staging.agg_signal_promotion_daily_metrics
WHERE date BETWEEN :date.min AND :date.max
  AND grouping_columns = 'overall'
ORDER BY date
