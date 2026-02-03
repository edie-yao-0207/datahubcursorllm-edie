-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: Workflow Velocity
-- View: Velocity Snapshot (KPIs)
-- =============================================================================
-- Visualization: Counter cards
-- Description: Key velocity metrics - throughput and time to graduation
-- =============================================================================

WITH latest AS (
  SELECT MAX(date) AS max_date
  FROM product_analytics_staging.agg_signal_promotion_daily_metrics
  WHERE date BETWEEN :date.min AND :date.max
    AND grouping_columns = 'overall'
)

SELECT
  monthly.total_trials AS monthly_trials,
  monthly.graduations AS monthly_graduations,
  monthly.graduation_rate AS graduation_rate,
  ROUND(monthly.avg_time_to_graduation_seconds / 86400.0, 1) AS avg_days_to_graduation,
  ROUND(monthly.avg_time_in_current_stage_seconds / 86400.0, 1) AS avg_days_in_stage
FROM product_analytics_staging.agg_signal_promotion_daily_metrics
WHERE date = (SELECT max_date FROM latest)
  AND grouping_columns = 'overall'
