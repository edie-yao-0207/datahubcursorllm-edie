-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: Diagnostic Breadth
-- View: Breadth Snapshot (KPIs)
-- =============================================================================
-- Visualization: Counter cards
-- Description: Current breadth metrics - unique signals promoted
-- =============================================================================

WITH latest AS (
  SELECT MAX(date) AS max_date
  FROM product_analytics_staging.agg_signal_promotion_daily_metrics
  WHERE date BETWEEN :date.min AND :date.max
    AND grouping_columns = 'overall'
)

SELECT
  daily.unique_signals AS daily_unique_signals,
  monthly.total_trials AS monthly_trials,
  monthly.graduations AS monthly_graduations
FROM product_analytics_staging.agg_signal_promotion_daily_metrics
WHERE date = (SELECT max_date FROM latest)
  AND grouping_columns = 'overall'
