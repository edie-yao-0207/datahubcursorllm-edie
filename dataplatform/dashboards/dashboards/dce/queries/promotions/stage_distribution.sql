-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: Workflow Velocity
-- View: Current Stage Distribution
-- =============================================================================
-- Visualization: Bar Chart
-- Description: Breakdown of promotions by current stage
-- =============================================================================

WITH latest AS (
  SELECT MAX(date) AS max_date
  FROM product_analytics_staging.agg_signal_promotion_daily_metrics
  WHERE date BETWEEN :date.min AND :date.max
    AND grouping_columns = 'stage'
)

SELECT
  COALESCE(s.name, CAST(m.current_stage AS STRING)) AS current_stage,
  m.monthly.total_trials AS count
FROM product_analytics_staging.agg_signal_promotion_daily_metrics m
LEFT JOIN definitions.promotion_stage s ON s.id = m.current_stage
WHERE m.date = (SELECT max_date FROM latest)
  AND m.grouping_columns = 'stage'
ORDER BY m.current_stage
