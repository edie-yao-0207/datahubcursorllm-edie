-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: Workflow Velocity
-- View: Outcome Distribution (30-Day)
-- =============================================================================
-- Visualization: Bar Chart
-- Description: Breakdown of promotion outcomes
-- =============================================================================

WITH latest AS (
  SELECT MAX(date) AS max_date
  FROM product_analytics_staging.agg_signal_promotion_daily_metrics
  WHERE date BETWEEN :date.min AND :date.max
    AND grouping_columns = 'outcome'
)

SELECT
  m.promotion_outcome AS outcome,
  m.monthly.total_trials AS count
FROM product_analytics_staging.agg_signal_promotion_daily_metrics m
WHERE m.date = (SELECT max_date FROM latest)
  AND m.grouping_columns = 'outcome'
ORDER BY m.monthly.total_trials DESC
