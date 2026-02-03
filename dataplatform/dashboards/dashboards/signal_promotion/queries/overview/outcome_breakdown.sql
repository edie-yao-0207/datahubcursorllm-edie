-- =============================================================================
-- Dashboard: Signal Promotion Metrics
-- Tab: Overview
-- View: Outcome Breakdown
-- =============================================================================
-- Visualization: Bar Chart
-- Description: Distribution of promotion outcomes in the 30-day window
-- =============================================================================

WITH latest_date AS (
  SELECT MAX(date) AS max_date
  FROM product_analytics_staging.agg_signal_promotion_daily_metrics
  WHERE date BETWEEN :date.min AND :date.max
    AND grouping_columns = 'outcome'
)

SELECT
  m.promotion_outcome AS outcome,
  m.monthly.total_trials AS count,
  m.monthly.total_trials / SUM(m.monthly.total_trials) OVER () AS percent_of_total
FROM product_analytics_staging.agg_signal_promotion_daily_metrics m
WHERE m.date = (SELECT max_date FROM latest_date)
  AND m.grouping_columns = 'outcome'
  AND m.promotion_outcome IS NOT NULL
ORDER BY 
  CASE m.promotion_outcome
    WHEN 'SUCCESS' THEN 1
    WHEN 'IN_PROGRESS' THEN 2
    WHEN 'ALPHA_ROLLBACK' THEN 3
    WHEN 'PRODUCTION_FAILURE' THEN 4
    ELSE 5
  END

