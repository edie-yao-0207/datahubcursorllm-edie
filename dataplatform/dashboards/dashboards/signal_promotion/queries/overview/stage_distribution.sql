-- =============================================================================
-- Dashboard: Signal Promotion Metrics
-- Tab: Overview
-- View: Stage Distribution
-- =============================================================================
-- Visualization: Bar Chart
-- Description: Distribution of promotions by current stage
-- =============================================================================

WITH latest_date AS (
  SELECT MAX(date) AS max_date
  FROM product_analytics_staging.agg_signal_promotion_daily_metrics
  WHERE date BETWEEN :date.min AND :date.max
    AND grouping_columns = 'stage'
)

SELECT
  ps.name AS current_stage,
  m.current_stage AS current_stage_id,
  m.monthly.total_trials AS count,
  m.monthly.total_trials / SUM(m.monthly.total_trials) OVER () AS percent_of_total
FROM product_analytics_staging.agg_signal_promotion_daily_metrics m
LEFT JOIN definitions.promotion_stage ps ON ps.id = m.current_stage
WHERE m.date = (SELECT max_date FROM latest_date)
  AND m.grouping_columns = 'stage'
  AND m.current_stage IS NOT NULL
ORDER BY 
  CASE m.current_stage
    WHEN 5 THEN 1  -- GA
    WHEN 4 THEN 2  -- BETA
    WHEN 3 THEN 3  -- ALPHA
    WHEN 2 THEN 4  -- PRE_ALPHA
    WHEN 0 THEN 5  -- INITIAL
    WHEN 1 THEN 6  -- FAILED
    ELSE 7
  END

