-- =============================================================================
-- Dashboard: Signal Promotion Metrics
-- Tab: AI Promotions
-- View: AI Promotions by Stage
-- =============================================================================
-- Visualization: Bar Chart
-- Description: Distribution of AI promotions across pipeline stages
-- =============================================================================

WITH ai_promotions AS (
  SELECT
    current.stage AS stage_id
  FROM product_analytics_staging.fct_signal_promotion_latest_state
  WHERE data_source = 20  -- DATA_SOURCE_AI_ML
)

SELECT
  ps.name AS stage,
  a.stage_id,
  COUNT(*) AS count,
  -- Percent of total as 0-1 decimal (UI formats as percentage)
  ROUND(COUNT(*) * 1.0 / SUM(COUNT(*)) OVER (), 3) AS percent_of_total
FROM ai_promotions a
LEFT JOIN definitions.promotion_stage ps ON ps.id = a.stage_id
GROUP BY ps.name, a.stage_id
ORDER BY 
  CASE a.stage_id
    WHEN 5 THEN 1  -- GA
    WHEN 4 THEN 2  -- BETA
    WHEN 3 THEN 3  -- ALPHA
    WHEN 2 THEN 4  -- PRE_ALPHA
    WHEN 0 THEN 5  -- INITIAL
    WHEN 1 THEN 6  -- FAILED
    ELSE 7
  END

