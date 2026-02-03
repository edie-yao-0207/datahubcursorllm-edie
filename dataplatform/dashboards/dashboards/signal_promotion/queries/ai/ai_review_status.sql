-- =============================================================================
-- Dashboard: Signal Promotion Metrics
-- Tab: AI Promotions
-- View: AI Review Status Breakdown
-- =============================================================================
-- Visualization: Bar Chart
-- Description: Current review status of AI promotions in the pipeline
-- =============================================================================

WITH ai_promotions AS (
  SELECT
    current.stage AS stage_id,
    current.status AS status_id,
    promotion_outcome
  FROM product_analytics_staging.fct_signal_promotion_latest_state
  WHERE data_source = 20  -- DATA_SOURCE_AI_ML
)

SELECT
  ps.name AS stage,
  pst.name AS status,
  COUNT(*) AS count,
  -- Percent of total as 0-1 decimal (UI formats as percentage)
  ROUND(COUNT(*) * 1.0 / SUM(COUNT(*)) OVER (), 3) AS percent_of_total
FROM ai_promotions a
LEFT JOIN definitions.promotion_stage ps ON ps.id = a.stage_id
LEFT JOIN definitions.promotion_status pst ON pst.id = a.status_id
WHERE a.stage_id NOT IN (1, 5)  -- Exclude FAILED and GA (terminal states)
GROUP BY ps.name, pst.name, a.stage_id, a.status_id
ORDER BY 
  a.stage_id DESC,
  a.status_id

