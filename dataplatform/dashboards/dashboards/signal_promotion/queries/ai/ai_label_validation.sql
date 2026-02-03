-- =============================================================================
-- Dashboard: Signal Promotion Metrics
-- Tab: AI Promotions
-- View: AI Label Validation Summary
-- =============================================================================
-- Visualization: Pie Chart or Counter
-- Description: Track how many AI promotion rollbacks have valid vs invalid labels
--              Measures labeling quality/accuracy
-- =============================================================================

WITH ai_rollbacks AS (
  SELECT
    promotion_uuid,
    promotion_outcome,
    parsed_comment.is_comment_valid AS is_valid,
    parsed_comment.is_rollback_from_testing AS is_rollback
  FROM product_analytics_staging.fct_signal_promotion_latest_state
  WHERE data_source = 20  -- DATA_SOURCE_AI_ML
    AND parsed_comment.is_rollback_from_testing = TRUE  -- Only rollbacks require labels
)

SELECT
  CASE 
    WHEN is_valid = TRUE THEN 'Valid Labels'
    WHEN is_valid = FALSE THEN 'Invalid/Missing Labels'
    ELSE 'Not Labeled'
  END AS label_status,
  COUNT(*) AS count,
  -- Return as 0-1 decimal (UI formats as percentage)
  ROUND(COUNT(*) * 1.0 / SUM(COUNT(*)) OVER (), 3) AS percent_of_total
FROM ai_rollbacks
GROUP BY 
  CASE 
    WHEN is_valid = TRUE THEN 'Valid Labels'
    WHEN is_valid = FALSE THEN 'Invalid/Missing Labels'
    ELSE 'Not Labeled'
  END
ORDER BY 
  CASE label_status
    WHEN 'Valid Labels' THEN 1
    WHEN 'Invalid/Missing Labels' THEN 2
    ELSE 3
  END

