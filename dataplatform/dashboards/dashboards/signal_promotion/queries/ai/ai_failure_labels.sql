-- =============================================================================
-- Dashboard: Signal Promotion Metrics
-- Tab: AI Promotions
-- View: AI Failure Labels Applied
-- =============================================================================
-- Visualization: Bar Chart
-- Description: Distribution of labels applied to failed AI promotions
--              Labels include: HAS_CONFIG_ISSUE, IS_COVERED, INTERNAL_TESTING, etc.
-- =============================================================================

WITH ai_failures AS (
  SELECT
    promotion_uuid,
    parsed_comment.labels AS labels_map
  FROM product_analytics_staging.fct_signal_promotion_latest_state
  WHERE data_source = 20  -- DATA_SOURCE_AI_ML
    AND current.stage = 1  -- FAILED stage
    AND parsed_comment.labels IS NOT NULL
),

-- Explode the labels map to get individual label counts
exploded_labels AS (
  SELECT
    promotion_uuid,
    label_entry.key AS label_name,
    label_entry.value AS is_valid
  FROM ai_failures
  LATERAL VIEW EXPLODE(labels_map) label_entry AS key, value
)

SELECT
  -- Format label name for readability
  INITCAP(REPLACE(label_name, '_', ' ')) AS label_display,
  label_name AS label_code,
  COUNT(DISTINCT promotion_uuid) AS promotion_count,
  SUM(CASE WHEN is_valid = TRUE THEN 1 ELSE 0 END) AS valid_label_count,
  SUM(CASE WHEN is_valid = FALSE THEN 1 ELSE 0 END) AS invalid_label_count,
  -- Validity rate as 0-1 decimal (UI formats as percentage)
  ROUND(
    SUM(CASE WHEN is_valid = TRUE THEN 1 ELSE 0 END) * 1.0 
    / NULLIF(COUNT(*), 0),
    3
  ) AS validity_rate_pct
FROM exploded_labels
GROUP BY label_name
ORDER BY promotion_count DESC

