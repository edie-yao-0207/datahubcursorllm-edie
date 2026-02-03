-- =============================================================================
-- Dashboard: Signal Promotion Metrics
-- Tab: AI Promotions
-- View: AI Failure Reasons (Tags)
-- =============================================================================
-- Visualization: Bar Chart
-- Description: Top failure tags explaining WHY AI promotions failed
--              Tags include: WRONG_BIT_START, WRONG_SCALE, WRONG_MAPPING, etc.
-- =============================================================================

WITH ai_failures AS (
  SELECT
    promotion_uuid,
    parsed_comment.tags AS tags_map
  FROM product_analytics_staging.fct_signal_promotion_latest_state
  WHERE data_source = 20  -- DATA_SOURCE_AI_ML
    AND current.stage = 1  -- FAILED stage
    AND parsed_comment.tags IS NOT NULL
),

-- Explode the tags map to get individual tag counts
exploded_tags AS (
  SELECT
    promotion_uuid,
    tag_entry.key AS tag_name,
    tag_entry.value AS is_valid
  FROM ai_failures
  LATERAL VIEW EXPLODE(tags_map) tag_entry AS key, value
)

SELECT
  -- Format tag name for readability (WRONG_BIT_START -> Wrong Bit Start)
  INITCAP(REPLACE(tag_name, '_', ' ')) AS failure_reason,
  tag_name AS tag_code,
  COUNT(DISTINCT promotion_uuid) AS promotion_count,
  SUM(CASE WHEN is_valid = TRUE THEN 1 ELSE 0 END) AS valid_tag_count,
  SUM(CASE WHEN is_valid = FALSE THEN 1 ELSE 0 END) AS invalid_tag_count
FROM exploded_tags
GROUP BY tag_name
ORDER BY promotion_count DESC
LIMIT 20

