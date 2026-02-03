-- =============================================================================
-- Dashboard: Signal Promotion Metrics
-- Tab: AI Promotions
-- View: AI Failed Promotions (Detailed)
-- =============================================================================
-- Visualization: Table
-- Description: Detailed list of failed AI promotions with failure reasons
--              Shows the tags/labels explaining WHY each promotion failed
-- =============================================================================

WITH ai_failures AS (
  SELECT
    f.promotion_uuid,
    f.signal_uuid,
    f.population_uuid,
    f.promotion_outcome,
    f.parsed_comment.labels AS labels_map,
    f.parsed_comment.tags AS tags_map,
    f.parsed_comment.note AS failure_note,
    f.parsed_comment.is_comment_valid AS has_valid_label,
    f.parsed_comment.is_rollback_from_testing AS is_rollback,
    f.time_to_current_state_seconds,
    f.record_count
  FROM product_analytics_staging.fct_signal_promotion_latest_state f
  WHERE f.data_source = 20  -- DATA_SOURCE_AI_ML
    AND f.current.stage = 1  -- FAILED stage
)

SELECT
  f.promotion_uuid,
  f.signal_uuid,
  f.population_uuid,
  
  -- Vehicle info
  pop.make,
  pop.model,
  pop.year,
  
  -- Signal info
  sig.obd_value,
  
  -- Failure classification
  f.promotion_outcome,
  CASE 
    WHEN f.is_rollback THEN 'Rollback'
    ELSE 'Direct Failure'
  END AS failure_type,
  
  -- Failure reasons (formatted from tags)
  CASE 
    WHEN f.tags_map IS NOT NULL THEN
      ARRAY_JOIN(
        TRANSFORM(
          MAP_KEYS(f.tags_map),
          tag -> INITCAP(REPLACE(tag, '_', ' '))
        ),
        ', '
      )
    ELSE 'No tags specified'
  END AS failure_reasons,
  
  -- Labels applied
  CASE 
    WHEN f.labels_map IS NOT NULL THEN
      ARRAY_JOIN(
        TRANSFORM(
          MAP_KEYS(f.labels_map),
          label -> INITCAP(REPLACE(label, '_', ' '))
        ),
        ', '
      )
    ELSE 'No labels'
  END AS labels_applied,
  
  -- Validation status
  CASE 
    WHEN f.has_valid_label = TRUE THEN '✓ Valid'
    WHEN f.has_valid_label = FALSE THEN '✗ Invalid'
    ELSE '- N/A'
  END AS label_status,
  
  -- Notes
  COALESCE(f.failure_note, '') AS notes,
  
  -- Metrics
  ROUND(f.time_to_current_state_seconds / 86400.0, 1) AS days_to_failure,
  f.record_count AS transition_count

FROM ai_failures f
LEFT JOIN signalpromotiondb.populations pop ON pop.population_uuid = f.population_uuid
LEFT JOIN signalpromotiondb.signals sig ON sig.signal_uuid = f.signal_uuid

ORDER BY f.time_to_current_state_seconds DESC
LIMIT 100

