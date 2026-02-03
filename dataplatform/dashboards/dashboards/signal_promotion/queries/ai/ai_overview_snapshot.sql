-- =============================================================================
-- Dashboard: Signal Promotion Metrics
-- Tab: AI Promotions
-- View: AI Overview Snapshot
-- =============================================================================
-- Visualization: Counter cards
-- Description: Summary metrics for AI/ML-generated promotions (data_source = 20)
-- =============================================================================

WITH ai_promotions AS (
  SELECT
    promotion_uuid,
    current.stage AS stage,
    current.status AS status,
    promotion_outcome,
    parsed_comment.is_comment_valid AS is_comment_valid,
    parsed_comment.is_promotion_from_training_data AS is_from_training_data,
    time_in_current_stage_seconds,
    time_to_current_state_seconds
  FROM product_analytics_staging.fct_signal_promotion_latest_state
  WHERE data_source = 20  -- DATA_SOURCE_AI_ML
)

SELECT
  COUNT(*) AS total_ai_promotions,
  
  -- Success metrics
  SUM(CASE WHEN promotion_outcome = 'SUCCESS' THEN 1 ELSE 0 END) AS successful,
  -- Success rate as 0-1 decimal (UI formats as percentage)
  ROUND(SUM(CASE WHEN promotion_outcome = 'SUCCESS' THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0), 3) AS success_rate_pct,
  
  -- In-progress / pending review
  SUM(CASE WHEN promotion_outcome = 'IN_PROGRESS' THEN 1 ELSE 0 END) AS in_progress,
  SUM(CASE WHEN status = 2 THEN 1 ELSE 0 END) AS pending_approval,  -- PENDING_APPROVAL status
  
  -- Failures
  SUM(CASE WHEN promotion_outcome = 'PRODUCTION_FAILURE' THEN 1 ELSE 0 END) AS production_failures,
  SUM(CASE WHEN promotion_outcome = 'ALPHA_ROLLBACK' THEN 1 ELSE 0 END) AS alpha_rollbacks,
  -- Failure rate as 0-1 decimal (UI formats as percentage)
  ROUND(
    SUM(CASE WHEN promotion_outcome IN ('PRODUCTION_FAILURE', 'ALPHA_ROLLBACK') THEN 1 ELSE 0 END) * 1.0 
    / NULLIF(COUNT(*), 0), 
    3
  ) AS failure_rate_pct,
  
  -- Label quality
  SUM(CASE WHEN is_comment_valid = TRUE THEN 1 ELSE 0 END) AS valid_labels,
  SUM(CASE WHEN is_comment_valid = FALSE THEN 1 ELSE 0 END) AS invalid_labels,
  -- Label accuracy as 0-1 decimal (UI formats as percentage)
  ROUND(
    SUM(CASE WHEN is_comment_valid = TRUE THEN 1 ELSE 0 END) * 1.0 
    / NULLIF(SUM(CASE WHEN is_comment_valid IS NOT NULL THEN 1 ELSE 0 END), 0),
    3
  ) AS label_accuracy_pct,
  
  -- From training data
  SUM(CASE WHEN is_from_training_data = TRUE THEN 1 ELSE 0 END) AS from_training_data,
  
  -- Review time metrics (for in-progress promotions in review stages)
  ROUND(
    AVG(CASE WHEN promotion_outcome = 'IN_PROGRESS' AND stage IN (2, 3, 4) 
        THEN time_in_current_stage_seconds / 86400.0 END),
    1
  ) AS avg_days_in_current_stage,
  ROUND(
    AVG(CASE WHEN promotion_outcome = 'IN_PROGRESS' AND stage IN (2, 3, 4)
        THEN time_to_current_state_seconds / 86400.0 END),
    1
  ) AS avg_total_days_in_review,
  ROUND(
    MAX(CASE WHEN promotion_outcome = 'IN_PROGRESS' AND stage IN (2, 3, 4)
        THEN time_in_current_stage_seconds / 86400.0 END),
    1
  ) AS max_days_waiting

FROM ai_promotions

