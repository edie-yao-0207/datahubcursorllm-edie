-- =============================================================================
-- Dashboard: Signal Promotion Metrics
-- Tab: AI Promotions
-- View: AI Promotions Pending Review
-- =============================================================================
-- Visualization: Table
-- Description: List of AI promotions awaiting approval in the review pipeline
-- =============================================================================

SELECT
  f.promotion_uuid,
  f.signal_uuid,
  f.population_uuid,
  ps.name AS current_stage,
  pst.name AS current_status,
  f.promotion_outcome,
  -- Vehicle info from population
  pop.make,
  pop.model,
  pop.year,
  -- Signal info
  sig.obd_value,
  sig.data_identifier,
  -- Time in current stage
  ROUND(f.time_in_current_stage_seconds / 86400.0, 1) AS days_in_stage,
  ROUND(f.time_to_current_state_seconds / 86400.0, 1) AS total_days_since_creation,
  f.record_count AS transition_count

FROM product_analytics_staging.fct_signal_promotion_latest_state f
LEFT JOIN definitions.promotion_stage ps ON ps.id = f.current.stage
LEFT JOIN definitions.promotion_status pst ON pst.id = f.current.status
LEFT JOIN signalpromotiondb.populations pop ON pop.population_uuid = f.population_uuid
LEFT JOIN signalpromotiondb.signals sig ON sig.signal_uuid = f.signal_uuid

WHERE f.data_source = 20  -- DATA_SOURCE_AI_ML
  AND f.promotion_outcome = 'IN_PROGRESS'
  AND f.current.stage IN (2, 3, 4)  -- PRE_ALPHA, ALPHA, BETA (active review stages)

ORDER BY 
  f.current.stage DESC,  -- Higher stages first (closer to graduation)
  f.time_in_current_stage_seconds DESC  -- Longest wait time first
LIMIT 100

