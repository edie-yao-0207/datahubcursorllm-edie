-- =============================================================================
-- Dashboard: Signal Promotion Metrics
-- Tab: Operations
-- View: Stuck Promotions (All Sources)
-- =============================================================================
-- Visualization: Table
-- Description: Promotions waiting in a stage for an extended period (all data sources)
-- =============================================================================

SELECT
  f.promotion_uuid,
  f.signal_uuid,
  f.population_uuid,
  ps.name AS current_stage,
  pst.name AS current_status,
  f.promotion_outcome,
  COALESCE(ds.name, CAST(f.data_source AS STRING)) AS data_source,
  u.name AS user_name,
  -- Vehicle info
  pop.make,
  pop.model,
  pop.year,
  -- Signal info
  sig.obd_value,
  sig.data_identifier,
  -- Time metrics
  ROUND(f.time_in_current_stage_seconds / 86400.0, 1) AS days_in_stage,
  ROUND(f.time_to_current_state_seconds / 86400.0, 1) AS total_days_since_creation,
  f.record_count AS transition_count

FROM product_analytics_staging.fct_signal_promotion_latest_state f
LEFT JOIN definitions.promotion_stage ps ON ps.id = f.current.stage
LEFT JOIN definitions.promotion_status pst ON pst.id = f.current.status
LEFT JOIN definitions.promotion_data_source_to_name ds ON ds.id = f.data_source
LEFT JOIN (
  SELECT * FROM datamodel_platform.dim_users 
  WHERE date = (SELECT MAX(date) FROM datamodel_platform.dim_users)
) u ON u.user_id = f.created_by
LEFT JOIN signalpromotiondb.populations pop ON pop.population_uuid = f.population_uuid
LEFT JOIN signalpromotiondb.signals sig ON sig.signal_uuid = f.signal_uuid

WHERE f.promotion_outcome = 'IN_PROGRESS'
  AND f.current.stage IN (2, 3, 4)  -- PRE_ALPHA, ALPHA, BETA (active review stages)
  AND f.time_in_current_stage_seconds > 7 * 86400  -- More than 7 days in current stage

ORDER BY 
  f.time_in_current_stage_seconds DESC  -- Longest wait time first
LIMIT 100

