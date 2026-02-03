-- =============================================================================
-- Dashboard: Signal Promotion Metrics
-- Tab: Operations
-- View: Recent Promotions
-- =============================================================================
-- Visualization: Table
-- Description: Latest promotions with full details for operational review
-- =============================================================================

SELECT
  f.promotion_uuid,
  f.signal_uuid,
  f.population_uuid,
  f.date AS last_updated,
  -- State
  ps.name AS current_stage,
  pst.name AS current_status,
  f.promotion_outcome,
  -- Source
  COALESCE(ds.name, CAST(f.data_source AS STRING)) AS data_source,
  u.name AS user_name,
  u.email AS user_email,
  -- Vehicle info
  pt.name AS population_type,
  pop.make,
  pop.model,
  pop.year,
  pop.engine_model,
  pop.powertrain,
  pop.fuel_group,
  -- Signal info
  ov.name AS obd_value,
  sig.protocol,
  sig.data_identifier,
  sig.network_interface,
  sig.request_id,
  sig.response_id,
  sig.bit_start,
  sig.bit_length,
  sig.scale,
  sig.offset,
  -- Time metrics
  ROUND(f.time_in_current_stage_seconds / 86400.0, 1) AS days_in_stage,
  ROUND(f.time_to_current_state_seconds / 86400.0, 1) AS total_days,
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
LEFT JOIN definitions.population_type pt ON pt.id = pop.type
LEFT JOIN definitions.obd_values ov ON ov.id = sig.obd_value

ORDER BY f.date DESC
LIMIT 100

