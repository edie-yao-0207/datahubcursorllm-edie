-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: CAN Pipeline Impact
-- View: Signal Catalog Snapshot
-- =============================================================================
-- Visualization: Counter
-- Description: Total signals, OBD values, signals with OBD values
-- =============================================================================

SELECT
  COUNT(DISTINCT signal_catalog_id) AS total_signals,
  COUNT(DISTINCT obd_value) AS unique_obd_values,
  COUNT(DISTINCT CASE WHEN obd_value IS NOT NULL THEN signal_catalog_id END) AS signals_with_obd_values,
  COUNT(DISTINCT mmyef_id) AS populations_with_signals,
  COUNT(DISTINCT CASE WHEN is_broadcast = TRUE AND is_standard = FALSE THEN signal_catalog_id END) AS proprietary_broadcast_signals,
  COUNT(DISTINCT CASE WHEN is_broadcast = TRUE AND is_standard = TRUE THEN signal_catalog_id END) AS standard_broadcast_signals
FROM product_analytics_staging.dim_combined_signal_definitions

