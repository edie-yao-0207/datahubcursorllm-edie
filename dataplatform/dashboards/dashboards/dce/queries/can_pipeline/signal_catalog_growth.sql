-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: CAN Pipeline Impact
-- View: Signal Catalog Growth
-- =============================================================================
-- Visualization: Line chart
-- Description: Signal catalog growth over time
-- =============================================================================

-- Note: This query assumes signal definitions are cumulative (new signals added over time)
-- If there's a date field in dim_combined_signal_definitions, use it. Otherwise, this shows current snapshot.
-- For growth tracking, we'd need a historical table or date field.

SELECT
  CURRENT_DATE() AS date,
  COUNT(DISTINCT signal_catalog_id) AS total_signals,
  COUNT(DISTINCT obd_value) AS unique_obd_values,
  COUNT(DISTINCT CASE WHEN obd_value IS NOT NULL THEN signal_catalog_id END) AS signals_with_obd_values
FROM product_analytics_staging.dim_combined_signal_definitions

-- TODO: If dim_combined_signal_definitions has a date/created_at field, use it for time-series growth
-- Example:
-- SELECT
--   date,
--   COUNT(DISTINCT signal_catalog_id) AS total_signals,
--   COUNT(DISTINCT obd_value) AS unique_obd_values
-- FROM product_analytics_staging.dim_combined_signal_definitions
-- WHERE date BETWEEN :date.min AND :date.max
-- GROUP BY date
-- ORDER BY date

