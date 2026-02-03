-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: Coverage
-- View: Quarter-over-Quarter Coverage Summary
-- =============================================================================
-- Visualization: Table
-- Description: Coverage metrics comparing current vs previous quarter
-- =============================================================================

SELECT
  m.signal_name,
  c.type AS signal_type,
  c.market,
  m.product_area,
  c.coverage_rate_current,
  c.coverage_rate_previous,
  c.coverage_rate_change,
  c.eligible_devices,
  c.devices_with_coverage_current,
  c.devices_with_coverage_previous,
  c.devices_gained,
  c.devices_lost,
  c.devices_net_change
FROM product_analytics_staging.agg_telematics_coverage_qoq_normalized c
JOIN product_analytics_staging.fct_telematics_stat_metadata m USING (type)
WHERE c.date = (SELECT MAX(date) FROM product_analytics_staging.agg_telematics_coverage_qoq_normalized WHERE date BETWEEN :date.min AND :date.max)
ORDER BY c.coverage_rate_change DESC
