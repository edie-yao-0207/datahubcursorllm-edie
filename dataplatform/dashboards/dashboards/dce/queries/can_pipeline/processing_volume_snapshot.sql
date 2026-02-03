-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: CAN Pipeline Impact
-- View: Processing Volume Snapshot
-- =============================================================================
-- Visualization: Counter
-- Description: Total frames, traces, populations (all-time historical data)
-- Note: This shows all-time totals, not filtered by date range, to match impact metrics document
-- =============================================================================

SELECT
  COUNT(DISTINCT trace_uuid) AS total_traces_processed,
  COUNT(*) AS total_unique_frames,
  SUM(duplicate_count) AS total_frames_including_duplicates,
  COUNT(DISTINCT mmyef_id) AS unique_populations,
  COUNT(DISTINCT device_id) AS unique_devices,
  COUNT(DISTINCT org_id) AS unique_organizations
FROM product_analytics_staging.fct_can_trace_recompiled
WHERE mmyef_id IS NOT NULL

