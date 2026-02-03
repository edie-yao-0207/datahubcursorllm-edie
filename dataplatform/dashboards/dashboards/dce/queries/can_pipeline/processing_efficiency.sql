-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: CAN Pipeline Impact
-- View: Processing Efficiency
-- =============================================================================
-- Visualization: Counter/Table
-- Description: UDF vs SQL passthrough rates (all-time historical data)
-- Note: This shows all-time totals to match impact metrics document
-- =============================================================================

SELECT
  COUNT(DISTINCT CASE WHEN processed_by_udf = TRUE THEN trace_uuid END) AS traces_with_udf_processing,
  COUNT(DISTINCT trace_uuid) AS total_traces,
  ROUND(COUNT(DISTINCT CASE WHEN processed_by_udf = TRUE THEN trace_uuid END)::FLOAT / NULLIF(COUNT(DISTINCT trace_uuid), 0), 4) AS udf_processing_percentage,
  ROUND(COUNT(DISTINCT CASE WHEN processed_by_udf = FALSE THEN trace_uuid END)::FLOAT / NULLIF(COUNT(DISTINCT trace_uuid), 0), 4) AS sql_passthrough_percentage
FROM product_analytics_staging.fct_can_trace_recompiled
WHERE mmyef_id IS NOT NULL

