-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: CAN Pipeline Impact
-- View: Collection Depth per Population
-- =============================================================================
-- Visualization: Table/Counter
-- Description: Average/median traces per population
-- =============================================================================

WITH population_trace_counts AS (
  SELECT 
    dvp.mmyef_id,
    COUNT(DISTINCT cts.trace_uuid) AS trace_count
  FROM product_analytics_staging.fct_can_trace_status cts
  JOIN product_analytics_staging.dim_device_vehicle_properties dvp
    ON cts.org_id = dvp.org_id
    AND cts.device_id = dvp.device_id
    AND dvp.date = (SELECT MAX(date) FROM product_analytics_staging.dim_device_vehicle_properties)
  WHERE cts.is_available = TRUE
    AND dvp.mmyef_id IS NOT NULL
  GROUP BY dvp.mmyef_id
)

SELECT
  COUNT(*) AS total_populations_with_traces,
  ROUND(AVG(trace_count), 1) AS avg_traces_per_population,
  PERCENTILE(trace_count, 0.5) AS median_traces_per_population,
  PERCENTILE(trace_count, 0.25) AS p25_traces_per_population,
  PERCENTILE(trace_count, 0.75) AS p75_traces_per_population,
  MIN(trace_count) AS min_traces,
  MAX(trace_count) AS max_traces
FROM population_trace_counts

