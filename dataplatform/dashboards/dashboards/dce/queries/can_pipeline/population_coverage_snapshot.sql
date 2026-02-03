-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: CAN Pipeline Impact
-- View: Population Coverage Snapshot
-- =============================================================================
-- Visualization: Counter
-- Description: Current population coverage metrics (total populations, covered populations, coverage %)
-- Note: Uses latest available date for vehicle properties, all-time for trace coverage
-- =============================================================================

WITH latest_vehicle_props AS (
  SELECT MAX(date) AS max_date
  FROM product_analytics_staging.dim_device_vehicle_properties
),
total_populations AS (
  SELECT COUNT(DISTINCT mmyef_id) AS total_populations
  FROM product_analytics_staging.dim_device_vehicle_properties
  WHERE date = (SELECT max_date FROM latest_vehicle_props)
    AND mmyef_id IS NOT NULL
),
covered_populations AS (
  SELECT COUNT(DISTINCT dvp.mmyef_id) AS covered_populations
  FROM product_analytics_staging.fct_can_trace_status cts
  JOIN product_analytics_staging.dim_device_vehicle_properties dvp
    ON cts.org_id = dvp.org_id
    AND cts.device_id = dvp.device_id
    AND dvp.date = (SELECT max_date FROM latest_vehicle_props)
  WHERE cts.is_available = TRUE
    AND dvp.mmyef_id IS NOT NULL
)

SELECT
  tp.total_populations,
  cp.covered_populations,
  ROUND(cp.covered_populations::FLOAT / NULLIF(tp.total_populations, 0), 4) AS coverage_percentage
FROM total_populations tp
CROSS JOIN covered_populations cp

