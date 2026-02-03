-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: CAN Pipeline Impact
-- View: Population Coverage Trend
-- =============================================================================
-- Visualization: Line chart
-- Description: Coverage percentage trend over time
-- =============================================================================

WITH daily_vehicle_props AS (
  SELECT DISTINCT
    date,
    mmyef_id
  FROM product_analytics_staging.dim_device_vehicle_properties
  WHERE date BETWEEN :date.min AND :date.max
    AND mmyef_id IS NOT NULL
),
daily_total_populations AS (
  SELECT
    date,
    COUNT(DISTINCT mmyef_id) AS total_populations
  FROM daily_vehicle_props
  GROUP BY date
),
daily_covered_populations AS (
  SELECT
    dvp.date,
    COUNT(DISTINCT dvp.mmyef_id) AS covered_populations
  FROM product_analytics_staging.fct_can_trace_status cts
  JOIN product_analytics_staging.dim_device_vehicle_properties dvp
    ON cts.org_id = dvp.org_id
    AND cts.device_id = dvp.device_id
    AND dvp.date BETWEEN :date.min AND :date.max
  WHERE cts.is_available = TRUE
    AND dvp.mmyef_id IS NOT NULL
  GROUP BY dvp.date
)

SELECT
  dtp.date,
  dtp.total_populations,
  COALESCE(dcp.covered_populations, 0) AS covered_populations,
  ROUND(COALESCE(dcp.covered_populations, 0)::FLOAT / NULLIF(dtp.total_populations, 0), 4) AS coverage_percentage
FROM daily_total_populations dtp
LEFT JOIN daily_covered_populations dcp ON dtp.date = dcp.date
ORDER BY dtp.date

