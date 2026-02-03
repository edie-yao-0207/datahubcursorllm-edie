-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: CAN Pipeline Impact
-- View: Representative Dataset Statistics
-- =============================================================================
-- Visualization: Table
-- Description: Main dataset tag statistics (all-time historical data)
-- Note: This shows all-time totals to match impact metrics document
-- =============================================================================

WITH traces_with_mmyef AS (
  SELECT
    cts.trace_uuid,
    cts.tag_names,
    dvp.mmyef_id
  FROM product_analytics_staging.fct_can_trace_status cts
  JOIN product_analytics_staging.dim_device_vehicle_properties dvp
    ON cts.date = dvp.date
    AND cts.org_id = dvp.org_id
    AND cts.device_id = dvp.device_id
  WHERE cts.is_available = TRUE
    AND dvp.mmyef_id IS NOT NULL
)

SELECT
  'can-set-main-0' AS tag_name,
  COUNT(DISTINCT trace_uuid) AS trace_count,
  COUNT(DISTINCT mmyef_id) AS population_count
FROM traces_with_mmyef
WHERE ARRAY_CONTAINS(tag_names, 'can-set-main-0')

UNION ALL

SELECT
  'can-set-signal-populations-0' AS tag_name,
  COUNT(DISTINCT trace_uuid) AS trace_count,
  COUNT(DISTINCT mmyef_id) AS population_count
FROM traces_with_mmyef
WHERE ARRAY_CONTAINS(tag_names, 'can-set-signal-populations-0')

UNION ALL

SELECT
  'Both Tags' AS tag_name,
  COUNT(DISTINCT trace_uuid) AS trace_count,
  COUNT(DISTINCT mmyef_id) AS population_count
FROM traces_with_mmyef
WHERE ARRAY_CONTAINS(tag_names, 'can-set-main-0')
  AND ARRAY_CONTAINS(tag_names, 'can-set-signal-populations-0')

