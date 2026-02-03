-- =============================================================================
-- Dashboard: CAN Trace Collection Dashboard
-- Tab: Diversity Metrics
-- View: Diversity Snapshot
-- =============================================================================
-- Visualization: Counter
-- Title: Diversity Snapshot
-- Description: Unique MMYEFs and devices with collected traces (all-time cumulative)
-- =============================================================================

-- Get total population counts (use latest date from dim table for current population)
WITH total_population AS (
    SELECT 
        COUNT(DISTINCT mmyef_id) AS total_mmyefs,
        COUNT(DISTINCT org_id, device_id) AS total_devices
    FROM product_analytics_staging.dim_device_vehicle_properties
    WHERE date = (SELECT MAX(date) FROM product_analytics_staging.dim_device_vehicle_properties)
      AND mmyef_id IS NOT NULL
),

-- Get counts with traces (all-time from fct_can_trace_status)
with_traces AS (
    SELECT
        COUNT(DISTINCT dvp.mmyef_id) AS unique_mmyefs,
        COUNT(DISTINCT cts.org_id, cts.device_id) AS unique_devices
    FROM product_analytics_staging.fct_can_trace_status cts
    JOIN product_analytics_staging.dim_device_vehicle_properties dvp
        ON cts.date = dvp.date
        AND cts.org_id = dvp.org_id
        AND cts.device_id = dvp.device_id
    WHERE dvp.mmyef_id IS NOT NULL
)

SELECT
    wt.unique_mmyefs,
    tp.total_mmyefs,
    CAST(wt.unique_mmyefs AS DOUBLE) / NULLIF(CAST(tp.total_mmyefs AS DOUBLE), 0) AS mmyef_coverage_pct,
    wt.unique_devices,
    tp.total_devices,
    CAST(wt.unique_devices AS DOUBLE) / NULLIF(CAST(tp.total_devices AS DOUBLE), 0) AS device_coverage_pct
FROM with_traces wt
CROSS JOIN total_population tp
