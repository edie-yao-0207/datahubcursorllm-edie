-- =============================================================================
-- Dashboard: CAN Trace Collection Dashboard
-- Tab: Diversity Metrics
-- View: Underrepresented MMYEFs
-- =============================================================================
-- Visualization: Table
-- Title: Underrepresented MMYEFs (0 Traces)
-- Description: MMYEFs with no collected traces (all-time cumulative) - prioritize for collection
-- =============================================================================

WITH
-- Get MMYEFs with traces (all-time from fct_can_trace_status)
mmyefs_with_traces AS (
    SELECT DISTINCT dvp.mmyef_id
    FROM product_analytics_staging.fct_can_trace_status cts
    JOIN product_analytics_staging.dim_device_vehicle_properties dvp
        ON cts.date = dvp.date
        AND cts.org_id = dvp.org_id
        AND cts.device_id = dvp.device_id
    WHERE dvp.mmyef_id IS NOT NULL
),

-- Get all MMYEFs in population (latest date from dim table)
all_mmyefs AS (
    SELECT DISTINCT dvp.mmyef_id
    FROM product_analytics_staging.dim_device_vehicle_properties dvp
    WHERE dvp.date = (SELECT MAX(date) FROM product_analytics_staging.dim_device_vehicle_properties)
      AND dvp.mmyef_id IS NOT NULL
),

-- Get vehicle properties with translations
vehicle_properties_with_translations AS (
    SELECT DISTINCT 
        dvp2.mmyef_id,
        dvp2.make,
        dvp2.model,
        dvp2.year,
        dvp2.engine_model,
        dvp2.powertrain AS powertrain_id,
        dvp2.fuel_group AS fuel_group_id,
        dvp2.trim,
        pt.name AS powertrain,
        fg.name AS fuel_group
    FROM product_analytics_staging.dim_device_vehicle_properties dvp2
    LEFT JOIN definitions.properties_fuel_powertrain pt ON pt.id = dvp2.powertrain
    LEFT JOIN definitions.properties_fuel_fuelgroup fg ON fg.id = dvp2.fuel_group
    WHERE dvp2.date = (SELECT MAX(date) FROM product_analytics_staging.dim_device_vehicle_properties)
)

SELECT
    am.mmyef_id,
    dvp.make,
    dvp.model,
    dvp.year,
    dvp.engine_model,
    dvp.powertrain,
    dvp.fuel_group,
    dvp.trim,
    0 AS total_collected_count
FROM all_mmyefs am
LEFT JOIN mmyefs_with_traces mwt ON am.mmyef_id = mwt.mmyef_id
LEFT JOIN vehicle_properties_with_translations dvp ON am.mmyef_id = dvp.mmyef_id
WHERE mwt.mmyef_id IS NULL
ORDER BY am.mmyef_id
LIMIT 100
