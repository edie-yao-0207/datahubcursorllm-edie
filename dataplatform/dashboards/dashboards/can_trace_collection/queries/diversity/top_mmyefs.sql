-- =============================================================================
-- Dashboard: CAN Trace Collection Dashboard
-- Tab: Diversity Metrics
-- View: Top MMYEFs
-- =============================================================================
-- Visualization: Table
-- Title: Top MMYEFs by Collection Count
-- Description: MMYEFs with the most collected traces (all-time cumulative)
-- =============================================================================

WITH
-- Aggregate collected traces per MMYEF (all-time from fct_can_trace_status)
mmyef_trace_counts AS (
    SELECT
        dvp.mmyef_id,
        COUNT(DISTINCT cts.trace_uuid) AS total_collected_count
    FROM product_analytics_staging.fct_can_trace_status cts
    JOIN product_analytics_staging.dim_device_vehicle_properties dvp
        ON cts.date = dvp.date
        AND cts.org_id = dvp.org_id
        AND cts.device_id = dvp.device_id
    WHERE dvp.mmyef_id IS NOT NULL
    GROUP BY dvp.mmyef_id
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
    mtc.mmyef_id,
    dvp.make,
    dvp.model,
    dvp.year,
    dvp.engine_model,
    dvp.powertrain,
    dvp.fuel_group,
    dvp.trim,
    mtc.total_collected_count
FROM mmyef_trace_counts mtc
LEFT JOIN vehicle_properties_with_translations dvp ON mtc.mmyef_id = dvp.mmyef_id
ORDER BY mtc.total_collected_count DESC
LIMIT 50
