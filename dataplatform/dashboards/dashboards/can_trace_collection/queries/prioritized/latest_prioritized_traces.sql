-- =============================================================================
-- Dashboard: CAN Trace Collection Dashboard
-- Tab: Prioritized Traces
-- View: Latest Prioritized Traces
-- =============================================================================
-- Visualization: Table
-- Title: Latest Prioritized Traces
-- Description: Most recent required traces prioritized by device_day_rank (1 = highest priority)
-- Shows traces from fct_can_traces_required, which contains the final prioritized list
-- =============================================================================

WITH latest_date AS (
    SELECT MAX(date) AS max_date
    FROM datamodel_dev.fct_can_traces_required
    WHERE date BETWEEN :date.min AND :date.max
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
    fctr.date,
    fctr.org_id,
    fctr.device_id,
    dvp.make,
    dvp.model,
    dvp.year,
    dvp.engine_model,
    dvp.powertrain,
    dvp.fuel_group,
    dvp.trim,
    fctr.start_time,
    fctr.end_time,
    fctr.device_day_rank,
    fctr.tags,
    SIZE(fctr.tags) AS tag_count
FROM datamodel_dev.fct_can_traces_required fctr
LEFT JOIN product_analytics_staging.dim_device_vehicle_properties dvp_props
    ON fctr.date = dvp_props.date
    AND fctr.org_id = dvp_props.org_id
    AND fctr.device_id = dvp_props.device_id
LEFT JOIN vehicle_properties_with_translations dvp ON dvp_props.mmyef_id = dvp.mmyef_id
CROSS JOIN latest_date ld
WHERE fctr.date = ld.max_date
ORDER BY fctr.date DESC, fctr.device_day_rank ASC
LIMIT 1000

