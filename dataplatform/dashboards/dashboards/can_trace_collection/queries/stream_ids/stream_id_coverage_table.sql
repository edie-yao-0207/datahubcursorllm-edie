-- =============================================================================
-- Dashboard: CAN Trace Collection Dashboard
-- Tab: Stream ID Coverage
-- View: Stream ID Coverage Table
-- =============================================================================
-- Visualization: Table
-- Title: Stream ID Coverage by MMYEF
-- Description: Stream ID coverage statistics by MMYEF (all-time cumulative from latest snapshot)
-- =============================================================================

-- Note: agg_mmyef_stream_ids stores cumulative counts per date partition.
-- MAX(date) partition contains cumulative counts up to that date (effectively all-time).
WITH latest_snapshot_date AS (
    SELECT MAX(date) AS max_date
    FROM datamodel_dev.agg_mmyef_stream_ids
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
    amsi.mmyef_id,
    dvp.make,
    dvp.model,
    dvp.year,
    dvp.engine_model,
    dvp.powertrain,
    dvp.fuel_group,
    dvp.trim,
    amsi.stream_id,
    amsi.obd_value,
    obd.name AS obd_value_name,
    amsi.trace_count,
    amsi.first_seen_date,
    amsi.last_seen_date
FROM datamodel_dev.agg_mmyef_stream_ids amsi
CROSS JOIN latest_snapshot_date lsd
LEFT JOIN vehicle_properties_with_translations dvp ON amsi.mmyef_id = dvp.mmyef_id
LEFT JOIN definitions.obd_values obd ON amsi.obd_value = obd.id
WHERE amsi.date = lsd.max_date
ORDER BY amsi.trace_count ASC, amsi.mmyef_id, amsi.stream_id
LIMIT 100
