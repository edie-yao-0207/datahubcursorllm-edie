-- =============================================================================
-- Dashboard: VDP Trace Explorer
-- Tab: ASC Files
-- View: ASC Traces
-- =============================================================================
-- Visualization: Table
-- Description: Browse ASC trace files with S3 paths
-- =============================================================================

SELECT
    exp.date,
    exp.org_id,
    exp.device_id,
    exp.trace_uuid,
    exp.s3_path,
    exp.metadata_json_path,
    exp.file_size_bytes,
    prop.make,
    prop.model,
    prop.year,
    prop.engine_model,
    properties_fuel_fuelgroup.name AS fuel_group,
    properties_fuel_powertrain.name AS powertrain
FROM product_analytics_staging.fct_can_trace_exports AS exp
JOIN product_analytics.dim_device_dimensions USING (date, org_id, device_id)
LEFT JOIN product_analytics_staging.dim_device_vehicle_properties AS prop USING (date, org_id, device_id)
LEFT JOIN definitions.properties_fuel_fuelgroup ON properties_fuel_fuelgroup.id = fuel_group
LEFT JOIN definitions.properties_fuel_powertrain ON properties_fuel_powertrain.id = powertrain
WHERE date BETWEEN :date.min AND :date.max

