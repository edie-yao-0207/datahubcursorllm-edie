-- =============================================================================
-- Dashboard: VDP Trace Explorer
-- Tab: Traces
-- View: Trace UUIDs
-- =============================================================================
-- Visualization: Table
-- Description: List of trace UUIDs with vehicle properties for filtering
-- =============================================================================

SELECT
    DATE(date) AS date,
    org_id,
    device_id,
    make,
    model,
    year,
    engine_model,
    properties_fuel_fuelgroup.name AS fuel_group, 
    properties_fuel_powertrain.name AS powertrain,
    trace_uuid,
    trace_duration_seconds,
    mmyef_id
FROM product_analytics_staging.dim_trace_characteristics
LEFT JOIN product_analytics_staging.dim_device_vehicle_properties USING (date, org_id, device_id)
LEFT JOIN definitions.properties_fuel_fuelgroup ON properties_fuel_fuelgroup.id = fuel_group
LEFT JOIN definitions.properties_fuel_powertrain ON properties_fuel_powertrain.id = powertrain
WHERE date BETWEEN :date.min AND :date.max
GROUP BY ALL

