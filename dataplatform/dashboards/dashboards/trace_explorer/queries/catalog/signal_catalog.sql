-- =============================================================================
-- Dashboard: VDP Trace Explorer
-- Tab: Signal Catalog
-- View: Signal Catalog
-- =============================================================================
-- Visualization: Table
-- Description: Complete signal catalog with definitions
-- =============================================================================

SELECT
  -- Primary columns for display
  ov.name AS obd_value_name,
  CASE
    WHEN dim.data_source_id = 0 THEN "Invalid"
    WHEN dim.data_source_id = 1 THEN "Global"
    WHEN dim.data_source_id = 2 THEN "J1979_DA"
    WHEN dim.data_source_id = 3 THEN "J1939_DA"
    WHEN dim.data_source_id = 4 THEN "SPS"
    WHEN dim.data_source_id = 5 THEN "A.I."
    ELSE "Unknown"
  END AS data_source_name,
  dim.make,
  dim.model,
  dim.year,
  dim.engine_model,
  pt.name AS powertrain_name,
  fg.name AS fuel_group_name,
  dim.trim,
  dim.data_identifier,
  dim.bit_start,
  dim.bit_length,
  endian.name AS endian_name,
  dim.scale,
  dim.internal_scaling,
  sign.name AS sign_name,
  dim.offset,
  dim.minimum,
  dim.maximum,
  dim.unit,
  dim.description,
  dim.mapping,
  dim.pgn,
  dim.spn,
  dim.passive_response_mask,
  dim.response_id,
  dim.request_id,
  dim.source_id,
  dim.application_id,
  dim.protocol_id,
  dim.signal_uuid,
  dim.mmyef_id,
  dim.request_period_ms,
  dim.is_broadcast,
  dim.is_standard,
  dim.stream_id,
  dim.signal_catalog_id,
  -- Raw IDs for reference
  dim.powertrain,
  dim.fuel_group,
  dim.obd_value,
  dim.endian,
  dim.sign,
  dim.data_source_id
FROM product_analytics_staging.dim_signal_catalog_definitions AS dim
LEFT JOIN definitions.properties_fuel_powertrain AS pt ON pt.id = dim.powertrain
LEFT JOIN definitions.properties_fuel_fuelgroup AS fg ON fg.id = dim.fuel_group
LEFT JOIN definitions.obd_values AS ov ON ov.id = dim.obd_value
LEFT JOIN definitions.signal_endian AS endian ON dim.endian = endian.id
LEFT JOIN definitions.signal_sign AS sign ON dim.sign = sign.id

