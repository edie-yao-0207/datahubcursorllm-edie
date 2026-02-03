-- =============================================================================
-- Dashboard: VDP Trace Explorer
-- Tab: Traces
-- View: Decoded Trace Contents
-- =============================================================================
-- Visualization: Table
-- Description: Unique signals found in a specific trace
-- =============================================================================

WITH data AS (
  SELECT DISTINCT
      dec.trace_uuid,
      signal.data_identifier,
      signal.obd_value,
      signal.bit_start,
      signal.bit_length,
      COALESCE(
        application.j1939.source_address,
        application.uds.source_address,
        application.none.arbitration_id
      ) AS source_address
  FROM product_analytics_staging.fct_can_trace_decoded AS dec
  LATERAL VIEW EXPLODE(decoded_signals) AS signal
  WHERE trace_uuid = :trace_uuid
)

SELECT
  data.*,
  ov.name AS obd_value_name,
  CONCAT_WS(
    " ",
    FORMAT_STRING("SI=%s", ov.name),
    FORMAT_STRING("SA=%d", source_address),
    FORMAT_STRING("DI=%d", data_identifier),
    FORMAT_STRING("BS=%d", bit_start)
  ) AS label
FROM data
LEFT JOIN definitions.obd_values AS ov ON ov.id = data.obd_value

