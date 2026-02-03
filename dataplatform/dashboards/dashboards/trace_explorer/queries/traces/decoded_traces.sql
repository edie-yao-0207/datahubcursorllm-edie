-- =============================================================================
-- Dashboard: VDP Trace Explorer
-- Tab: Traces
-- View: Decoded Traces
-- =============================================================================
-- Visualization: Line Chart
-- Description: Decoded signal values over time for a specific trace
-- =============================================================================

WITH data AS (
  SELECT
    dec.trace_uuid,
    FROM_UNIXTIME(start_timestamp_unix_us / 1000000) AS timestamp,
    signal.obd_value,
    signal.decoded_value,
    signal.data_identifier,
    signal.bit_start,
    COALESCE(
      application.j1939.source_address,
      application.uds.source_address,
      application.none.arbitration_id
    ) AS source_address
  FROM product_analytics_staging.fct_can_trace_decoded AS dec
  LATERAL VIEW EXPLODE(decoded_signals) AS signal
  WHERE dec.trace_uuid = :trace_uuid
    AND NOT signal.is_out_of_range
    AND signal.can_apply_rule
)

SELECT
  data.trace_uuid,
  data.timestamp,
  data.decoded_value,
  data.source_address,
  ov.name AS obd_value_name,
  CONCAT_WS(
    " ",
    FORMAT_STRING("SI=%s", ov.name),
    FORMAT_STRING("SA=%d", data.source_address),
    FORMAT_STRING("DI=%d", data.data_identifier),
    FORMAT_STRING("BS=%d", data.bit_start)
  ) AS label
FROM data
LEFT JOIN definitions.obd_values AS ov ON ov.id = data.obd_value
ORDER BY data.timestamp ASC

