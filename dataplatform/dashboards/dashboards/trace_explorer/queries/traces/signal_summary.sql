-- =============================================================================
-- Dashboard: VDP Trace Explorer
-- Tab: Traces
-- View: Signal Summary
-- =============================================================================
-- Visualization: Table
-- Description: Aggregate statistics per signal in a trace
-- =============================================================================

WITH data AS (
  SELECT
    dec.trace_uuid,
    signal.obd_value,
    signal.decoded_value,
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
  ov.name AS obd_value_name,
  data.source_address,
  COUNT(*) AS readings,
  ROUND(MIN(data.decoded_value), 2) AS min_value,
  ROUND(MAX(data.decoded_value), 2) AS max_value,
  ROUND(AVG(data.decoded_value), 2) AS avg_value,
  ROUND(STDDEV(data.decoded_value), 2) AS std_dev
FROM data
LEFT JOIN definitions.obd_values AS ov ON ov.id = data.obd_value
GROUP BY data.trace_uuid, ov.name, data.source_address
ORDER BY readings DESC


