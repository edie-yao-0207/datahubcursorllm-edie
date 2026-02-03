-- =============================================================================
-- Dashboard: CAN Trace Collection Dashboard
-- Tab: By Tag + Stream
-- View: Training Candidates by Stream ID
-- =============================================================================
-- Visualization: Table
-- Title: Training Candidates by Stream ID
-- Description: Training candidates grouped by stream ID for underrepresented signals
-- Note: Shows candidates when table has data. Table may be empty if all stream_ids have sufficient coverage.
-- =============================================================================

WITH
-- Get latest date for training candidates (handle empty table case)
training_latest_date AS (
    SELECT MAX(date) AS max_date
    FROM datamodel_dev.fct_can_trace_training_by_stream_id
    WHERE date BETWEEN :date.min AND :date.max
)

SELECT
    ftsi.stream_id,
    ftsi.obd_value,
    obd.name AS obd_value_name,
    COUNT(*) AS candidate_count,
    MIN(ftsi.global_trace_rank) AS best_rank,
    MAX(ftsi.global_trace_rank) AS worst_rank,
    COUNT(DISTINCT dvp.mmyef_id) AS mmyef_count
FROM datamodel_dev.fct_can_trace_training_by_stream_id ftsi
LEFT JOIN product_analytics_staging.dim_device_vehicle_properties dvp
    ON ftsi.date = dvp.date
    AND ftsi.org_id = dvp.org_id
    AND ftsi.device_id = dvp.device_id
LEFT JOIN definitions.obd_values obd ON ftsi.obd_value = obd.id
CROSS JOIN training_latest_date tld
WHERE ftsi.date = tld.max_date
  AND tld.max_date IS NOT NULL
GROUP BY ftsi.stream_id, ftsi.obd_value, obd.name
ORDER BY candidate_count DESC, best_rank ASC
LIMIT 50

