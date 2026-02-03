-- =============================================================================
-- Dashboard: CAN Trace Collection Dashboard
-- Tab: By Tag
-- View: Training Needs by Tag (Stream IDs)
-- =============================================================================
-- Visualization: Table
-- Title: Training Needs by Tag (Stream IDs)
-- Description: Stream IDs/OBD values needing more traces for training, grouped by tag
-- Note: Training needs are specific to can-set-training-stream-id-0 tag. 
-- Table may be empty if all stream_ids have sufficient coverage.
-- =============================================================================

WITH
-- Get latest date for training candidates (handle empty table case)
training_latest_date AS (
    SELECT MAX(date) AS max_date
    FROM datamodel_dev.fct_can_trace_training_by_stream_id
    WHERE date BETWEEN :date.min AND :date.max
),

-- Aggregate training candidates by stream_id and obd_value
training_candidates_agg AS (
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
)

SELECT
    'can-set-training-stream-id-0' AS tag_name,
    stream_id,
    obd_value,
    obd_value_name,
    candidate_count,
    best_rank,
    worst_rank,
    mmyef_count
FROM training_candidates_agg
ORDER BY candidate_count DESC, best_rank ASC
LIMIT 100

