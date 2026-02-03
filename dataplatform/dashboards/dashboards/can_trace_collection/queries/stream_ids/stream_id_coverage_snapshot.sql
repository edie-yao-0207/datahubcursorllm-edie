-- =============================================================================
-- Dashboard: CAN Trace Collection Dashboard
-- Tab: Stream ID Coverage
-- View: Stream ID Coverage Snapshot
-- =============================================================================
-- Visualization: Counter
-- Title: Stream ID Coverage Snapshot
-- Description: Total stream IDs and percentage with sufficient data (>= 200 traces globally per stream_id)
-- =============================================================================

-- Note: agg_mmyef_stream_ids stores cumulative counts per date partition.
-- MAX(date) partition contains cumulative counts up to that date (effectively all-time).
-- Each row represents (date, mmyef_id, stream_id, obd_value), so we need to aggregate
-- trace_count globally per stream_id to determine if a stream_id has sufficient data.
WITH latest_snapshot_date AS (
    SELECT MAX(date) AS max_date
    FROM datamodel_dev.agg_mmyef_stream_ids
),

-- Aggregate trace_count globally per stream_id (across all mmyef_id/obd_value combinations)
stream_id_totals AS (
    SELECT 
        amsi.stream_id,
        SUM(amsi.trace_count) AS total_trace_count
    FROM datamodel_dev.agg_mmyef_stream_ids amsi
    CROSS JOIN latest_snapshot_date lsd
    WHERE amsi.date = lsd.max_date
    GROUP BY amsi.stream_id
)

SELECT
    COUNT(DISTINCT stream_id) AS total_stream_ids,
    COUNT(DISTINCT CASE WHEN total_trace_count < 200 THEN stream_id END) AS underrepresented_stream_ids,
    CAST(COUNT(DISTINCT CASE WHEN total_trace_count >= 200 THEN stream_id END) AS DOUBLE) / 
        NULLIF(CAST(COUNT(DISTINCT stream_id) AS DOUBLE), 0) AS percent_sufficient
FROM stream_id_totals
