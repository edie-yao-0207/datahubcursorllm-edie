-- =============================================================================
-- Dashboard: CAN Trace Collection Dashboard
-- Tab: Candidates
-- View: Candidate Counts Summary
-- =============================================================================
-- Visualization: Table
-- Title: Candidate Counts by Query Type (Latest Date)
-- Description: Summary of candidate counts by query type for the latest date
-- =============================================================================

WITH latest_date AS (
    SELECT MAX(date) AS max_date
    FROM datamodel_dev.fct_can_traces_required
    WHERE date BETWEEN :date.min AND :date.max
)

SELECT
    'Representative' AS query_type,
    COUNT(*) AS candidate_count
FROM datamodel_dev.fct_can_trace_representative_candidates
WHERE date = (SELECT max_date FROM latest_date)

UNION ALL

SELECT
    'Training (Reverse Engineering)' AS query_type,
    COUNT(*) AS candidate_count
FROM datamodel_dev.fct_can_trace_reverse_engineering_candidates
WHERE date = (SELECT max_date FROM latest_date)
  AND for_training = TRUE

UNION ALL

SELECT
    'Inference (Reverse Engineering)' AS query_type,
    COUNT(*) AS candidate_count
FROM datamodel_dev.fct_can_trace_reverse_engineering_candidates
WHERE date = (SELECT max_date FROM latest_date)
  AND for_inference = TRUE

UNION ALL

SELECT
    'Training (Stream ID)' AS query_type,
    COUNT(*) AS candidate_count
FROM datamodel_dev.fct_can_trace_training_by_stream_id
WHERE date = (SELECT max_date FROM latest_date)

UNION ALL

SELECT
    'Seatbelt Trip Start' AS query_type,
    COUNT(*) AS candidate_count
FROM datamodel_dev.fct_can_trace_seatbelt_trip_start_candidates
WHERE date = (SELECT max_date FROM latest_date)

ORDER BY candidate_count DESC

