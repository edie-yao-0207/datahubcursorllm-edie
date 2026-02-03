-- =============================================================================
-- Dashboard: CAN Trace Collection Dashboard
-- Tab: Candidates
-- View: Ranking Distribution
-- =============================================================================
-- Visualization: Bar
-- Title: Ranking Distribution (Top 100 Ranks)
-- Description: Distribution of candidates across rank buckets
-- =============================================================================

WITH latest_date AS (
    SELECT MAX(date) AS max_date
    FROM datamodel_dev.fct_can_traces_required
    WHERE date BETWEEN :date.min AND :date.max
),

all_ranks AS (
    SELECT global_trace_rank
    FROM datamodel_dev.fct_can_trace_representative_candidates
    WHERE date = (SELECT max_date FROM latest_date)
    
    UNION ALL
    
    SELECT global_trace_rank
    FROM datamodel_dev.fct_can_trace_reverse_engineering_candidates
    WHERE date = (SELECT max_date FROM latest_date)
    
    UNION ALL
    
    SELECT global_trace_rank
    FROM datamodel_dev.fct_can_trace_training_by_stream_id
    WHERE date = (SELECT max_date FROM latest_date)
    
    UNION ALL
    
    SELECT global_trace_rank
    FROM datamodel_dev.fct_can_trace_seatbelt_trip_start_candidates
    WHERE date = (SELECT max_date FROM latest_date)
)

SELECT
    CASE
        WHEN global_trace_rank <= 10 THEN '1-10'
        WHEN global_trace_rank <= 25 THEN '11-25'
        WHEN global_trace_rank <= 50 THEN '26-50'
        WHEN global_trace_rank <= 100 THEN '51-100'
        ELSE '100+'
    END AS rank_bucket,
    COUNT(*) AS candidate_count
FROM all_ranks
GROUP BY rank_bucket
ORDER BY 
    CASE rank_bucket
        WHEN '1-10' THEN 1
        WHEN '11-25' THEN 2
        WHEN '26-50' THEN 3
        WHEN '51-100' THEN 4
        ELSE 5
    END

