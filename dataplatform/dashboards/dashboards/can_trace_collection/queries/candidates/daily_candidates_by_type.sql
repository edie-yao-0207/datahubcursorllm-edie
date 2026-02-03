-- =============================================================================
-- Dashboard: CAN Trace Collection Dashboard
-- Tab: Candidates
-- View: Daily Candidates by Query Type
-- =============================================================================
-- Visualization: Line
-- Title: Daily Candidates by Query Type
-- Description: Daily candidate counts broken down by query type
-- =============================================================================

WITH
-- Explode tags array from fct_can_traces_required to get one row per tag
traces_with_tags_exploded AS (
    SELECT
        CAST(fctr.date AS DATE) AS date,
        tag AS query_type
    FROM datamodel_dev.fct_can_traces_required fctr
    LATERAL VIEW explode(fctr.tags) tag_table AS tag
    WHERE fctr.date BETWEEN :date.min AND :date.max
),

-- Get all distinct query types from the data (to ensure we show all tags even if date range is empty)
all_query_types AS (
    SELECT DISTINCT tag AS query_type
    FROM datamodel_dev.fct_can_traces_required fctr
    LATERAL VIEW explode(fctr.tags) tag_table AS tag
),

-- Count candidates per date and query_type
all_candidates AS (
    SELECT 
        date,
        query_type,
        COUNT(*) AS candidate_count
    FROM traces_with_tags_exploded
    GROUP BY date, query_type
),

-- Generate date series
date_series AS (
    SELECT explode(sequence(
        date(:date.min),
        date(:date.max),
        interval 1 day
    )) AS date
),

-- Cross join date series with all query types to ensure all combinations exist
date_query_types AS (
    SELECT 
        CAST(ds.date AS DATE) AS date,
        aqt.query_type
    FROM date_series ds
    CROSS JOIN all_query_types aqt
)

-- Join with actual counts, filling missing dates with 0
SELECT
    dqt.date,
    dqt.query_type,
    COALESCE(ac.candidate_count, 0) AS candidate_count
FROM date_query_types dqt
LEFT JOIN all_candidates ac ON dqt.date = ac.date AND dqt.query_type = ac.query_type
ORDER BY date, query_type

