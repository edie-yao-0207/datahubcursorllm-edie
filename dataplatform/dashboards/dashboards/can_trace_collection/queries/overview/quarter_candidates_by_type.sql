-- =============================================================================
-- Dashboard: CAN Trace Collection Dashboard
-- Tab: Overview
-- View: Quarter Candidates by Type
-- =============================================================================
-- Visualization: Area
-- Title: Candidates by Query Type (Last 90 Days)
-- Description: Daily candidate counts by query type for the last 90 days
-- Note: Shows trailing 90-day window to avoid future dates
-- =============================================================================

WITH
-- Calculate trailing 90-day window
date_range AS (
    SELECT
        DATE_SUB(CURRENT_DATE(), 90) AS start_date,
        CURRENT_DATE() AS end_date
    FROM (SELECT 1 AS dummy) t
),

-- Explode tags array from fct_can_traces_required to get one row per tag
traces_with_tags_exploded AS (
    SELECT
        CAST(fctr.date AS DATE) AS date,
        tag AS query_type
    FROM datamodel_dev.fct_can_traces_required fctr
    CROSS JOIN date_range dr
    LATERAL VIEW explode(fctr.tags) tag_table AS tag
    WHERE fctr.date >= dr.start_date
      AND fctr.date <= dr.end_date
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

-- Generate date series for last 90 days
date_series AS (
    SELECT explode(sequence(
        dr.start_date,
        dr.end_date,
        interval 1 day
    )) AS date
    FROM date_range dr
),

-- Cross join date series with all query types to ensure all combinations exist
date_query_types AS (
    SELECT
        CAST(ds.date AS DATE) AS date,
        aqt.query_type
    FROM date_series ds
    CROSS JOIN all_query_types aqt
),

-- Join with actual counts, filling missing dates with 0
daily_counts AS (
    SELECT
        dqt.date,
        dqt.query_type,
        COALESCE(ac.candidate_count, 0) AS candidate_count
    FROM date_query_types dqt
    LEFT JOIN all_candidates ac ON dqt.date = ac.date AND dqt.query_type = ac.query_type
)

SELECT
    date,
    query_type,
    candidate_count
FROM daily_counts
ORDER BY date, query_type

