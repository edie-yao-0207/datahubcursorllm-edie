-- =============================================================================
-- Dashboard: CAN Trace Collection Dashboard
-- Tab: Overview
-- View: Quarter Candidates Generated Trend
-- =============================================================================
-- Visualization: Line
-- Title: Candidates Generated Over Time (Last 90 Days)
-- Description: Daily candidates generated for the last 90 days (high volume, random sampling)
-- Note: Shows trailing 90-day window to avoid future dates
-- This shows candidate generation (from fct_can_traces_required), which is 100x more than uploaded traces
-- =============================================================================

WITH
-- Calculate trailing 90-day window
date_range AS (
    SELECT
        DATE_SUB(CURRENT_DATE(), 90) AS start_date,
        CURRENT_DATE() AS end_date
    FROM (SELECT 1 AS dummy) t
),

-- Candidates generated per day in last 90 days
candidates_per_day AS (
    SELECT 
        CAST(ftr.date AS DATE) AS date,
        COUNT(*) AS candidates_generated
    FROM datamodel_dev.fct_can_traces_required ftr
    CROSS JOIN date_range dr
    WHERE ftr.date >= dr.start_date
      AND ftr.date <= dr.end_date
    GROUP BY ftr.date
),

-- Generate date series for last 90 days
date_series AS (
    SELECT explode(sequence(
        dr.start_date,
        dr.end_date,
        interval 1 day
    )) AS date
    FROM date_range dr
)

SELECT
    CAST(ds.date AS DATE) AS date,
    COALESCE(cpd.candidates_generated, 0) AS candidates_generated
FROM date_series ds
LEFT JOIN candidates_per_day cpd ON ds.date = cpd.date
ORDER BY date

