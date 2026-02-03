-- =============================================================================
-- Dashboard: CAN Trace Collection Dashboard
-- Tab: Overview
-- View: Quarter Traces Collected Trend
-- =============================================================================
-- Visualization: Line
-- Title: Traces Collected Over Time (Last 90 Days)
-- Description: Daily traces collected/uploaded to library for the last 90 days (low volume, uploaded traces)
-- Note: Shows trailing 90-day window to avoid future dates
-- This shows uploaded traces (from fct_can_trace_status), which are 100x less than candidates
-- =============================================================================

WITH
-- Calculate trailing 90-day window
date_range AS (
    SELECT
        DATE_SUB(CURRENT_DATE(), 90) AS start_date,
        CURRENT_DATE() AS end_date
    FROM (SELECT 1 AS dummy) t
),

-- Traces collected per day in last 90 days
collected_per_day AS (
    SELECT 
        CAST(cts.date AS DATE) AS date,
        COUNT(*) AS traces_collected
    FROM product_analytics_staging.fct_can_trace_status cts
    CROSS JOIN date_range dr
    WHERE cts.date >= dr.start_date
      AND cts.date <= dr.end_date
    GROUP BY date
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
    COALESCE(cpd.traces_collected, 0) AS traces_collected
FROM date_series ds
LEFT JOIN collected_per_day cpd ON ds.date = cpd.date
ORDER BY date

