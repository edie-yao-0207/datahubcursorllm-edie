-- =============================================================================
-- Dashboard: CAN Trace Collection Dashboard
-- Tab: Diversity Metrics
-- View: Collection Distribution
-- =============================================================================
-- Visualization: Bar
-- Title: Collection Distribution (Traces per MMYEF)
-- Description: Histogram of MMYEFs by trace count buckets (all-time cumulative)
-- =============================================================================

WITH
-- Get all MMYEFs in population (latest date from dim table)
all_mmyefs AS (
    SELECT DISTINCT dvp.mmyef_id
    FROM product_analytics_staging.dim_device_vehicle_properties dvp
    WHERE dvp.date = (SELECT MAX(date) FROM product_analytics_staging.dim_device_vehicle_properties)
      AND dvp.mmyef_id IS NOT NULL
),

-- Aggregate collected traces per MMYEF (all-time from fct_can_trace_status)
mmyef_trace_counts AS (
    SELECT
        dvp.mmyef_id,
        COUNT(DISTINCT cts.trace_uuid) AS total_collected_count
    FROM product_analytics_staging.fct_can_trace_status cts
    JOIN product_analytics_staging.dim_device_vehicle_properties dvp
        ON cts.date = dvp.date
        AND cts.org_id = dvp.org_id
        AND cts.device_id = dvp.device_id
    WHERE dvp.mmyef_id IS NOT NULL
    GROUP BY dvp.mmyef_id
),

-- Join all MMYEFs with trace counts (0 for MMYEFs with no traces)
mmyef_with_counts AS (
    SELECT
        am.mmyef_id,
        COALESCE(mtc.total_collected_count, 0) AS total_collected_count
    FROM all_mmyefs am
    LEFT JOIN mmyef_trace_counts mtc ON am.mmyef_id = mtc.mmyef_id
)

SELECT
    CASE
        WHEN total_collected_count = 0 THEN '0'
        WHEN total_collected_count BETWEEN 1 AND 5 THEN '1-5'
        WHEN total_collected_count BETWEEN 6 AND 10 THEN '6-10'
        WHEN total_collected_count BETWEEN 11 AND 20 THEN '11-20'
        WHEN total_collected_count BETWEEN 21 AND 50 THEN '21-50'
        WHEN total_collected_count BETWEEN 51 AND 100 THEN '51-100'
        ELSE '100+'
    END AS trace_count_bucket,
    COUNT(*) AS mmyef_count
FROM mmyef_with_counts
GROUP BY trace_count_bucket
ORDER BY 
    CASE trace_count_bucket
        WHEN '0' THEN 1
        WHEN '1-5' THEN 2
        WHEN '6-10' THEN 3
        WHEN '11-20' THEN 4
        WHEN '21-50' THEN 5
        WHEN '51-100' THEN 6
        ELSE 7
    END
