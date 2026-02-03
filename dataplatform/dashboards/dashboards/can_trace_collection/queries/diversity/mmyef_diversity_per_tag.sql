-- =============================================================================
-- Dashboard: CAN Trace Collection Dashboard
-- Tab: Diversity Metrics
-- View: MMYEF Diversity Per Tag
-- =============================================================================
-- Visualization: Table
-- Title: MMYEF Diversity Per Tag
-- Description: MMYEF coverage and diversity metrics for each trace collection tag (all-time cumulative)
-- =============================================================================

WITH
-- Get all MMYEFs in the population (latest date from dim table)
total_population AS (
    SELECT COUNT(DISTINCT mmyef_id) AS total_mmyefs
    FROM product_analytics_staging.dim_device_vehicle_properties
    WHERE date = (SELECT MAX(date) FROM product_analytics_staging.dim_device_vehicle_properties)
      AND mmyef_id IS NOT NULL
),

-- Explode tag_names array from fct_can_trace_status to get one row per tag per trace
traces_with_tags_exploded AS (
    SELECT
        dvp.mmyef_id,
        exploded_tag AS tag_name,
        cts.trace_uuid
    FROM product_analytics_staging.fct_can_trace_status cts
    JOIN product_analytics_staging.dim_device_vehicle_properties dvp
        ON cts.date = dvp.date
        AND cts.org_id = dvp.org_id
        AND cts.device_id = dvp.device_id
    LATERAL VIEW explode(cts.tag_names) tag_table AS exploded_tag
    WHERE dvp.mmyef_id IS NOT NULL
      AND cts.tag_names IS NOT NULL
),

-- Aggregate trace counts per tag per MMYEF (all-time)
mmyef_tag_counts AS (
    SELECT
        mmyef_id,
        tag_name,
        COUNT(DISTINCT trace_uuid) AS tag_count
    FROM traces_with_tags_exploded
    GROUP BY mmyef_id, tag_name
),

-- Aggregate metrics per tag
tag_diversity AS (
    SELECT
        tag_name,
        COUNT(DISTINCT mmyef_id) AS mmyefs_with_tag,
        SUM(tag_count) AS total_traces,
        AVG(tag_count) AS avg_traces_per_mmyef,
        PERCENTILE(tag_count, 0.5) AS median_traces_per_mmyef,
        MAX(tag_count) AS max_traces_per_mmyef
    FROM mmyef_tag_counts
    GROUP BY tag_name
)

SELECT
    td.tag_name,
    td.mmyefs_with_tag,
    tp.total_mmyefs AS total_mmyef_population,
    CAST(td.mmyefs_with_tag AS DOUBLE) / NULLIF(CAST(tp.total_mmyefs AS DOUBLE), 0) AS mmyef_coverage_pct,
    td.total_traces,
    ROUND(td.avg_traces_per_mmyef, 2) AS avg_traces_per_mmyef,
    ROUND(td.median_traces_per_mmyef, 2) AS median_traces_per_mmyef,
    td.max_traces_per_mmyef
FROM tag_diversity td
CROSS JOIN total_population tp
ORDER BY td.mmyefs_with_tag DESC, td.total_traces DESC
