-- =============================================================================
-- Dashboard: CAN Trace Collection Dashboard
-- Tab: Overview
-- View: Coverage Percentage by Tag
-- =============================================================================
-- Visualization: Line
-- Title: MMYEF Coverage % by Tag Over Time
-- Description: Percentage of MMYEF population covered by each tag over time (cumulative)
-- Note: Uses agg_tags_per_mmyef which stores cumulative counts per date partition.
-- Coverage = (MMYEFs with traces for tag up to date X) / (Total MMYEFs in population)
-- =============================================================================

WITH
-- Get total MMYEF population (use latest date for normalization)
total_population AS (
    SELECT COUNT(DISTINCT mmyef_id) AS total_mmyefs
    FROM product_analytics_staging.dim_device_vehicle_properties
    WHERE date = (SELECT MAX(date) FROM product_analytics_staging.dim_device_vehicle_properties)
      AND mmyef_id IS NOT NULL
),

-- Explode tag_counts_map from agg_tags_per_mmyef to get one row per (date, MMYEF, tag)
mmyef_tags_exploded AS (
    SELECT
        CAST(atpm.date AS DATE) AS date,
        atpm.mmyef_id,
        exploded.tag_name,
        exploded.tag_count
    FROM datamodel_dev.agg_tags_per_mmyef atpm
    LATERAL VIEW explode(atpm.tag_counts_map) exploded AS tag_name, tag_count
    WHERE atpm.date BETWEEN :date.min AND :date.max
      AND atpm.tag_counts_map IS NOT NULL
),

-- Get all distinct tags from the data (to ensure we show all tags even if date range is empty)
all_tags AS (
    SELECT DISTINCT exploded.tag_name
    FROM datamodel_dev.agg_tags_per_mmyef atpm
    LATERAL VIEW explode(atpm.tag_counts_map) exploded AS tag_name, tag_count
    WHERE atpm.tag_counts_map IS NOT NULL
),

-- Count distinct MMYEFs with each tag per date (MMYEFs with tag_count > 0)
coverage_counts AS (
    SELECT
        date,
        tag_name,
        COUNT(DISTINCT CASE WHEN tag_count > 0 THEN mmyef_id END) AS mmyefs_with_tag
    FROM mmyef_tags_exploded
    GROUP BY date, tag_name
),

-- Generate date series
date_series AS (
    SELECT explode(sequence(
        date(:date.min),
        date(:date.max),
        interval 1 day
    )) AS date
),

-- Cross join date series with all tags to ensure all combinations exist
date_tags AS (
    SELECT
        CAST(ds.date AS DATE) AS date,
        at.tag_name
    FROM date_series ds
    CROSS JOIN all_tags at
),

-- Join with actual counts and calculate coverage percentage
coverage_with_pct AS (
    SELECT
        dt.date,
        dt.tag_name,
        COALESCE(cc.mmyefs_with_tag, 0) AS mmyefs_with_tag,
        CAST(COALESCE(cc.mmyefs_with_tag, 0) AS DOUBLE) / NULLIF(CAST(tp.total_mmyefs AS DOUBLE), 0) AS coverage_pct
    FROM date_tags dt
    CROSS JOIN total_population tp
    LEFT JOIN coverage_counts cc ON dt.date = cc.date AND dt.tag_name = cc.tag_name
)

SELECT
    date,
    tag_name,
    coverage_pct
FROM coverage_with_pct
ORDER BY date, tag_name

