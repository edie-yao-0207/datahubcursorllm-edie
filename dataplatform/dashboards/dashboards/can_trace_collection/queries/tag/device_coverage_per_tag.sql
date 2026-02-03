-- =============================================================================
-- Dashboard: CAN Trace Collection Dashboard
-- Tab: By Tag
-- View: Device Coverage Percentage by Tag
-- =============================================================================
-- Visualization: Line
-- Title: Device Coverage % by Tag Over Time
-- Description: Percentage of device population covered by each tag over time (cumulative)
-- Note: Uses fct_can_trace_status which stores all-time collected traces.
-- Coverage = (Devices with traces for tag up to date X) / (Total devices in population)
-- =============================================================================

WITH
-- Get total device population (use latest date for normalization)
total_population AS (
    SELECT COUNT(DISTINCT org_id, device_id) AS total_devices
    FROM product_analytics_staging.dim_device_vehicle_properties
    WHERE date = (SELECT MAX(date) FROM product_analytics_staging.dim_device_vehicle_properties)
      AND mmyef_id IS NOT NULL
),

-- Explode tag_names array from fct_can_trace_status to get one row per tag per trace
-- Count distinct devices with each tag per date
traces_with_tags_exploded AS (
    SELECT
        CAST(cts.date AS DATE) AS date,
        cts.org_id,
        cts.device_id,
        exploded_tag AS tag_name,
        cts.trace_uuid
    FROM product_analytics_staging.fct_can_trace_status cts
    LATERAL VIEW explode(cts.tag_names) tag_table AS exploded_tag
    WHERE cts.date BETWEEN :date.min AND :date.max
      AND cts.tag_names IS NOT NULL
),

-- Count distinct devices with each tag per date (cumulative: all devices seen up to this date)
-- Similar to MMYEF coverage, but using device-level aggregation
device_tag_first_seen AS (
    SELECT
        org_id,
        device_id,
        tag_name,
        MIN(date) AS first_seen_date
    FROM traces_with_tags_exploded
    GROUP BY org_id, device_id, tag_name
),

-- Get all distinct tags from the data (to ensure we show all tags even if date range is empty)
all_tags AS (
    SELECT DISTINCT exploded_tag AS tag_name
    FROM product_analytics_staging.fct_can_trace_status cts
    LATERAL VIEW explode(cts.tag_names) tag_table AS exploded_tag
    WHERE cts.tag_names IS NOT NULL
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

-- Count cumulative devices (devices that first appeared on or before this date)
cumulative_device_tags AS (
    SELECT
        dt.date,
        dt.tag_name,
        COUNT(DISTINCT dtfs.org_id, dtfs.device_id) AS devices_with_tag
    FROM date_tags dt
    LEFT JOIN device_tag_first_seen dtfs
        ON dtfs.tag_name = dt.tag_name
        AND dtfs.first_seen_date <= dt.date
    GROUP BY dt.date, dt.tag_name
),

coverage_with_pct AS (
    SELECT
        cdt.date,
        cdt.tag_name,
        COALESCE(cdt.devices_with_tag, 0) AS devices_with_tag,
        CAST(COALESCE(cdt.devices_with_tag, 0) AS DOUBLE) / NULLIF(CAST(tp.total_devices AS DOUBLE), 0) AS coverage_pct
    FROM cumulative_device_tags cdt
    CROSS JOIN total_population tp
)

SELECT
    date,
    tag_name,
    coverage_pct
FROM coverage_with_pct
ORDER BY date, tag_name

