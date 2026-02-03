-- =============================================================================
-- Dashboard: CAN Trace Collection Dashboard
-- Tab: By Tag
-- View: Device Diversity Per Tag
-- =============================================================================
-- Visualization: Table
-- Title: Device Diversity Per Tag
-- Description: Device coverage and diversity metrics for each trace collection tag (all-time cumulative)
-- =============================================================================

WITH
-- Get all devices in the population (latest date from dim table)
total_population AS (
    SELECT COUNT(DISTINCT org_id, device_id) AS total_devices
    FROM product_analytics_staging.dim_device_vehicle_properties
    WHERE date = (SELECT MAX(date) FROM product_analytics_staging.dim_device_vehicle_properties)
      AND mmyef_id IS NOT NULL
),

-- Explode tag_names array from fct_can_trace_status to get one row per tag per trace
traces_with_tags_exploded AS (
    SELECT
        cts.org_id,
        cts.device_id,
        exploded_tag AS tag_name,
        cts.trace_uuid
    FROM product_analytics_staging.fct_can_trace_status cts
    LATERAL VIEW explode(cts.tag_names) tag_table AS exploded_tag
    WHERE cts.tag_names IS NOT NULL
),

-- Aggregate trace counts per tag per device (all-time)
device_tag_counts AS (
    SELECT
        org_id,
        device_id,
        tag_name,
        COUNT(DISTINCT trace_uuid) AS tag_count
    FROM traces_with_tags_exploded
    GROUP BY org_id, device_id, tag_name
),

-- Aggregate metrics per tag
tag_diversity AS (
    SELECT
        tag_name,
        COUNT(DISTINCT STRUCT(org_id, device_id)) AS devices_with_tag,
        SUM(tag_count) AS total_traces,
        AVG(tag_count) AS avg_traces_per_device,
        PERCENTILE(tag_count, 0.5) AS median_traces_per_device,
        MAX(tag_count) AS max_traces_per_device
    FROM device_tag_counts
    GROUP BY tag_name
)

SELECT
    td.tag_name,
    td.devices_with_tag,
    tp.total_devices AS total_device_population,
    CAST(td.devices_with_tag AS DOUBLE) / NULLIF(CAST(tp.total_devices AS DOUBLE), 0) AS device_coverage_pct,
    td.total_traces,
    ROUND(td.avg_traces_per_device, 2) AS avg_traces_per_device,
    ROUND(td.median_traces_per_device, 2) AS median_traces_per_device,
    td.max_traces_per_device
FROM tag_diversity td
CROSS JOIN total_population tp
ORDER BY td.devices_with_tag DESC, td.total_traces DESC

