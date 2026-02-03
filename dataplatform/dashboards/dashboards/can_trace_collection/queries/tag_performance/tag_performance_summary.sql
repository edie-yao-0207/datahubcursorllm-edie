-- =============================================================================
-- Dashboard: CAN Trace Collection Dashboard
-- Tab: Tag Performance
-- View: Tag Performance Summary
-- =============================================================================
-- Visualization: Table
-- Title: Tag Performance Summary
-- Description: High-level performance metrics per tag: MMYEF coverage, quota fulfillment, training needs, and reverse engineering gaps
-- =============================================================================

WITH
-- Get all MMYEFs in the population (latest date from dim table)
total_population AS (
    SELECT COUNT(DISTINCT mmyef_id) AS total_mmyefs
    FROM product_analytics_staging.dim_device_vehicle_properties
    WHERE date = (SELECT MAX(date) FROM product_analytics_staging.dim_device_vehicle_properties)
      AND mmyef_id IS NOT NULL
),

-- MMYEF coverage per tag (from mmyef_diversity_per_tag logic)
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

mmyef_tag_counts AS (
    SELECT
        mmyef_id,
        tag_name,
        COUNT(DISTINCT trace_uuid) AS tag_count
    FROM traces_with_tags_exploded
    GROUP BY mmyef_id, tag_name
),

tag_coverage AS (
    SELECT
        tag_name,
        COUNT(DISTINCT mmyef_id) AS mmyefs_with_tag,
        SUM(tag_count) AS total_traces
    FROM mmyef_tag_counts
    GROUP BY tag_name
),

-- Quota fulfillment per tag (from quota_fulfillment_by_type logic)
latest_snapshot_date AS (
    SELECT MAX(date) AS max_date
    FROM datamodel_dev.agg_tags_per_mmyef
),

mmyef_tags_exploded AS (
    SELECT
        atpm.mmyef_id,
        exploded.tag_name,
        exploded.tag_count,
        CASE 
            WHEN exploded.tag_name = 'can-set-main-0' THEN 10
            ELSE 0  -- No quota for other tags
        END AS quota
    FROM datamodel_dev.agg_tags_per_mmyef atpm
    CROSS JOIN latest_snapshot_date lsd
    LATERAL VIEW explode(atpm.tag_counts_map) exploded AS tag_name, tag_count
    WHERE atpm.date = lsd.max_date
      AND atpm.tag_counts_map IS NOT NULL
),

tag_quota_aggregates AS (
    SELECT
        tag_name,
        quota,
        COUNT(DISTINCT CASE WHEN tag_count >= quota AND quota > 0 THEN mmyef_id END) AS meeting_quota_count,
        COUNT(DISTINCT mmyef_id) AS total_mmyefs_with_tag_for_quota
    FROM mmyef_tags_exploded
    GROUP BY tag_name, quota
),

tag_quota_summary AS (
    SELECT
        tag_name,
        quota,
        COALESCE(meeting_quota_count, 0) AS mmyefs_meeting_quota,
        total_mmyefs_with_tag_for_quota AS total_mmyefs_with_quota,
        CASE 
            WHEN quota > 0 THEN
                CAST(COALESCE(meeting_quota_count, 0) AS DOUBLE) / NULLIF(CAST(total_mmyefs_with_tag_for_quota AS DOUBLE), 0)
            ELSE NULL
        END AS quota_fulfillment_rate
    FROM tag_quota_aggregates
),

-- Training needs: Stream IDs needing more traces (for can-set-training-stream-id-0 tag)
-- Get latest date for training candidates
training_latest_date AS (
    SELECT MAX(date) AS max_date
    FROM datamodel_dev.fct_can_trace_training_by_stream_id
    WHERE date BETWEEN :date.min AND :date.max
),

training_needs AS (
    SELECT 
        'can-set-training-stream-id-0' AS tag_name,
        COUNT(DISTINCT stream_id) AS stream_ids_needing_traces
    FROM datamodel_dev.fct_can_trace_training_by_stream_id
    WHERE date = (SELECT max_date FROM training_latest_date)
),

-- Reverse engineering gaps: Devices with coverage gaps (applies to training/inference tags)
reverse_eng_latest_date AS (
    SELECT MAX(date) AS max_date
    FROM datamodel_dev.fct_can_trace_reverse_engineering_candidates
    WHERE date BETWEEN :date.min AND :date.max
),

reverse_eng_gaps AS (
    SELECT
        COUNT(DISTINCT STRUCT(org_id, device_id)) AS devices_with_gaps,
        COUNT(DISTINCT CASE WHEN for_training = TRUE THEN STRUCT(org_id, device_id) ELSE NULL END) AS devices_for_training,
        COUNT(DISTINCT CASE WHEN for_inference = TRUE THEN STRUCT(org_id, device_id) ELSE NULL END) AS devices_for_inference
    FROM datamodel_dev.fct_can_trace_reverse_engineering_candidates
    WHERE date = (SELECT max_date FROM reverse_eng_latest_date)
      AND coverage_gap_count > 0
),

-- Get all tags from tag_coverage
all_tags AS (
    SELECT DISTINCT tag_name FROM tag_coverage
)

SELECT
    at.tag_name,
    -- Coverage metrics
    COALESCE(tc.mmyefs_with_tag, 0) AS mmyefs_with_tag,
    tp.total_mmyefs AS total_mmyef_population,
    CASE 
        WHEN tp.total_mmyefs > 0 THEN
            CAST(COALESCE(tc.mmyefs_with_tag, 0) AS DOUBLE) / CAST(tp.total_mmyefs AS DOUBLE)
        ELSE 0.0
    END AS mmyef_coverage_pct,
    COALESCE(tc.total_traces, 0) AS total_traces,
    -- Quota metrics
    COALESCE(tqs.quota, 0) AS quota,
    COALESCE(tqs.mmyefs_meeting_quota, 0) AS mmyefs_meeting_quota,
    COALESCE(tqs.total_mmyefs_with_quota, 0) AS total_mmyefs_with_quota,
    tqs.quota_fulfillment_rate,
    -- Training needs (only for can-set-training-stream-id-0)
    CASE 
        WHEN at.tag_name = 'can-set-training-stream-id-0' THEN COALESCE(tn.stream_ids_needing_traces, 0)
        ELSE NULL
    END AS stream_ids_needing_traces,
    -- Reverse engineering gaps (applies to training/inference tags)
    CASE 
        WHEN at.tag_name = 'can-set-training-0' THEN COALESCE(reg.devices_for_training, 0)
        WHEN at.tag_name = 'can-set-inference-0' THEN COALESCE(reg.devices_for_inference, 0)
        ELSE NULL
    END AS reverse_eng_devices_count
FROM all_tags at
CROSS JOIN total_population tp
CROSS JOIN reverse_eng_gaps reg
LEFT JOIN tag_coverage tc ON at.tag_name = tc.tag_name
LEFT JOIN tag_quota_summary tqs ON at.tag_name = tqs.tag_name
LEFT JOIN training_needs tn ON at.tag_name = tn.tag_name
ORDER BY tc.mmyefs_with_tag DESC, tc.total_traces DESC

