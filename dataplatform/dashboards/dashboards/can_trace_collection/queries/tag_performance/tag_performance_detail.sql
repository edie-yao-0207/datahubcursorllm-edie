-- =============================================================================
-- Dashboard: CAN Trace Collection Dashboard
-- Tab: Tag Performance
-- View: Tag Performance Detail
-- =============================================================================
-- Visualization: Table
-- Title: Tag Performance Detail
-- Description: Detailed performance metrics per (tag, MMYEF) combination: collected counts, quota status, training needs, and reverse engineering gaps
-- =============================================================================

WITH
-- Get latest snapshot date for agg_tags_per_mmyef
latest_snapshot_date AS (
    SELECT MAX(date) AS max_date
    FROM datamodel_dev.agg_tags_per_mmyef
),

-- Explode tag_counts_map to get one row per (MMYEF, tag) combination
mmyef_tags_exploded AS (
    SELECT
        atpm.mmyef_id,
        atpm.total_collected_count,
        exploded.tag_name,
        exploded.tag_count AS collected_count,
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

-- Get vehicle properties with translations
vehicle_properties_with_translations AS (
    SELECT DISTINCT 
        dvp2.mmyef_id,
        dvp2.make,
        dvp2.model,
        dvp2.year,
        dvp2.engine_model,
        dvp2.powertrain AS powertrain_id,
        dvp2.fuel_group AS fuel_group_id,
        dvp2.trim,
        pt.name AS powertrain,
        fg.name AS fuel_group
    FROM product_analytics_staging.dim_device_vehicle_properties dvp2
    LEFT JOIN definitions.properties_fuel_powertrain pt ON pt.id = dvp2.powertrain
    LEFT JOIN definitions.properties_fuel_fuelgroup fg ON fg.id = dvp2.fuel_group
    WHERE dvp2.date = (SELECT MAX(date) FROM product_analytics_staging.dim_device_vehicle_properties)
),

-- Training needs: Stream IDs needing more traces (for can-set-training-stream-id-0)
training_latest_date AS (
    SELECT MAX(date) AS max_date
    FROM datamodel_dev.fct_can_trace_training_by_stream_id
    WHERE date BETWEEN :date.min AND :date.max
),

training_needs_by_mmyef AS (
    SELECT
        dvp.mmyef_id,
        COUNT(DISTINCT ftsi.stream_id) AS stream_ids_needing_traces
    FROM datamodel_dev.fct_can_trace_training_by_stream_id ftsi
    JOIN product_analytics_staging.dim_device_vehicle_properties dvp
        ON ftsi.date = dvp.date
        AND ftsi.org_id = dvp.org_id
        AND ftsi.device_id = dvp.device_id
    WHERE ftsi.date = (SELECT max_date FROM training_latest_date)
      AND dvp.mmyef_id IS NOT NULL
    GROUP BY dvp.mmyef_id
),

-- Reverse engineering gaps: Devices with coverage gaps (for training/inference tags)
reverse_eng_latest_date AS (
    SELECT MAX(date) AS max_date
    FROM datamodel_dev.fct_can_trace_reverse_engineering_candidates
    WHERE date BETWEEN :date.min AND :date.max
),

reverse_eng_gaps_by_mmyef AS (
    SELECT
        dvp.mmyef_id,
        COUNT(DISTINCT CASE WHEN rec.for_training = TRUE THEN STRUCT(rec.org_id, rec.device_id) ELSE NULL END) AS devices_for_training,
        COUNT(DISTINCT CASE WHEN rec.for_inference = TRUE THEN STRUCT(rec.org_id, rec.device_id) ELSE NULL END) AS devices_for_inference
    FROM datamodel_dev.fct_can_trace_reverse_engineering_candidates rec
    JOIN product_analytics_staging.dim_device_vehicle_properties dvp
        ON rec.date = dvp.date
        AND rec.org_id = dvp.org_id
        AND rec.device_id = dvp.device_id
    WHERE rec.date = (SELECT max_date FROM reverse_eng_latest_date)
      AND rec.coverage_gap_count > 0
      AND dvp.mmyef_id IS NOT NULL
    GROUP BY dvp.mmyef_id
)

SELECT
    mte.tag_name,
    mte.mmyef_id,
    dvp.make,
    dvp.model,
    dvp.year,
    dvp.engine_model,
    dvp.powertrain,
    dvp.fuel_group,
    dvp.trim,
    mte.collected_count,
    mte.quota,
    CASE 
        WHEN mte.quota > 0 AND mte.collected_count >= mte.quota THEN 'Met Quota'
        WHEN mte.quota > 0 AND mte.collected_count >= CAST(mte.quota * 0.7 AS INT) THEN 'Near Quota'
        WHEN mte.quota > 0 AND mte.collected_count > 0 THEN 'In Progress'
        WHEN mte.quota = 0 THEN 'No Quota'
        ELSE 'Not Started'
    END AS quota_status,
    CAST(mte.collected_count AS DOUBLE) / NULLIF(CAST(mte.quota AS DOUBLE), 0) AS quota_progress,
    mte.total_collected_count,
    -- Training needs (only for can-set-training-stream-id-0)
    CASE 
        WHEN mte.tag_name = 'can-set-training-stream-id-0' THEN COALESCE(tn.stream_ids_needing_traces, 0)
        ELSE NULL
    END AS stream_ids_needing_traces,
    -- Reverse engineering gaps (only for training/inference tags)
    CASE 
        WHEN mte.tag_name = 'can-set-training-0' THEN COALESCE(reg.devices_for_training, 0)
        WHEN mte.tag_name = 'can-set-inference-0' THEN COALESCE(reg.devices_for_inference, 0)
        ELSE NULL
    END AS reverse_eng_devices_count
FROM mmyef_tags_exploded mte
LEFT JOIN vehicle_properties_with_translations dvp ON mte.mmyef_id = dvp.mmyef_id
LEFT JOIN training_needs_by_mmyef tn ON mte.mmyef_id = tn.mmyef_id AND mte.tag_name = 'can-set-training-stream-id-0'
LEFT JOIN reverse_eng_gaps_by_mmyef reg ON mte.mmyef_id = reg.mmyef_id AND mte.tag_name IN ('can-set-training-0', 'can-set-inference-0')
ORDER BY mte.tag_name, mte.collected_count ASC, mte.total_collected_count ASC, mte.mmyef_id
LIMIT 500

