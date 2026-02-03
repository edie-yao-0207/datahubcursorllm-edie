-- =============================================================================
-- Dashboard: ELD Malfunctions Dashboard
-- Tab: Overview
-- View: Signal Disagreement Summary
-- =============================================================================
-- Visualization: Table
-- Title: "Signal Disagreement Summary"
-- Description: Per-signal aggregated disagreement metrics across all devices. Shows which signals have the most disagreement between sources. Metrics include device counts, multi-source prevalence, average CV, agreement scores, and high disagreement counts.
-- =============================================================================

WITH
-- Explode the map to get individual OBD values with their comparison metrics
source_comparison_exploded AS (
    SELECT
        sc.date,
        sc.org_id,
        sc.device_id,
        obd_value_key AS obd_value,
        comparison_data.source_count,
        comparison_data.cv,
        comparison_data.agreement_score,
        comparison_data.max_divergence_pct,
        comparison_data.mean_value,
        comparison_data.median_value
    FROM datamodel_dev.agg_signal_source_comparison_daily sc
    LATERAL VIEW explode(obd_value_to_comparison_map) AS obd_value_key, comparison_data
    WHERE sc.date BETWEEN :date.min AND :date.max
        AND comparison_data IS NOT NULL
),

-- Aggregate per signal across all devices
signal_aggregation AS (
    SELECT
        obd_value,
        COUNT(DISTINCT device_id) AS device_count,
        COUNT(DISTINCT CASE WHEN source_count > 1 THEN device_id END) AS multi_source_device_count,
        CAST(COUNT(DISTINCT CASE WHEN source_count > 1 THEN device_id END) AS DOUBLE) / CAST(COUNT(DISTINCT device_id) AS DOUBLE) * 100.0 AS multi_source_pct,
        AVG(CASE WHEN source_count > 1 THEN cv ELSE NULL END) AS avg_cv,
        PERCENTILE_APPROX(CASE WHEN source_count > 1 THEN cv ELSE NULL END, 0.5) AS median_cv,
        PERCENTILE_APPROX(CASE WHEN source_count > 1 THEN cv ELSE NULL END, 0.95) AS p95_cv,
        AVG(CASE WHEN source_count > 1 THEN agreement_score ELSE NULL END) AS avg_agreement_score,
        PERCENTILE_APPROX(CASE WHEN source_count > 1 THEN agreement_score ELSE NULL END, 0.5) AS median_agreement_score,
        PERCENTILE_APPROX(CASE WHEN source_count > 1 THEN agreement_score ELSE NULL END, 0.05) AS p5_agreement_score,
        AVG(CASE WHEN source_count > 1 AND ABS(mean_value) > 0.001 THEN max_divergence_pct ELSE NULL END) AS avg_max_divergence_pct,
        PERCENTILE_APPROX(CASE WHEN source_count > 1 AND ABS(mean_value) > 0.001 THEN max_divergence_pct ELSE NULL END, 0.5) AS median_max_divergence_pct,
        PERCENTILE_APPROX(CASE WHEN source_count > 1 AND ABS(mean_value) > 0.001 THEN max_divergence_pct ELSE NULL END, 0.95) AS p95_max_divergence_pct,
        COUNT(DISTINCT CASE 
            WHEN source_count > 1 AND (cv > 0.1 OR agreement_score < 80.0) 
            THEN device_id 
        END) AS high_disagreement_count,
        COUNT(DISTINCT CASE 
            WHEN source_count > 1 AND cv > 0.01
            THEN device_id 
        END) AS devices_with_measurable_cv,
        -- Count devices with disagreement (using same criteria as overall_disagreement_stats)
        COUNT(DISTINCT CASE 
            WHEN source_count > 1 AND (
                cv > 0.01 OR 
                agreement_score < 95.0 OR
                (max_divergence_pct > 10.0 AND ABS(mean_value) > 0.001)
            ) THEN device_id 
        END) AS devices_with_disagreement
    FROM source_comparison_exploded
    GROUP BY obd_value
),

-- Join with definition table to get signal names
signal_with_names AS (
    SELECT
        sa.*,
        COALESCE(obd_def.name, CAST(sa.obd_value AS STRING)) AS obd_value_name
    FROM signal_aggregation sa
    LEFT JOIN definitions.obd_values AS obd_def
        ON sa.obd_value = obd_def.id
)

SELECT
    obd_value_name,
    device_count,
    multi_source_device_count,
    multi_source_pct,
    devices_with_disagreement,
    CAST(devices_with_disagreement AS DOUBLE) / NULLIF(multi_source_device_count, 0) * 100.0 AS pct_multi_source_devices_with_disagreement,
    CAST(devices_with_disagreement AS DOUBLE) / NULLIF(device_count, 0) * 100.0 AS pct_all_devices_with_disagreement,
    avg_cv,
    median_cv,
    p95_cv,
    avg_agreement_score,
    median_agreement_score,
    p5_agreement_score,
    avg_max_divergence_pct,
    median_max_divergence_pct,
    p95_max_divergence_pct,
    high_disagreement_count,
    devices_with_measurable_cv
FROM signal_with_names
WHERE multi_source_device_count > 0
ORDER BY 
    -- Order by highest disagreement percentage (of multi-source devices) first, then by worst-case metrics
    COALESCE(CAST(devices_with_disagreement AS DOUBLE) / NULLIF(multi_source_device_count, 0) * 100.0, 0) DESC,
    COALESCE(p95_cv, 0) DESC,
    COALESCE(p5_agreement_score, 100) ASC,
    COALESCE(p95_max_divergence_pct, 0) DESC

