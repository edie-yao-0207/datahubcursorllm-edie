-- =============================================================================
-- Dashboard: ELD Malfunctions Dashboard
-- Tab: Overview
-- View: Device Multi-Source Prevalence
-- =============================================================================
-- Visualization: Counter
-- Description: High-level device impact metrics showing percentage of devices with multi-source signals and average percentage of signals per device
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
        comparison_data.mean_value
    FROM datamodel_dev.agg_signal_source_comparison_daily sc
    LATERAL VIEW explode(obd_value_to_comparison_map) AS obd_value_key, comparison_data
    WHERE sc.date BETWEEN :date.min AND :date.max
        AND comparison_data IS NOT NULL
),

-- Device-level aggregation: count signals per device and how many have multi-source/disagreement
device_signal_counts AS (
    SELECT
        device_id,
        COUNT(DISTINCT obd_value) AS total_signals,
        COUNT(DISTINCT CASE WHEN source_count > 1 THEN obd_value END) AS multi_source_signals,
        COUNT(DISTINCT CASE 
            WHEN source_count > 1 AND (
                cv > 0.01 OR 
                agreement_score < 95.0 OR
                (max_divergence_pct > 10.0 AND ABS(mean_value) > 0.001)
            ) THEN obd_value 
        END) AS disagreement_signals
    FROM source_comparison_exploded
    GROUP BY device_id
),

-- Calculate percentages per device
device_percentages AS (
    SELECT
        device_id,
        total_signals,
        multi_source_signals,
        disagreement_signals,
        CAST(multi_source_signals AS DOUBLE) / NULLIF(total_signals, 0) * 100.0 AS pct_signals_multi_source,
        CAST(disagreement_signals AS DOUBLE) / NULLIF(total_signals, 0) * 100.0 AS pct_signals_disagreement,
        CASE WHEN multi_source_signals > 0 THEN 1 ELSE 0 END AS has_multi_source
    FROM device_signal_counts
),

-- Overall statistics
overall_stats AS (
    SELECT
        COUNT(DISTINCT device_id) AS total_devices,
        SUM(has_multi_source) AS devices_with_multi_source,
        CAST(SUM(has_multi_source) AS DOUBLE) / CAST(COUNT(DISTINCT device_id) AS DOUBLE) * 100.0 AS pct_devices_with_multi_source,
        AVG(pct_signals_multi_source) AS avg_pct_signals_multi_source_per_device,
        AVG(pct_signals_disagreement) AS avg_pct_signals_disagreement_per_device
    FROM device_percentages
)

SELECT
    total_devices,
    devices_with_multi_source,
    pct_devices_with_multi_source,
    COALESCE(avg_pct_signals_multi_source_per_device, 0) AS avg_pct_signals_multi_source_per_device,
    COALESCE(avg_pct_signals_disagreement_per_device, 0) AS avg_pct_signals_disagreement_per_device
FROM overall_stats

