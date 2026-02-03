-- =============================================================================
-- Dashboard: ELD Malfunctions Dashboard
-- Tab: Overview
-- View: Overall Disagreement Statistics
-- =============================================================================
-- Visualization: Counter
-- Description: High-level summary statistics showing overall disagreement metrics across all signals and devices
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

-- Device-level disagreement flags
device_disagreement AS (
    SELECT
        device_id,
        obd_value,
        source_count,
        agreement_score,
        CASE 
            WHEN source_count > 1 AND (
                cv > 0.01 OR 
                agreement_score < 95.0 OR
                (max_divergence_pct > 10.0 AND ABS(mean_value) > 0.001)
            ) THEN 1 
            ELSE 0 
        END AS has_disagreement
    FROM source_comparison_exploded
),

-- Signal-level disagreement flags
signal_disagreement AS (
    SELECT
        obd_value,
        COUNT(DISTINCT CASE WHEN source_count > 1 THEN device_id END) AS multi_source_device_count,
        COUNT(DISTINCT CASE WHEN has_disagreement = 1 THEN device_id END) AS devices_with_disagreement
    FROM device_disagreement
    GROUP BY obd_value
),

-- Overall statistics
device_level_stats AS (
    SELECT
        COUNT(DISTINCT CASE WHEN source_count > 1 THEN device_id END) AS total_devices_with_multiple_sources,
        COUNT(DISTINCT CASE WHEN has_disagreement = 1 THEN device_id END) AS devices_with_disagreement,
        AVG(CASE WHEN source_count > 1 THEN agreement_score ELSE NULL END) AS avg_agreement_score_all_signals
    FROM device_disagreement
),

signal_level_stats AS (
    SELECT
        COUNT(DISTINCT CASE WHEN multi_source_device_count > 0 THEN obd_value END) AS total_signals_with_multiple_sources,
        COUNT(DISTINCT CASE WHEN devices_with_disagreement > 0 THEN obd_value END) AS signals_with_disagreement
    FROM signal_disagreement
),

overall_stats AS (
    SELECT
        dls.total_devices_with_multiple_sources,
        dls.devices_with_disagreement,
        dls.avg_agreement_score_all_signals,
        sls.total_signals_with_multiple_sources,
        sls.signals_with_disagreement
    FROM device_level_stats dls
    CROSS JOIN signal_level_stats sls
)

SELECT
    total_devices_with_multiple_sources,
    devices_with_disagreement,
    CAST(devices_with_disagreement AS DOUBLE) / NULLIF(total_devices_with_multiple_sources, 0) * 100.0 AS pct_devices_with_disagreement,
    total_signals_with_multiple_sources,
    signals_with_disagreement,
    CAST(signals_with_disagreement AS DOUBLE) / NULLIF(total_signals_with_multiple_sources, 0) * 100.0 AS pct_signals_with_disagreement,
    COALESCE(avg_agreement_score_all_signals, 0) AS avg_agreement_score_all_signals
FROM overall_stats

