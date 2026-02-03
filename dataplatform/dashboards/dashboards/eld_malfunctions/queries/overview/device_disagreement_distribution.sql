-- =============================================================================
-- Dashboard: ELD Malfunctions Dashboard
-- Tab: Overview
-- View: Device Disagreement Distribution
-- =============================================================================
-- Visualization: Bar Chart
-- Description: Distribution of devices by percentage of their signals that have disagreement
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

-- Device-level aggregation: count signals per device and how many have disagreement
device_signal_counts AS (
    SELECT
        device_id,
        COUNT(DISTINCT obd_value) AS total_signals,
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

-- Calculate percentage per device and bucket into ranges
device_buckets AS (
    SELECT
        device_id,
        total_signals,
        disagreement_signals,
        CAST(disagreement_signals AS DOUBLE) / NULLIF(total_signals, 0) * 100.0 AS pct_signals_disagreement,
        CASE
            WHEN total_signals = 0 THEN '0% (No Signals)'
            WHEN disagreement_signals = 0 THEN '0%'
            WHEN CAST(disagreement_signals AS DOUBLE) / NULLIF(total_signals, 0) * 100.0 <= 25.0 THEN '1-25%'
            WHEN CAST(disagreement_signals AS DOUBLE) / NULLIF(total_signals, 0) * 100.0 <= 50.0 THEN '25-50%'
            WHEN CAST(disagreement_signals AS DOUBLE) / NULLIF(total_signals, 0) * 100.0 <= 75.0 THEN '50-75%'
            WHEN CAST(disagreement_signals AS DOUBLE) / NULLIF(total_signals, 0) * 100.0 < 100.0 THEN '75-100%'
            ELSE '100%'
        END AS disagreement_bucket
    FROM device_signal_counts
)

-- Count devices in each bucket
SELECT
    disagreement_bucket,
    COUNT(DISTINCT device_id) AS device_count
FROM device_buckets
GROUP BY disagreement_bucket
ORDER BY 
    CASE disagreement_bucket
        WHEN '0% (No Signals)' THEN 0
        WHEN '0%' THEN 1
        WHEN '1-25%' THEN 2
        WHEN '25-50%' THEN 3
        WHEN '50-75%' THEN 4
        WHEN '75-100%' THEN 5
        WHEN '100%' THEN 6
    END

