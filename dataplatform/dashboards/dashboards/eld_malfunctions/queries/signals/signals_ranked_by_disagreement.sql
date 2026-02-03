-- =============================================================================
-- Dashboard: ELD Malfunctions Dashboard
-- Tab: Overview
-- View: Signals Ranked by Disagreement
-- =============================================================================
-- Visualization: Bar Chart
-- Title: "Signals Ranked by Disagreement"
-- Description: Top signals ranked by disagreement level (coefficient of variation). Shows which signals have the most variation between sources. Only includes signals with multiple sources.
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
        comparison_data.agreement_score
    FROM datamodel_dev.agg_signal_source_comparison_daily sc
    LATERAL VIEW explode(obd_value_to_comparison_map) AS obd_value_key, comparison_data
    WHERE sc.date BETWEEN :date.min AND :date.max
        AND comparison_data IS NOT NULL
        AND comparison_data.source_count > 1
),

-- Aggregate per signal across all devices
signal_aggregation AS (
    SELECT
        obd_value,
        AVG(cv) AS avg_cv,
        AVG(agreement_score) AS avg_agreement_score,
        CAST(COUNT(DISTINCT CASE WHEN source_count > 1 THEN device_id END) AS DOUBLE) / CAST(COUNT(DISTINCT device_id) AS DOUBLE) * 100.0 AS multi_source_pct
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
    avg_cv,
    avg_agreement_score,
    multi_source_pct
FROM signal_with_names
WHERE avg_cv IS NOT NULL
ORDER BY avg_cv DESC
LIMIT 30

