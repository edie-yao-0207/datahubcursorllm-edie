-- =============================================================================
-- Dashboard: ELD Malfunctions Dashboard
-- Tab: Comparison
-- View: Malfunctions Pivoted by MMY + Cable + Product
-- =============================================================================
-- Visualization: Table
-- Title: "Malfunction Rates by MMY + Cable + Product"
-- Description: Pivoted table showing count of each malfunction type per MMY + cable + product combination
-- =============================================================================

WITH
-- Explode ELD events to get individual records
eld_events_exploded AS (
    SELECT
        date,
        org_id,
        object_id AS device_id,
        record.diagnostic_or_malfunction.diagnostic_or_malfunction_code AS diagnostic_or_malfunction_code,
        record.diagnostic_or_malfunction.event_code AS malfunction_event_code
    FROM kinesisstats_history.osDEldEvent
    LATERAL VIEW explode(value.proto_value.eld_events) exploded AS eld_event
    LATERAL VIEW explode(eld_event.records) exploded_records AS record
    WHERE date BETWEEN :date.min AND :date.max
      AND record.event_type = 7  -- ET_DIAGNOSTIC_OR_MALFUNCTION
      AND record.diagnostic_or_malfunction.diagnostic_or_malfunction_code IS NOT NULL
      AND record.diagnostic_or_malfunction.event_code = 1  -- EC_MALFUNCTION_CREATED
),

-- Join with vehicle properties to get MMYEF
malfunctions_with_mmyef AS (
    SELECT
        ese.date,
        ese.org_id,
        ese.device_id,
        ese.diagnostic_or_malfunction_code,
        dvp.make,
        dvp.model,
        dvp.year,
        dvp.mmyef_id
    FROM eld_events_exploded ese
    LEFT JOIN product_analytics_staging.dim_device_vehicle_properties dvp
        ON ese.org_id = dvp.org_id
        AND ese.device_id = dvp.device_id
        AND dvp.date = (SELECT MAX(date) FROM product_analytics_staging.dim_device_vehicle_properties)
    WHERE dvp.make IS NOT NULL
      AND dvp.model IS NOT NULL
      AND dvp.year IS NOT NULL
),

-- Join with device dimensions to get cable and product info
malfunctions_with_all_dimensions AS (
    SELECT
        mwm.make,
        mwm.model,
        mwm.year,
        mwm.diagnostic_or_malfunction_code,
        mwm.org_id,
        mwm.device_id,
        ddd.cable_id,
        ddd.cable_name,
        ddd.product_id,
        ddd.product_name
    FROM malfunctions_with_mmyef mwm
    LEFT JOIN product_analytics.dim_device_dimensions ddd
        ON mwm.org_id = ddd.org_id
        AND mwm.device_id = ddd.device_id
        AND ddd.date = (SELECT MAX(date) FROM product_analytics.dim_device_dimensions)
)

-- Pivot malfunction types into columns directly
SELECT
    mwad.make,
    mwad.model,
    mwad.year,
    COALESCE(CAST(mwad.cable_id AS STRING), 'Unknown') AS cable_id,
    COALESCE(mwad.cable_name, 'Unknown') AS cable_name,
    COALESCE(CAST(mwad.product_id AS STRING), 'Unknown') AS product_id,
    COALESCE(mwad.product_name, 'Unknown') AS product_name,
    SUM(CASE WHEN mwad.diagnostic_or_malfunction_code = 'P' THEN 1 ELSE 0 END) AS power_malfunction_count,
    SUM(CASE WHEN mwad.diagnostic_or_malfunction_code = 'E' THEN 1 ELSE 0 END) AS engine_sync_malfunction_count,
    SUM(CASE WHEN mwad.diagnostic_or_malfunction_code = 'T' THEN 1 ELSE 0 END) AS timing_malfunction_count,
    SUM(CASE WHEN mwad.diagnostic_or_malfunction_code = 'L' THEN 1 ELSE 0 END) AS positioning_malfunction_count,
    SUM(CASE WHEN mwad.diagnostic_or_malfunction_code = 'R' THEN 1 ELSE 0 END) AS data_recording_malfunction_count,
    SUM(CASE WHEN mwad.diagnostic_or_malfunction_code = 'S' THEN 1 ELSE 0 END) AS data_transfer_malfunction_count,
    SUM(CASE WHEN mwad.diagnostic_or_malfunction_code = 'O' THEN 1 ELSE 0 END) AS other_eld_malfunction_count,
    SUM(CASE WHEN mwad.diagnostic_or_malfunction_code = '1' THEN 1 ELSE 0 END) AS power_diagnostic_count,
    SUM(CASE WHEN mwad.diagnostic_or_malfunction_code = '2' THEN 1 ELSE 0 END) AS engine_sync_diagnostic_count,
    SUM(CASE WHEN mwad.diagnostic_or_malfunction_code = '3' THEN 1 ELSE 0 END) AS missing_required_data_diagnostic_count,
    SUM(CASE WHEN mwad.diagnostic_or_malfunction_code = '4' THEN 1 ELSE 0 END) AS data_transfer_diagnostic_count,
    SUM(CASE WHEN mwad.diagnostic_or_malfunction_code = '5' THEN 1 ELSE 0 END) AS unidentified_driving_diagnostic_count,
    SUM(CASE WHEN mwad.diagnostic_or_malfunction_code = '6' THEN 1 ELSE 0 END) AS other_eld_diagnostic_count,
    COUNT(*) AS total_malfunction_count,
    COUNT(DISTINCT mwad.device_id) AS affected_device_count
FROM malfunctions_with_all_dimensions mwad
GROUP BY mwad.make, mwad.model, mwad.year, mwad.cable_id, mwad.cable_name, mwad.product_id, mwad.product_name
ORDER BY total_malfunction_count DESC

