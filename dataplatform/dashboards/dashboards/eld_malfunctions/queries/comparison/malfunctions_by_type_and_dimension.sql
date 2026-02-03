-- =============================================================================
-- Dashboard: ELD Malfunctions Dashboard
-- Tab: Comparison
-- View: Malfunctions by Type and Dimension
-- =============================================================================
-- Visualization: Table
-- Title: "Malfunctions by Type, Cable, and Product"
-- Description: Detailed breakdown showing malfunction type across cable and product combinations
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
        ese.*,
        dvp.make,
        dvp.model,
        dvp.year
    FROM eld_events_exploded ese
    LEFT JOIN product_analytics_staging.dim_device_vehicle_properties dvp
        ON ese.org_id = dvp.org_id
        AND ese.device_id = dvp.device_id
        AND dvp.date = (SELECT MAX(date) FROM product_analytics_staging.dim_device_vehicle_properties)
),

-- Translate codes to human-readable names
malfunctions_with_labels AS (
    SELECT
        mwm.*,
        CASE mwm.diagnostic_or_malfunction_code
            WHEN 'P' THEN 'Power Malfunction'
            WHEN 'E' THEN 'Engine Sync Malfunction'
            WHEN 'T' THEN 'Timing Malfunction'
            WHEN 'L' THEN 'Positioning Malfunction'
            WHEN 'R' THEN 'Data Recording Malfunction'
            WHEN 'S' THEN 'Data Transfer Malfunction'
            WHEN 'O' THEN 'Other ELD Malfunction'
            WHEN '1' THEN 'Power Diagnostic'
            WHEN '2' THEN 'Engine Sync Diagnostic'
            WHEN '3' THEN 'Missing Required Data Diagnostic'
            WHEN '4' THEN 'Data Transfer Diagnostic'
            WHEN '5' THEN 'Unidentified Driving Diagnostic'
            WHEN '6' THEN 'Other ELD Diagnostic'
            ELSE CONCAT('Unknown (', mwm.diagnostic_or_malfunction_code, ')')
        END AS malfunction_type_label
    FROM malfunctions_with_mmyef mwm
),

-- Join with device dimensions to get cable and product info
-- Use latest available date from dim_device_dimensions (may be before date range end)
malfunctions_with_dimensions AS (
    SELECT
        mwl.*,
        ddd.cable_id,
        ddd.cable_name,
        ddd.product_id,
        ddd.product_name
    FROM malfunctions_with_labels mwl
    LEFT JOIN product_analytics.dim_device_dimensions ddd
        ON mwl.org_id = ddd.org_id
        AND mwl.device_id = ddd.device_id
        AND ddd.date = (SELECT MAX(date) FROM product_analytics.dim_device_dimensions)
)

SELECT
    malfunction_type_label AS malfunction_type,
    diagnostic_or_malfunction_code AS malfunction_code,
    make,
    model,
    year,
    COALESCE(CAST(cable_id AS STRING), 'Unknown') AS cable_id,
    COALESCE(cable_name, 'Unknown') AS cable_name,
    COALESCE(CAST(product_id AS STRING), 'Unknown') AS product_id,
    COALESCE(product_name, 'Unknown') AS product_name,
    COUNT(*) AS malfunction_count,
    COUNT(DISTINCT device_id) AS affected_device_count
FROM malfunctions_with_dimensions
GROUP BY malfunction_type_label, diagnostic_or_malfunction_code, make, model, year, cable_id, cable_name, product_id, product_name
ORDER BY malfunction_count DESC

