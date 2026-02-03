-- =============================================================================
-- Dashboard: ELD Malfunctions Dashboard
-- Tab: By Dimension
-- View: Malfunctions by Product
-- =============================================================================
-- Visualization: Bar
-- Title: "Malfunctions by Product"
-- Description: Breakdown of malfunctions by product with product name translation
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
        dvp.year
    FROM eld_events_exploded ese
    LEFT JOIN product_analytics_staging.dim_device_vehicle_properties dvp
        ON ese.org_id = dvp.org_id
        AND ese.device_id = dvp.device_id
        AND dvp.date = (SELECT MAX(date) FROM product_analytics_staging.dim_device_vehicle_properties)
),

-- Join with device dimensions to get product_id and product_name
-- Use latest available date from dim_device_dimensions (may be before date range end)
malfunctions_with_product AS (
    SELECT
        mwm.date,
        mwm.org_id,
        mwm.device_id,
        mwm.diagnostic_or_malfunction_code,
        mwm.make,
        mwm.model,
        mwm.year,
        ddd.product_id,
        ddd.product_name
    FROM malfunctions_with_mmyef mwm
    LEFT JOIN product_analytics.dim_device_dimensions ddd
        ON mwm.org_id = ddd.org_id
        AND mwm.device_id = ddd.device_id
        AND ddd.date = (SELECT MAX(date) FROM product_analytics.dim_device_dimensions)
)

SELECT
    COALESCE(CAST(mwp.product_id AS STRING), 'Unknown') AS product_id,
    COALESCE(mwp.product_name, 'Unknown') AS product_name,
    COUNT(*) AS malfunction_count,
    COUNT(DISTINCT mwp.device_id) AS affected_device_count
FROM malfunctions_with_product mwp
GROUP BY mwp.product_id, mwp.product_name
ORDER BY malfunction_count DESC

