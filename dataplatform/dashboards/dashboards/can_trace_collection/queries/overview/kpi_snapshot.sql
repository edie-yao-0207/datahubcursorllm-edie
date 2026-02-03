-- =============================================================================
-- Dashboard: CAN Trace Collection Dashboard
-- Tab: Overview
-- View: KPI Snapshot
-- =============================================================================
-- Visualization: Counter
-- Title: Overview KPIs
-- Description: High-level KPIs for CAN trace collection pipeline (all-time and current fiscal quarter)
-- Note: Uses definitions.445_calendar table for fiscal quarter definitions
-- =============================================================================

WITH
-- Get current fiscal quarter dates from definition table
current_quarter_info AS (
    SELECT
        fiscal_year,
        quarter,
        eoq AS quarter_end
    FROM definitions.445_calendar
    WHERE date = CURRENT_DATE()
),
-- Calculate quarter start by finding MIN(date) for the current fiscal year/quarter
quarter_start_date AS (
    SELECT
        MIN(date) AS quarter_start
    FROM definitions.445_calendar cal
    CROSS JOIN current_quarter_info cqi
    WHERE cal.fiscal_year = cqi.fiscal_year
      AND cal.quarter = cqi.quarter
),
-- Combine quarter info
fiscal_quarter AS (
    SELECT
        CAST(CONCAT('20', SUBSTRING(cqi.fiscal_year, 3)) AS INT) AS fiscal_year,  -- Extract year from 'FY24' format -> 2024
        CAST(SUBSTRING(cqi.quarter, 2) AS INT) AS fiscal_quarter_num,  -- Extract quarter number from 'Q4' format
        qsd.quarter_start,
        cqi.quarter_end
    FROM current_quarter_info cqi
    CROSS JOIN quarter_start_date qsd
),

-- All-time: Count candidates from all candidate tables
total_candidates AS (
    SELECT COUNT(*) AS cnt
    FROM datamodel_dev.fct_can_traces_required
),

-- Quarter: Count candidates in current fiscal quarter
total_candidates_quarter AS (
    SELECT COUNT(*) AS cnt
    FROM datamodel_dev.fct_can_traces_required ftr
    CROSS JOIN fiscal_quarter fq
    WHERE ftr.date >= fq.quarter_start
      AND ftr.date <= fq.quarter_end
),

-- All-time: Total collected traces from fct_can_trace_status (authoritative source)
-- Count ALL traces across all date partitions (table is partitioned by trace start_time)
total_collected AS (
    SELECT COUNT(*) AS cnt
    FROM product_analytics_staging.fct_can_trace_status
),

-- Quarter: Total collected traces in current fiscal quarter
total_collected_quarter AS (
    SELECT COUNT(*) AS cnt
    FROM product_analytics_staging.fct_can_trace_status cts
    CROSS JOIN fiscal_quarter fq
    WHERE cts.date >= fq.quarter_start
      AND cts.date <= fq.quarter_end
),

-- All-time: Active MMYEFs with candidates
active_mmyefs AS (
    SELECT COUNT(DISTINCT dvp.mmyef_id) AS cnt
    FROM datamodel_dev.fct_can_traces_required ftr
    JOIN product_analytics_staging.dim_device_vehicle_properties dvp
        ON ftr.date = dvp.date
        AND ftr.org_id = dvp.org_id
        AND ftr.device_id = dvp.device_id
    WHERE dvp.mmyef_id IS NOT NULL
),

-- Quarter: Active MMYEFs with candidates in current fiscal quarter
active_mmyefs_quarter AS (
    SELECT COUNT(DISTINCT dvp.mmyef_id) AS cnt
    FROM datamodel_dev.fct_can_traces_required ftr
    JOIN product_analytics_staging.dim_device_vehicle_properties dvp
        ON ftr.date = dvp.date
        AND ftr.org_id = dvp.org_id
        AND ftr.device_id = dvp.device_id
    CROSS JOIN fiscal_quarter fq
    WHERE ftr.date >= fq.quarter_start
      AND ftr.date <= fq.quarter_end
      AND dvp.mmyef_id IS NOT NULL
),

-- All-time: MMYEFs with collected traces (from fct_can_trace_status)
-- Count distinct MMYEFs across ALL traces
mmyefs_with_traces AS (
    SELECT COUNT(DISTINCT dvp.mmyef_id) AS cnt
    FROM product_analytics_staging.fct_can_trace_status cts
    JOIN product_analytics_staging.dim_device_vehicle_properties dvp
        ON cts.date = dvp.date
        AND cts.org_id = dvp.org_id
        AND cts.device_id = dvp.device_id
    WHERE dvp.mmyef_id IS NOT NULL
),

-- Quarter: MMYEFs with collected traces in current fiscal quarter
mmyefs_with_traces_quarter AS (
    SELECT COUNT(DISTINCT dvp.mmyef_id) AS cnt
    FROM product_analytics_staging.fct_can_trace_status cts
    JOIN product_analytics_staging.dim_device_vehicle_properties dvp
        ON cts.date = dvp.date
        AND cts.org_id = dvp.org_id
        AND cts.device_id = dvp.device_id
    CROSS JOIN fiscal_quarter fq
    WHERE cts.date >= fq.quarter_start
      AND cts.date <= fq.quarter_end
      AND dvp.mmyef_id IS NOT NULL
),

-- All-time: Unique devices (from fct_can_trace_status)
unique_devices AS (
    SELECT COUNT(DISTINCT org_id, device_id) AS cnt
    FROM product_analytics_staging.fct_can_trace_status
),

-- Quarter: Unique devices in current fiscal quarter
unique_devices_quarter AS (
    SELECT COUNT(DISTINCT org_id, device_id) AS cnt
    FROM product_analytics_staging.fct_can_trace_status cts
    CROSS JOIN fiscal_quarter fq
    WHERE cts.date >= fq.quarter_start
      AND cts.date <= fq.quarter_end
),

-- Total MMYEF population for diversity calculation (use latest date from dim table)
total_mmyef_population AS (
    SELECT COUNT(DISTINCT mmyef_id) AS cnt
    FROM product_analytics_staging.dim_device_vehicle_properties
    WHERE date = (SELECT MAX(date) FROM product_analytics_staging.dim_device_vehicle_properties)
      AND mmyef_id IS NOT NULL
)

SELECT
    -- All-time metrics
    COALESCE(tc.cnt, 0) AS total_candidates,
    COALESCE(tcol.cnt, 0) AS total_collected,
    COALESCE(am.cnt, 0) AS active_mmyefs,
    COALESCE(mwt.cnt, 0) AS mmyefs_with_traces,
    COALESCE(ud.cnt, 0) AS unique_devices,
    CASE 
        WHEN tmp.cnt > 0 THEN 
            CAST(mwt.cnt AS DOUBLE) / CAST(tmp.cnt AS DOUBLE)
        ELSE 0.0
    END AS quota_fulfillment_rate,
    
    -- Quarter metrics
    COALESCE(tcq.cnt, 0) AS total_candidates_quarter,
    COALESCE(tcolq.cnt, 0) AS total_collected_quarter,
    COALESCE(amq.cnt, 0) AS active_mmyefs_quarter,
    COALESCE(mwtq.cnt, 0) AS mmyefs_with_traces_quarter,
    COALESCE(udq.cnt, 0) AS unique_devices_quarter,
    CASE 
        WHEN tmp.cnt > 0 THEN 
            CAST(mwtq.cnt AS DOUBLE) / CAST(tmp.cnt AS DOUBLE)
        ELSE 0.0
    END AS quota_fulfillment_rate_quarter,
    
    -- Quarter info for display
    fq.fiscal_year,
    fq.fiscal_quarter_num,
    CONCAT('Q', CAST(fq.fiscal_quarter_num AS STRING), ' ', CAST(fq.fiscal_year AS STRING)) AS quarter_label
FROM fiscal_quarter fq
CROSS JOIN total_candidates tc
CROSS JOIN total_collected tcol
CROSS JOIN active_mmyefs am
CROSS JOIN mmyefs_with_traces mwt
CROSS JOIN unique_devices ud
CROSS JOIN total_mmyef_population tmp
CROSS JOIN total_candidates_quarter tcq
CROSS JOIN total_collected_quarter tcolq
CROSS JOIN active_mmyefs_quarter amq
CROSS JOIN mmyefs_with_traces_quarter mwtq
CROSS JOIN unique_devices_quarter udq
