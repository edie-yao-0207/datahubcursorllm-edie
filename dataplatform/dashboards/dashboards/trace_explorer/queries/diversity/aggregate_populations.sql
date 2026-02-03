-- =============================================================================
-- Dashboard: VDP Trace Explorer
-- Tab: Trace Diversity
-- View: Aggregate Populations
-- =============================================================================
-- Visualization: Multiple line charts
-- Description: Combined cumulative coverage by Device and by MMYEF, per date
-- =============================================================================

WITH date_spine AS (
  SELECT DISTINCT date
  FROM product_analytics_staging.dim_device_vehicle_properties
  WHERE date BETWEEN :date.min AND :date.max
    AND NOT (make IS NULL OR model IS NULL OR year IS NULL)
),

/* =========================
   DEVICE-SCOPE METRICS
   ========================= */
device_population AS (
  SELECT COUNT(DISTINCT device_id) AS total_devices
  FROM product_analytics_staging.dim_device_vehicle_properties
  WHERE date BETWEEN :date.min AND :date.max
    AND NOT (make IS NULL OR model IS NULL OR year IS NULL)
),

device_traces AS (
  SELECT
    date,
    org_id,
    device_id,
    COUNT(DISTINCT trace_uuid) AS trace_cnt
  FROM product_analytics_staging.dim_trace_characteristics
  WHERE date BETWEEN :date.min AND :date.max
  GROUP BY date, org_id, device_id
),

first_trace_per_device AS (
  SELECT
    org_id,
    device_id,
    MIN(date) AS first_trace_date
  FROM device_traces
  GROUP BY org_id, device_id
),

device_daily_totals AS (
  SELECT
    date,
    SUM(trace_cnt) AS daily_traces_device
  FROM device_traces
  GROUP BY date
),

device_series AS (
  SELECT
    ds.date,
    COALESCE(ddt.daily_traces_device, 0) AS daily_traces_device,
    SUM(COALESCE(ddt.daily_traces_device, 0)) OVER (
      ORDER BY ds.date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_traces_device,
    COUNT(dft.device_id) AS cum_unique_devices_with_traces
  FROM date_spine ds
  LEFT JOIN device_daily_totals ddt USING (date)
  LEFT JOIN first_trace_per_device dft
    ON dft.first_trace_date <= ds.date
  GROUP BY ds.date, ddt.daily_traces_device
),

/* =========================
   MMYEF-SCOPE METRICS
   ========================= */

-- First date each MMYEF ever appears (no window restriction for true first-seen)
mmyef_first_seen AS (
  SELECT
    mmyef_id,
    MIN(date) AS first_seen_date
  FROM product_analytics_staging.dim_device_vehicle_properties
  WHERE NOT (make IS NULL OR model IS NULL OR year IS NULL)
  GROUP BY mmyef_id
),

-- New MMYEFs that first appear on each date in the reporting window
mmyef_new_by_day AS (
  SELECT
    ds.date,
    COUNT(*) AS new_mmyefs_today
  FROM date_spine ds
  JOIN mmyef_first_seen fs
    ON fs.first_seen_date = ds.date
  GROUP BY ds.date
),

-- Baseline MMYEFs that existed before the window starts
mmyef_baseline AS (
  SELECT COUNT(DISTINCT mmyef_id) AS baseline_mmyefs
  FROM product_analytics_staging.dim_device_vehicle_properties
  WHERE date < :date.min
    AND NOT (make IS NULL OR model IS NULL OR year IS NULL)
),

-- Cumulative MMYEF universe size by day (baseline + first-seen within window)
mmyef_population_series AS (
  SELECT
    ds.date,
    COALESCE(nbd.new_mmyefs_today, 0) AS new_mmyefs_today,
    (SELECT baseline_mmyefs FROM mmyef_baseline)
      + SUM(COALESCE(nbd.new_mmyefs_today, 0)) OVER (
          ORDER BY ds.date
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cum_mmyef_population
  FROM date_spine ds
  LEFT JOIN mmyef_new_by_day nbd USING (date)
),

-- Trace activity by MMYEF within the window
mmyef_traces AS (
  SELECT
    t.date,
    d.mmyef_id,
    COUNT(DISTINCT t.trace_uuid) AS count_traces
  FROM product_analytics_staging.dim_trace_characteristics t
  JOIN product_analytics_staging.dim_device_vehicle_properties d
    USING (date, org_id, device_id)
  WHERE t.date BETWEEN :date.min AND :date.max
    AND NOT (d.make IS NULL OR d.model IS NULL OR d.year IS NULL)
  GROUP BY t.date, d.mmyef_id
),

first_trace_per_mmyef AS (
  SELECT
    mmyef_id,
    MIN(date) AS first_trace_date
  FROM mmyef_traces
  GROUP BY mmyef_id
),

mmyef_daily_totals AS (
  SELECT
    date,
    SUM(count_traces) AS daily_traces_mmyef
  FROM mmyef_traces
  GROUP BY date
),

mmyef_series AS (
  SELECT
    ds.date,
    COALESCE(mdt.daily_traces_mmyef, 0) AS daily_traces_mmyef,
    SUM(COALESCE(mdt.daily_traces_mmyef, 0)) OVER (
      ORDER BY ds.date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_traces_mmyef,
    COUNT(ftm.mmyef_id) AS cum_mmyefs_with_traces
  FROM date_spine ds
  LEFT JOIN mmyef_daily_totals mdt USING (date)
  LEFT JOIN first_trace_per_mmyef ftm
    ON ftm.first_trace_date <= ds.date
  GROUP BY ds.date, mdt.daily_traces_mmyef
)

SELECT
  ds.date,

  -- Device metrics
  ds.daily_traces_device,
  ds.cum_traces_device,
  ds.cum_unique_devices_with_traces,
  dp.total_devices,
  CAST(ds.cum_unique_devices_with_traces AS DOUBLE) / NULLIF(dp.total_devices, 0) AS cum_device_coverage_pct,

  -- MMYEF metrics
  ms.daily_traces_mmyef,
  ms.cum_traces_mmyef,
  ms.cum_mmyefs_with_traces,
  mps.new_mmyefs_today,
  mps.cum_mmyef_population AS total_mmyef_population,
  CAST(ms.cum_mmyefs_with_traces AS DOUBLE) / NULLIF(mps.cum_mmyef_population, 0) AS cum_mmyef_coverage_pct

FROM device_series ds
JOIN mmyef_series ms USING (date)
JOIN mmyef_population_series mps USING (date)
CROSS JOIN device_population dp
ORDER BY ds.date


