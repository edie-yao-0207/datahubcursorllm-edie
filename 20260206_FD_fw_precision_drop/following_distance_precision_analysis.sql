-- Analysis 1: Daily Following Distance firmware precision (Dec 2025 vs Jan 2026)
WITH tailgating_events AS (
  SELECT
    org_id,
    device_id,
    date,
    event_id,
    cm_build,
    model_registry_key,
    cm_product_name,
    ai_release_stage,
    is_inbox_event,
    is_tp,
    dataset_name,
    batch_name,
    sample_class AS firmware_or_inbox_sample
  FROM dojo.metrics_std_tailgating
  WHERE date BETWEEN CAST('2025-12-01' AS DATE) AND CAST('2026-01-31' AS DATE)
    AND dev_cfg_mlapp_fd_unsafe_following_distance_seconds >= 0.1
),
enriched AS (
  SELECT
    r.org_id,
    r.device_id,
    r.date,
    r.event_id,
    r.is_tp,
    r.firmware_or_inbox_sample,
    COALESCE(dafs.model_registry_key, 'null') AS model_registry_key,
    COALESCE(dd_cm.product_name, 'null') AS product_name,
    COALESCE(dafs.ai_release_stage, 'GA') AS ai_release_stage,
    COALESCE(do.account_arr_segment, 'Null') AS account_arr_segment,
    COALESCE(do.account_size_segment_name, 'Null') AS account_size_segment_name,
    CASE
      WHEN do.account_industry IS NULL THEN 'Null'
      WHEN do.account_industry IN ('Mining, Quarrying, Oil & Gas', 'Mining, Quarrying, Oil and Gas') THEN 'Mining, Quarrying, Oil & Gas'
      WHEN do.account_industry IN ('Government', 'Goverment') THEN 'Government'
      WHEN do.account_industry IN ('Retail Trade', 'Retail') THEN 'Retail Trade'
      WHEN do.account_industry IN ('Transportation & Warehousing', 'Passenger Transit') THEN 'Transportation & Warehousing'
      ELSE do.account_industry
    END AS account_industry
  FROM tailgating_events r
  LEFT JOIN (
    SELECT org_id, vg_device_id, date, model_registry_key, ai_release_stage
    FROM dojo.device_ai_features_daily_snapshot
    WHERE date BETWEEN CAST('2025-12-01' AS DATE) AND CAST('2026-01-31' AS DATE)
      AND feature_enabled = 'haTailgating'
  ) dafs ON r.org_id = dafs.org_id AND r.device_id = dafs.vg_device_id AND r.date = dafs.date
  LEFT JOIN (
    SELECT associated_devices.vg_device_id AS vg_device_id, org_id, date, product_name
    FROM datamodel_core.dim_devices
    WHERE date BETWEEN CAST('2025-12-01' AS DATE) AND CAST('2026-01-31' AS DATE)
  ) dd_cm ON r.device_id = dd_cm.vg_device_id AND r.org_id = dd_cm.org_id AND r.date = dd_cm.date
  LEFT JOIN (
    SELECT org_id, date, account_arr_segment, account_size_segment_name, account_industry
    FROM datamodel_core.dim_organizations
    WHERE date BETWEEN CAST('2025-12-01' AS DATE) AND CAST('2026-01-31' AS DATE)
  ) do ON r.org_id = do.org_id AND r.date = do.date
  WHERE r.dataset_name = 'na_std_precision'
    AND (dafs.ai_release_stage IS NULL OR dafs.ai_release_stage <> 'SHADOW')
    AND NOT r.is_tp IS NULL
    AND dd_cm.product_name IN ('CM31', 'CM32', 'CM33', 'CM34')
    AND r.firmware_or_inbox_sample = 'firmware_sample'
)
SELECT
  date,
  COUNT(*) AS fw_annotation_count,
  SUM(CASE WHEN is_tp = 1 THEN 1 ELSE 0 END) AS fw_tp_count,
  ROUND(SUM(CASE WHEN is_tp = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) AS firmware_precision
FROM enriched
GROUP BY date
ORDER BY date;

-- Analysis 10: Config threshold (dev_cfg_mlapp_fd_unsafe_following_distance_seconds) distribution & precision
WITH all_precision_results AS (
  SELECT
    date,
    dev_cfg_mlapp_fd_unsafe_following_distance_seconds AS cfg,
    is_tp,
    sample_class
  FROM dojo.metrics_std_tailgating
  WHERE date BETWEEN CAST('2025-12-01' AS DATE) AND CAST('2026-01-31' AS DATE)
    AND ai_release_stage = 'GA'
    AND is_tp IS NOT NULL
    AND dev_cfg_mlapp_fd_unsafe_following_distance_seconds >= 0.1
    AND dataset_name = 'na_std_precision'
    AND is_inbox_event IS NOT NULL
    AND ai_release_stage <> 'SHADOW'
    AND sample_class = 'firmware_sample'
),
agg AS (
  SELECT
    DATE_FORMAT(DATE_TRUNC('month', date), 'yyyy-MM') AS month,
    CASE
      WHEN cfg <= 0.5 THEN 'a_le_0.5'
      WHEN cfg <= 0.6 THEN 'b_0.5-0.6'
      WHEN cfg <= 0.7 THEN 'c_0.6-0.7'
      WHEN cfg <= 0.8 THEN 'd_0.7-0.8'
      WHEN cfg <= 0.9 THEN 'e_0.8-0.9'
      WHEN cfg <= 1.0 THEN 'f_0.9-1.0'
      WHEN cfg <= 1.2 THEN 'g_1.0-1.2'
      ELSE 'h_gt_1.2'
    END AS cfg_bucket,
    COUNT(*) AS n,
    SUM(CASE WHEN is_tp = 1 THEN 1 ELSE 0 END) AS tp
  FROM all_precision_results
  GROUP BY 1, 2
),
totals AS (
  SELECT month, SUM(n) AS total FROM agg GROUP BY 1
)
SELECT
  a.month,
  a.cfg_bucket,
  a.n,
  ROUND(a.n * 100.0 / t.total, 2) AS pct,
  a.tp,
  ROUND(a.tp * 1.0 / a.n, 4) AS precision
FROM agg a
JOIN totals t ON a.month = t.month
ORDER BY 1, 2;
