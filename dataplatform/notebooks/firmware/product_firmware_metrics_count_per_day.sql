-- Databricks notebook source
-- Using dynamic partition overwrite mode means we don't need to specify which partitions to overwrite when using INSERT OVERWRITE
-- More info: https://spark.apache.org/docs/latest/configuration.html (search spark.sql.sources.partitionOverwriteMode)
SET spark.sql.sources.partitionOverwriteMode = dynamic
;

-- Allow schema updates.  New columns must always be added at the end of the
-- table.
SET spark.databricks.delta.schema.autoMerge.enabled = TRUE;


-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW daily_active_devices_by_release_and_org_type AS (
  SELECT
    `date`,
    product_id,
    variant_id,
    latest_build_on_day AS fw,
    org_type LIKE "Internal" AS internal_org_if_true,
    COUNT(DISTINCT device_id) AS active_devices
  FROM
    data_analytics.vg3x_daily_summary
  WHERE `date` BETWEEN DATE_SUB(CURRENT_DATE(), ${lookback_days}) AND CURRENT_DATE()
  GROUP BY
    `date`,
    product_id,
    variant_id,
    fw,
    internal_org_if_true
)
;


-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW device_linked_to_release_and_org_type AS (
  SELECT
    `date`,
    product_id,
    variant_id,
    device_id,
    latest_build_on_day AS fw,
    org_type LIKE "Internal" AS internal_org_if_true
  FROM
    data_analytics.vg3x_daily_summary
  WHERE `date` BETWEEN DATE_SUB(CURRENT_DATE(), ${lookback_days}) AND CURRENT_DATE()
);


-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW device_metric_counts_with_rank AS (
  WITH firmware_metrics AS (
    SELECT
      fm.`date`,
      b.product_id,
      b.variant_id,
      b.internal_org_if_true,
      fm.build AS fw,
      fm.name AS metric_name,
      fm.device_id,
      -- given an array("foo:bar", "service:baz") return "baz":
      substring_index(
        get(filter(fm.tags, tag -> substring_index(tag, ":", 1) == "service"), 0),
        ":", -1) as service_name,
      -- given an array("foo:bar", "manager:baz") return "baz":
      substring_index(
        get(filter(fm.tags, tag -> substring_index(tag, ":", 1) == "manager"), 0),
        ":", -1) as manager_name,
      COALESCE(SUM(fm.value), 0) AS metric_count
    FROM
      product_analytics_staging.fct_osdfirmwaremetrics fm
    JOIN dataprep_firmware.data_ingestion_high_water_mark hwm
    INNER JOIN device_linked_to_release_and_org_type as b
      ON fm.`date` = b.date AND fm.device_id = b.device_id
    WHERE fm.`date` BETWEEN DATE_SUB(CURRENT_DATE(), ${lookback_days}) AND CURRENT_DATE()
      AND fm.metric_type = 1 -- 0: INVALID 1: COUNT 2: GAUGE
      AND fm.time <= hwm.time_ms
    GROUP BY
      fm.`date`,
      b.product_id,
      b.variant_id,
      b.internal_org_if_true,
      fm.build,
      fm.name,
      fm.device_id,
      service_name,
      manager_name
  )
  SELECT
    fm.`date`,
    fm.product_id,
    fm.variant_id,
    fm.internal_org_if_true,
    fm.fw,
    fm.metric_name,
    fm.device_id,
    fm.service_name,
    fm.manager_name,
    fm.metric_count,
    ROW_NUMBER() OVER (PARTITION BY fm.`date`, fm.product_id, fm.variant_id, fm.internal_org_if_true, fm.fw, fm.metric_name, fm.service_name, fm.manager_name ORDER BY metric_count DESC) AS row_rank
  FROM
    firmware_metrics fm
);


-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW final_with_links AS (
  WITH transposed_worst_10 AS (
    SELECT
      `date`,
      product_id,
      variant_id,
      internal_org_if_true,
      fw,
      service_name,
      manager_name,
      metric_name,
      COUNT(*) AS number_of_devices_reporting_this_metric,
      SUM(metric_count) AS total_metric_count,
      ROUND(SUM(metric_count) / COUNT(*), 3) AS average_metric_count_for_devices_reporting_anomaly,
      PERCENTILE_APPROX(metric_count, 0.25) AS 25th_percentile_for_devices_reporting_metric,
      PERCENTILE_APPROX(metric_count, 0.50) AS 50th_percentile_for_devices_reporting_metric,
      PERCENTILE_APPROX(metric_count, 0.75) AS 75th_percentile_for_devices_reporting_metric,
      MAX(IF(row_rank = 1, device_id, NULL)) AS worst_device,
      MAX(IF(row_rank = 2, device_id, NULL)) AS 2nd_worst_device,
      MAX(IF(row_rank = 3, device_id, NULL)) AS 3rd_worst_device,
      MAX(IF(row_rank = 4, device_id, NULL)) AS 4th_worst_device,
      MAX(IF(row_rank = 5, device_id, NULL)) AS 5th_worst_device,
      MAX(IF(row_rank = 6, device_id, NULL)) AS 6th_worst_device,
      MAX(IF(row_rank = 7, device_id, NULL)) AS 7th_worst_device,
      MAX(IF(row_rank = 8, device_id, NULL)) AS 8th_worst_device,
      MAX(IF(row_rank = 9, device_id, NULL)) AS 9th_worst_device,
      MAX(IF(row_rank = 10, device_id, NULL)) AS 10th_worst_device,
      MAX(IF(row_rank = 1, metric_count, NULL)) AS worst_device_metric_count,
      MAX(IF(row_rank = 2, metric_count, NULL)) AS 2nd_worst_device_metric_count,
      MAX(IF(row_rank = 3, metric_count, NULL)) AS 3rd_worst_device_metric_count,
      MAX(IF(row_rank = 4, metric_count, NULL)) AS 4th_worst_device_metric_count,
      MAX(IF(row_rank = 5, metric_count, NULL)) AS 5th_worst_device_metric_count,
      MAX(IF(row_rank = 6, metric_count, NULL)) AS 6th_worst_device_metric_count,
      MAX(IF(row_rank = 7, metric_count, NULL)) AS 7th_worst_device_metric_count,
      MAX(IF(row_rank = 8, metric_count, NULL)) AS 8th_worst_device_metric_count,
      MAX(IF(row_rank = 9, metric_count, NULL)) AS 9th_worst_device_metric_count,
      MAX(IF(row_rank = 10, metric_count, NULL)) AS 10th_worst_device_metric_count
    FROM
      device_metric_counts_with_rank
    GROUP BY
      `date`,
      product_id,
      variant_id,
      internal_org_if_true,
      fw,
      service_name,
      manager_name,
      metric_name
  )

  SELECT
    a.`date`,
    a.product_id,
    a.fw,
    a.internal_org_if_true,
    b.metric_name,
    a.active_devices AS number_of_active_devices,
    b.number_of_devices_reporting_this_metric,
    b.total_metric_count,
    b.average_metric_count_for_devices_reporting_anomaly,
    b.25th_percentile_for_devices_reporting_metric,
    b.50th_percentile_for_devices_reporting_metric,
    b.75th_percentile_for_devices_reporting_metric,
    b.worst_device,
    b.2nd_worst_device,
    b.3rd_worst_device,
    b.4th_worst_device,
    b.5th_worst_device,
    b.6th_worst_device,
    b.7th_worst_device,
    b.8th_worst_device,
    b.9th_worst_device,
    b.10th_worst_device,
    b.worst_device_metric_count,
    b.2nd_worst_device_metric_count,
    b.3rd_worst_device_metric_count,
    b.4th_worst_device_metric_count,
    b.5th_worst_device_metric_count,
    b.6th_worst_device_metric_count,
    b.7th_worst_device_metric_count,
    b.8th_worst_device_metric_count,
    b.9th_worst_device_metric_count,
    b.10th_worst_device_metric_count,
    b.service_name,
    b.manager_name,
    a.variant_id
  FROM
    daily_active_devices_by_release_and_org_type AS a
  LEFT JOIN
    transposed_worst_10 AS b
  USING
    (`date`, product_id, variant_id, internal_org_if_true, fw)
  WHERE
    b.metric_name IS NOT NULL
)
;


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS firmware.device_metrics_count
USING DELTA
PARTITIONED BY (`date`)
 AS
  SELECT * FROM final_with_links
;


-- COMMAND ----------

INSERT OVERWRITE firmware.device_metrics_count SELECT * FROM final_with_links
;

