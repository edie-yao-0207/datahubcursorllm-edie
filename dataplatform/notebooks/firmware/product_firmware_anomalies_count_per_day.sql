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

CREATE OR REPLACE TEMP VIEW device_anomaly_counts_with_rank AS (
  WITH daily_anomaly_counts_by_service_manager_and_device AS (
    SELECT
      ae.`date`,
      b.product_id,
      b.variant_id,
      b.internal_org_if_true,
      ae.build as fw,
      ae.service_name_prefix as service_name,
      ae.manager_name,
      CASE
        WHEN ae.service_name_prefix = "kernel" THEN "kernel panic"
        WHEN ae.service_name_prefix = "kernel-panic-oom" THEN "kernel panic: out of memory"
        ELSE ae.short_or_truncated_description
      END AS anomaly_description_first_100_chars,
      ae.device_id,
      sum(ae.anomaly_count) AS anomaly_count
    FROM
      product_analytics.agg_osdanomalyevent_hourly ae
    INNER JOIN device_linked_to_release_and_org_type as b
      USING (date, device_id)
    JOIN dataprep_firmware.data_ingestion_high_water_mark hwm
    WHERE ae.`date` BETWEEN DATE_SUB(CURRENT_DATE(), ${lookback_days}) AND CURRENT_DATE()
      AND (TO_UNIX_TIMESTAMP(date, "yyyy-MM-dd")+(hour_utc*60*60))*1000 <= hwm.time_ms
    GROUP BY
      ae.`date`,
      b.product_id,
      b.variant_id,
      b.internal_org_if_true,
      ae.build,
      service_name,
      anomaly_description_first_100_chars,
      ae.device_id,
      ae.manager_name
  )

  SELECT
    `date`,
    product_id,
    internal_org_if_true,
    fw,
    variant_id,
    service_name,
    manager_name,
    anomaly_description_first_100_chars,
    device_id,
    anomaly_count,
    ROW_NUMBER() OVER (PARTITION BY `date`, product_id, variant_id, internal_org_if_true, fw, service_name, manager_name, anomaly_description_first_100_chars ORDER BY anomaly_count DESC) AS row_rank,
    COUNT(*) OVER (PARTITION BY `date`, product_id, variant_id, internal_org_if_true, fw, service_name, manager_name, anomaly_description_first_100_chars) AS total_devices_in_partition
  FROM
    daily_anomaly_counts_by_service_manager_and_device
)
;


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
      anomaly_description_first_100_chars,
      COUNT(*) AS number_of_devices_reporting_this_anomaly,
      SUM(anomaly_count) AS total_anomaly_count,
      ROUND(SUM(anomaly_count) / COUNT(*), 3) AS average_anomaly_count_for_devices_reporting_anomaly,
      PERCENTILE_APPROX(anomaly_count, 0.25) AS 25th_percentile_for_devices_reporting_anomaly,
      PERCENTILE_APPROX(anomaly_count, 0.50) AS 50th_percentile_for_devices_reporting_anomaly,
      PERCENTILE_APPROX(anomaly_count, 0.75) AS 75th_percentile_for_devices_reporting_anomaly,
      PERCENTILE_APPROX(anomaly_count, 0.95) AS 95th_percentile_for_devices_reporting_anomaly,
      PERCENTILE_APPROX(anomaly_count, 0.99) AS 99th_percentile_for_devices_reporting_anomaly,
      SUM(CASE WHEN row_rank <= 10 THEN anomaly_count ELSE 0 END) AS worst_10_devices_anomaly_count,
      SUM(CASE WHEN row_rank <= 10 THEN anomaly_count ELSE 0 END)/SUM(anomaly_count) AS worst_10_devices_anomaly_proportion_of_total,
      CEILING(total_devices_in_partition * 0.05) AS 5_percent_devices_count,
      SUM(CASE WHEN row_rank <= CEILING(total_devices_in_partition * 0.05) THEN anomaly_count ELSE 0 END) AS worst_5_percent_devices_anomaly_count,
      ROUND(
        SUM(CASE WHEN row_rank <= CEILING(total_devices_in_partition * 0.05) THEN anomaly_count ELSE 0 END)
        / NULLIF(SUM(anomaly_count), 0), 4
      ) AS worst_5_percent_devices_anomaly_proportion_of_total,
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
      MAX(IF(row_rank = 1, anomaly_count, NULL)) AS worst_device_anomaly_count,
      MAX(IF(row_rank = 2, anomaly_count, NULL)) AS 2nd_worst_device_anomaly_count,
      MAX(IF(row_rank = 3, anomaly_count, NULL)) AS 3rd_worst_device_anomaly_count,
      MAX(IF(row_rank = 4, anomaly_count, NULL)) AS 4th_worst_device_anomaly_count,
      MAX(IF(row_rank = 5, anomaly_count, NULL)) AS 5th_worst_device_anomaly_count,
      MAX(IF(row_rank = 6, anomaly_count, NULL)) AS 6th_worst_device_anomaly_count,
      MAX(IF(row_rank = 7, anomaly_count, NULL)) AS 7th_worst_device_anomaly_count,
      MAX(IF(row_rank = 8, anomaly_count, NULL)) AS 8th_worst_device_anomaly_count,
      MAX(IF(row_rank = 9, anomaly_count, NULL)) AS 9th_worst_device_anomaly_count,
      MAX(IF(row_rank = 10, anomaly_count, NULL)) AS 10th_worst_device_anomaly_count
    FROM device_anomaly_counts_with_rank
    GROUP BY
      `date`,
      product_id,
      variant_id,
      internal_org_if_true,
      fw,
      service_name,
      manager_name,
      anomaly_description_first_100_chars,
      total_devices_in_partition
  )
  SELECT
    a.`date`,
    a.product_id,
    a.fw,
    a.internal_org_if_true,
    b.anomaly_description_first_100_chars,
    a.active_devices AS number_of_active_devices,
    b.number_of_devices_reporting_this_anomaly,
    b.total_anomaly_count,
    b.average_anomaly_count_for_devices_reporting_anomaly,
    b.25th_percentile_for_devices_reporting_anomaly,
    b.50th_percentile_for_devices_reporting_anomaly,
    b.75th_percentile_for_devices_reporting_anomaly,
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
    b.worst_device_anomaly_count,
    b.2nd_worst_device_anomaly_count,
    b.3rd_worst_device_anomaly_count,
    b.4th_worst_device_anomaly_count,
    b.5th_worst_device_anomaly_count,
    b.6th_worst_device_anomaly_count,
    b.7th_worst_device_anomaly_count,
    b.8th_worst_device_anomaly_count,
    b.9th_worst_device_anomaly_count,
    b.10th_worst_device_anomaly_count,
    b.service_name,
    b.manager_name,
    b.95th_percentile_for_devices_reporting_anomaly,
    b.99th_percentile_for_devices_reporting_anomaly,
    b.worst_10_devices_anomaly_count,
    b.worst_10_devices_anomaly_proportion_of_total,
    b.5_percent_devices_count,
    b.worst_5_percent_devices_anomaly_count,
    b.worst_5_percent_devices_anomaly_proportion_of_total,
    a.variant_id
  FROM
    daily_active_devices_by_release_and_org_type AS a
  LEFT JOIN
    transposed_worst_10 AS b
  USING
    (`date`, product_id, variant_id, internal_org_if_true, fw)
  WHERE
    b.anomaly_description_first_100_chars IS NOT NULL
)
;



-- COMMAND ----------

CREATE TABLE IF NOT EXISTS firmware.device_anomalies_count
USING DELTA
PARTITIONED BY (`date`)
 AS
  SELECT * FROM final_with_links
;


-- COMMAND ----------

INSERT OVERWRITE firmware.device_anomalies_count SELECT * FROM final_with_links
;

