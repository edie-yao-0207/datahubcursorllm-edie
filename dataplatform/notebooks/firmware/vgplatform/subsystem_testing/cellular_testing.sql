-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Freya Modem Subsystem Metric Reporting
-- MAGIC
-- MAGIC This notebook generates metric reporting data for [Freya Modem subsystem regression testing](https://samsaradev.atlassian.net/wiki/x/wwKiuw). Ensure that the data fields at the top of the notebook are consistent for the devices under test. Afterwards "Run all" on this notebook for a newly compiled set of data.
-- MAGIC
-- MAGIC These metrics should aid in providing a baseline performance of VG55 devices and discern any improvements or regressions as we continue development. If more specific metrics are needed for reporting please feel free to extend the notebook where appropriate.
-- MAGIC
-- MAGIC This notebook is not intended for extended periods of study. Shorter timespans should be used to lower the chances of gateway_id and device_id mappings differing across the study period.
-- MAGIC
-- MAGIC Cellular uptime assumes that the device will have regular stability for ~15 minute reporting periods as it relies on kinesisstats.osdfirmwaremetrics last_hub_connection.delta_ms.max which should be reported every 10 minutes. If tests require a faster cycling time then this method of data retrieval should be modified. Additionally we assume that devices are exlclusively using the cellular interface for connection. Wifi and ethernet should be turned off or not used.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Refine Notebook Parameters

-- COMMAND ----------

-- Collect high level parameters from automated cellular regression tests
SELECT * FROM testautomation_dev.rf_chamber_results WHERE org_id LIKE "%${org_id_under_test}%"

-- COMMAND ----------

-- Convert dates to timestamps if studying beta populations/deployed devices
-- Generates timestamp from 12AM of specified start day, to 11:59PM of specified end day
SELECT
  TO_UNIX_TIMESTAMP(DATE("2025-04-10")+1,"yyyy-MM-dd")*1000-1000 as timestamp_end_ms,
  TO_UNIX_TIMESTAMP(DATE("2025-04-05"),"yyyy-MM-dd")*1000 as timestamp_start_ms

-- COMMAND ----------

-- Create show_page links for device debugging/case studies
CREATE OR REPLACE TEMP FUNCTION device_show_url(org_id BIGINT, device_id BIGINT)
RETURNS STRING
RETURN CONCAT("https://cloud.samsara.com/o/", org_id, "/devices/", device_id, "/show?end_ms=${timestamp_end_ms}&duration=", CAST(${timestamp_end_ms} - ${timestamp_start_ms} AS DOUBLE) / 1000);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Collect daily metrics of test device population

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW daily_metrics_filtered AS (
  SELECT *
  FROM data_analytics.dataprep_vg3x_daily_health_metrics_extended as daily_metrics
  WHERE
    (
      ('${population_specification}' = 'all') OR
      ('${population_specification}' = 'beta' AND daily_metrics.product_program_id=176) OR
      ('${population_specification}' = 'linux 6.1 beta' AND daily_metrics.product_program_id=245) OR
      ('${population_specification}' = 'RF Tent (Left)' AND daily_metrics.org_id=INT(8003499)) OR
      ('${population_specification}' = 'RF Tent (Right)' AND daily_metrics.org_id=INT(8007563)) OR
      ('${population_specification}' = 'RF Tent (Both)' AND (daily_metrics.org_id=INT(8003499) OR daily_metrics.org_id=INT(8007563))) OR
      ('${population_specification}' = 'specific organization' AND daily_metrics.org_id=INT("${org_id_under_test}"))
    ) AND
    daily_metrics.latest_build_on_day=STRING("${build_under_test}") AND
    daily_metrics.product_type LIKE STRING("%${product_under_test}%") AND
    daily_metrics.date BETWEEN TO_DATE(FROM_UNIXTIME("${timestamp_start_ms}"/1000)) AND TO_DATE(FROM_UNIXTIME("${timestamp_end_ms}"/1000))
);

CREATE OR REPLACE TEMP VIEW cellular_regression_test_device_daily_reports AS (
  SELECT
    daily_metrics_filtered.date,
    daily_metrics_filtered.device_id,
    daily_metrics_filtered.org_id,
    daily_metrics_filtered.perc_gps_uptime,
    daily_metrics_filtered.rollout_stage_id,
    daily_metrics_filtered.product_program_id,
    daily_metrics_filtered.product_type,
    daily_metrics_filtered.latest_build_on_day,
    daily_metrics_filtered.status
  FROM
    daily_metrics_filtered
);
CACHE TABLE cellular_regression_test_device_daily_reports;
-- SELECT * FROM cellular_regression_test_device_daily_reports ORDER BY date;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW cellular_regression_test_device_population AS (
SELECT
  device_id,
  org_id,
  rollout_stage_id,
  product_program_id,
  product_type
FROM
  cellular_regression_test_device_daily_reports
GROUP BY
  device_id,
  org_id,
  rollout_stage_id,
  product_program_id,
  product_type
);
CACHE TABLE cellular_regression_test_device_population;

-- COMMAND ----------

-- return population size
SELECT COUNT(DISTINCT(pop.device_id)) FROM cellular_regression_test_device_population as pop;

-- COMMAND ----------

-- return population size by day
SELECT date, COUNT(DISTINCT(pop.device_id)) FROM cellular_regression_test_device_daily_reports as pop GROUP BY date ORDER BY date ASC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Anomaly and Panic Data Prep

-- COMMAND ----------

-- Use output from table anomaly_counts to log in anomalies CSV
CREATE OR REPLACE TEMP VIEW anomalies_filtered AS (
  SELECT *
  FROM kinesisstats.osdanomalyevent as events
  WHERE
    events.time BETWEEN "${timestamp_start_ms}" AND "${timestamp_end_ms}" AND
    events.value.proto_value.anomaly_event.build LIKE STRING("${build_under_test}")
);

CREATE OR REPLACE TEMP VIEW firmware_anomaly_events_filtered AS (
  SELECT
    events.object_id,
    events.org_id,
    events.time,
    events.date,
    events.value.proto_value.anomaly_event.boot_count,
    events.value.proto_value.anomaly_event.service_name as service,
    events.value.proto_value.anomaly_event.description as description,
    events.value.proto_value.anomaly_event.build as build
  FROM
    cellular_regression_test_device_population AS test_devices
  JOIN
    anomalies_filtered AS events
  ON
    events.object_id=test_devices.device_id
);
CACHE TABLE firmware_anomaly_events_filtered;
-- SELECT * FROM firmware_anomaly_events_filtered ORDER BY time

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Anomaly Metrics

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW firmware_anomalies_filtered AS (
  SELECT
    *
  FROM
    firmware_anomaly_events_filtered
  WHERE
    description NOT LIKE "%panic%"
);
CACHE TABLE firmware_anomalies_filtered;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW anomaly_counts AS (
  SELECT
    build,
    SPLIT(service, ':')[0] as service,
    COUNT(*) AS occurrence_count,
    COUNT(DISTINCT firmware_anomalies_filtered.object_id) as affected_devices,
    COUNT(*) / COUNT(DISTINCT firmware_anomalies_filtered.object_id) as count_per_affected_device,
    COUNT(*) / (SELECT COUNT(DISTINCT(pop.device_id)) FROM cellular_regression_test_device_population as pop) as count_per_device_in_testbed
  FROM
    firmware_anomalies_filtered
  GROUP BY
    build,
    SPLIT(service, ':')[0]
);
SELECT * FROM anomaly_counts ORDER BY occurrence_count DESC;

-- COMMAND ----------

-- Get showpages for presenting devices
SELECT
  object_id,
  org_id,
  service,
  description,
  time,
  device_show_url(org_id, object_id) AS show_page
FROM firmware_anomalies_filtered
-- WHERE
--   service LIKE "%cellular_manager%" OR
--   service LIKE "%location_manager%"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Panic Metrics

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW panics_filtered AS (
  SELECT
    *
  FROM
    firmware_anomaly_events_filtered
  WHERE
    description LIKE "%panic%"
);
CACHE TABLE panics_filtered;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW panic_counts AS (
  SELECT
    build,
    SPLIT(service, ':')[0] as service,
    COUNT(*) AS occurrence_count,
    COUNT(DISTINCT panics_filtered.object_id) as affected_devices,
    COUNT(*) / COUNT(DISTINCT panics_filtered.object_id) as count_per_affected_device,
    COUNT(*) / (SELECT COUNT(DISTINCT(pop.device_id)) FROM cellular_regression_test_device_population as pop) as count_per_device_in_testbed
  FROM
    panics_filtered
  GROUP BY
    build,
    SPLIT(service, ':')[0]
);
SELECT * FROM panic_counts ORDER BY occurrence_count DESC;

-- COMMAND ----------

-- Show panic types
SELECT
  service,
  COUNT(*),
  COUNT(DISTINCT object_id) as affected_devices,
  COUNT(*) / COUNT(DISTINCT object_id) as count_per_affected_device,
  COUNT(*) / (SELECT COUNT(DISTINCT(pop.device_id)) FROM cellular_regression_test_device_population as pop) as count_per_device_in_testbed
FROM panics_filtered
-- WHERE
--   service LIKE "%cellular_manager%" OR
--   service LIKE "%location_manager%"
GROUP BY
  service
ORDER BY count(1) DESC

-- COMMAND ----------

-- See instances to view description for problem services
CREATE OR REPLACE TEMP VIEW panic_search AS (
SELECT
  object_id,
  org_id,
  service,
  description,
  time,
  device_show_url(org_id, object_id) AS show_page
FROM
  panics_filtered
-- WHERE
  -- (
    -- description LIKE "%failed to start mdm9607Gps%" OR
    -- description LIKE "%mdm9607 gps%" OR
    -- service LIKE "%location_manager%" OR
    -- service LIKE "%gpsd%" OR
    -- service LIKE "%cellular_manager%"
    -- description LIKE "%No GPS Data with good SNR%"
    -- description LIKE "%rmnet_data1%" --
  -- )
);
CACHE TABLE panic_search;
SELECT * FROM panic_search;

-- COMMAND ----------

-- see panic counts per device for debugging
SELECT
  org_id,
  object_id,
  service,
  COUNT(*) AS occurrence_count,
  device_show_url(org_id, object_id) AS show_page
FROM
  panics_filtered
GROUP BY
  org_id,
  object_id,
  service
ORDER BY occurrence_count DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Firmware Metrics Data Prep

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW firmware_metrics_filtered AS (
  SELECT
    *,
    exploded_metrics.name AS metric_name,
    exploded_metrics.value AS metric_value,
    exploded_metrics.tags AS metric_tags
  FROM kinesisstats.osdfirmwaremetrics as metrics
  LATERAL VIEW EXPLODE(metrics.value.proto_value.firmware_metrics.metrics) as exploded_metrics
  WHERE
    metrics.time BETWEEN "${timestamp_start_ms}" AND "${timestamp_end_ms}" AND
    metrics.value.proto_value.firmware_metrics.build LIKE "%${build_under_test}%"
);

CREATE OR REPLACE TEMP VIEW cellular_regression_test_firmware_metrics_joined AS (
  SELECT
    fw_metrics.object_id,
    fw_metrics.org_id,
    fw_metrics.time,
    fw_metrics.date,
    fw_metrics.value.proto_value.firmware_metrics.build as build,
    metric_name,
    metric_value,
    metric_tags
  FROM
    cellular_regression_test_device_population AS test_devices
  JOIN
    firmware_metrics_filtered AS fw_metrics
  ON
    test_devices.device_id=fw_metrics.object_id
);
CACHE TABLE cellular_regression_test_firmware_metrics_joined;
-- SELECT * FROM cellular_regression_test_firmware_metrics_joined

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Kernel Panics

-- COMMAND ----------

SELECT
  *,
  device_show_url(org_id, object_id) AS show_page
FROM
  cellular_regression_test_firmware_metrics_joined
WHERE
  metric_name LIKE "%kernel.panic%"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Firmware Metrics

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW firmware_metrics_study AS (
  SELECT
    *
  FROM
    cellular_regression_test_firmware_metrics_joined as firmware_metrics_joined
  WHERE
    metric_name LIKE "%network.wifi.gpio.fix%" OR             -- Related to GPIO/Wifi driver issues (patched)
    metric_name LIKE "%network.qmi.interface.missing%" OR     -- Related to rmnet_data0 interface issues (patched)
    metric_name LIKE "%network.wifi.interface.up.failure%" OR -- Related to GPIO/Wifi driver issues (patched)
    metric_name LIKE "%network.qmi.service_handle.missing%" OR -- Related to LE217 issue
    metric_name LIKE "%system.reboot%"                        -- General health monitor
  --   metric_tags[0] LIKE "%RMNET_MISSING%"                  -- Related to rmnet_data0 interface issues (patched)
  --   metric_tags[0] LIKE "%NO_HUB_CONNECTIVITY%" OR         -- Generic reboot reason, should investigate cause
  --   metric_tags[0] LIKE "%CELLULAR_QMI_ERRORS%" OR         -- Generic reboot reason, should investigate cause
  --   metric_tags[0] LIKE "%FIRMWARE_UPGRADE%" OR            -- Separate firmware upgrade reason from normal counts
    -- May add any other metrics to track or investigate
);
CACHE TABLE firmware_metrics_study;

-- COMMAND ----------

-- Get showpages for presenting devices
SELECT
  object_id,
  org_id,
  metric_name,
  metric_tags,
  time,
  device_show_url(org_id, object_id) AS show_page
FROM firmware_metrics_study
ORDER BY metric_name DESC

-- COMMAND ----------

-- Use output from table firmware_metrics_count to log in firmware metrics CSV
CREATE OR REPLACE TEMP VIEW cellular_regression_firmware_bug_metric_count AS (
  SELECT
    metric_name,
    metric_tags,
    COUNT(DISTINCT object_id) AS affected_device_count,
    COUNT(*) AS total_metric_count,
    COUNT(*) / (SELECT COUNT(DISTINCT(pop.device_id)) FROM cellular_regression_test_device_population as pop) AS count_per_device_in_testbed,
    COUNT(*) / DATEDIFF(TO_DATE(FROM_UNIXTIME("${timestamp_end_ms}"/1000)), TO_DATE(FROM_UNIXTIME("${timestamp_start_ms}"/1000)))+1 AS avg_count_per_day_of_study,
    DATEDIFF(TO_DATE(FROM_UNIXTIME("${timestamp_end_ms}"/1000)), TO_DATE(FROM_UNIXTIME("${timestamp_start_ms}"/1000)))+1 AS days_of_study
  FROM
    firmware_metrics_study
  GROUP BY
    metric_name,
    metric_tags
);
SELECT * FROM cellular_regression_firmware_bug_metric_count ORDER BY total_metric_count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Cellular Uptime Metrics

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW cellular_regression_test_uptime_metrics  AS (
  SELECT *
  FROM cellular_regression_test_firmware_metrics_joined
  WHERE metric_name = 'hubclient.last_hub_connection.delta_ms.max'
  GROUP BY ALL
  ORDER BY time DESC
);
CACHE TABLE cellular_regression_test_uptime_metrics;
-- SELECT * FROM cellular_regression_test_uptime_metrics;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW cellular_uptime_last_hub_connections AS (
  WITH cellular_uptime_with_prev_time AS (
    SELECT
      *,
      LAG(time) OVER (PARTITION BY object_id ORDER BY time) AS previous_report_time,
      time - LAG(time) OVER (PARTITION BY object_id ORDER BY time) AS time_since_last_report
    FROM
      cellular_regression_test_uptime_metrics
  )
  SELECT
    *,
    -- collect last_hub_connection delta values
    CASE
      -- account for disconnections impacting reporting before relying on metric_value, check-in should be every 10 mins
      -- report if check-in not performed in over 15 minutes.
      WHEN (metric_value IS NULL OR metric_value < time_since_last_report) AND time_since_last_report > 600000*1.5 then time_since_last_report
      -- null case
      WHEN metric_value IS NULL THEN 0
      -- do not count compounding disconnection instances, we rely on a summation of time_disconnected after this table is populated
      WHEN LEAD(metric_value) OVER (PARTITION BY object_id ORDER BY time) - 600000 > 0 THEN null
      -- ignore cases where hubclient.last_hub_connection.delta_ms.max is under 70 seconds. Devices upload every ~60 seconds but can vary by a few seconds.
      -- Don't count these cases as a disconnection
      WHEN metric_value < 70000 THEN 0
      -- limit time_disconnected to the period of reporting
      WHEN metric_value > time - MIN(time) OVER (PARTITION BY object_id) THEN time - MIN(time) OVER (PARTITION BY object_id)
      ELSE metric_value
    END AS time_disconnected
  FROM
    cellular_uptime_with_prev_time
);
CACHE TABLE cellular_uptime_last_hub_connections;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW cellular_uptime_last_hub_connection_summation AS (
  SELECT
    object_id,
    org_id,
    COUNT(*) AS metric_sample_count,
    SUM(time_disconnected) AS total_time_disconnected,
    MAX(time) - MIN(time) AS total_time_span
  FROM
    cellular_uptime_last_hub_connections
  GROUP BY
    object_id,
    org_id
);
SELECT * FROM cellular_uptime_last_hub_connection_summation;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW cellular_regression_test_uptime_results AS (
  SELECT
    *,
    (1 - (total_time_disconnected / total_time_span))*100 as cellular_uptime_percent
  FROM
    cellular_uptime_last_hub_connection_summation
  ORDER BY cellular_uptime_percent ASC
);
CACHE TABLE cellular_regression_test_uptime_results;
SELECT * FROM cellular_regression_test_uptime_results;

-- COMMAND ----------

-- Get showpages for presenting devices
SELECT
  *,
  device_show_url(org_id, object_id) AS show_page
FROM cellular_regression_test_uptime_results

-- COMMAND ----------

-- Show accumulated average, median, and mode of cellular_uptime
SELECT
  ROUND(AVG(cellular_uptime_percent),1) AS avg_cellular_uptime,
  ROUND(MEDIAN(cellular_uptime_percent),1) AS median_cellular_uptime,
  ROUND(STDDEV(cellular_uptime_percent),1) AS stddev_cellular_uptime,
  ROUND(PERCENTILE(cellular_uptime_percent, 0.25),1) AS p25_cellular_uptime,
  ROUND(PERCENTILE(cellular_uptime_percent, 0.15),1) AS p15_cellular_uptime,
  ROUND(PERCENTILE(cellular_uptime_percent, 0.10),1) AS p10_cellular_uptime,
  ROUND(PERCENTILE(cellular_uptime_percent, 0.05),1) AS p05_cellular_uptime,
  ROUND(PERCENTILE(cellular_uptime_percent, 0.01),1) AS p01_cellular_uptime
FROM
  cellular_regression_test_uptime_results
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## GPS Uptime Metrics

-- COMMAND ----------

-- Show accumulated average, median, and mode of perc_gps_uptime
SELECT
  ROUND(AVG(perc_gps_uptime),1) AS avg_perc_gps_uptime,
  ROUND(MEDIAN(perc_gps_uptime),1) AS median_perc_gps_uptime,
  ROUND(STDDEV(perc_gps_uptime),1) AS stddev_perc_gps_uptime,
  ROUND(PERCENTILE(perc_gps_uptime, 0.25),1) AS p25_gps_uptime,
  ROUND(PERCENTILE(perc_gps_uptime, 0.15),1) AS p15_gps_uptime,
  ROUND(PERCENTILE(perc_gps_uptime, 0.10),1) AS p10_gps_uptime,
  ROUND(PERCENTILE(perc_gps_uptime, 0.05),1) AS p05_gps_uptime,
  ROUND(PERCENTILE(perc_gps_uptime, 0.01),1) AS p01_gps_uptime
FROM
  cellular_regression_test_device_daily_reports
;

-- COMMAND ----------

-- Show top and worst performers
CREATE OR REPLACE TEMP VIEW gps_uptime_results AS (
SELECT
  device_id,
  org_id,
  AVG(perc_gps_uptime) AS avg_perc_gps_uptime
FROM
  cellular_regression_test_device_daily_reports
GROUP BY
  device_id,
  org_id
ORDER BY avg_perc_gps_uptime ASC
);
CACHE TABLE gps_uptime_results;

-- COMMAND ----------

-- Get showpages for presenting devices
SELECT
  *,
  device_show_url(org_id, device_id) AS show_page
FROM gps_uptime_results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CPU and Memory Consumption

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW cpu_and_mem_usage AS (
  SELECT
    *
  FROM
    cellular_regression_test_firmware_metrics_joined
  WHERE
    metric_name LIKE "%system.cpu.usage.avg%" OR
    metric_name LIKE "%system.cpu.usage.max%" OR
    metric_name LIKE "%system.mem.usage.avg%" OR
    metric_name LIKE "%system.mem.usage.max%"
  GROUP BY ALL
);
-- SELECT * FROM cpu_and_mem_usage ORDER BY date

SELECT
  ROUND(AVG(CASE WHEN metric_name = 'system.cpu.usage.avg' THEN metric_value ELSE NULL END), 1) AS avg_system_cpu_usage_avg,
  ROUND(AVG(CASE WHEN metric_name = 'system.cpu.usage.max' THEN metric_value ELSE NULL END), 1) AS avg_system_cpu_usage_max,
  ROUND(PERCENTILE(CASE WHEN metric_name = 'system.cpu.usage.max' THEN metric_value ELSE NULL END, 0.9), 1) AS avg_system_cpu_usage_max_p90,
  ROUND(AVG(CASE WHEN metric_name = 'system.mem.usage.avg' THEN metric_value ELSE NULL END), 1) AS avg_system_mem_usage_avg,
  ROUND(AVG(CASE WHEN metric_name = 'system.mem.usage.max' THEN metric_value ELSE NULL END), 1) AS avg_system_mem_usage_max,
  ROUND(PERCENTILE(CASE WHEN metric_name = 'system.mem.usage.max' THEN metric_value ELSE NULL END, 0.9), 1) AS avg_system_mem_usage_max_p90
FROM
  cpu_and_mem_usage