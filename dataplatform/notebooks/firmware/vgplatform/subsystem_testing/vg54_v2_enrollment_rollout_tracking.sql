-- Databricks notebook source
-- MAGIC %md
-- MAGIC # VG54 v2 Enrollment Rollout Tracking Check-in
-- MAGIC
-- MAGIC To be exported and merged into backend for automated buildkite runs
-- MAGIC
-- MAGIC LD FF: https://app.launchdarkly.com/projects/samsara/flags/migrate-existing-gateways-to-use-secure-authentication/targeting?env=production&selected-env=production
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Enrollment Rollout Tracking

-- COMMAND ----------

-- DBTITLE 1,Get daily device reports
CREATE OR REPLACE TABLE firmware_dev.vg54_v2_enrollment_daily_devices AS (
  SELECT
    date,
    device_id,
    org_name,
    org_id,
    org_type,
    product_type,
    latest_build_on_day,
    status
  FROM data_analytics.dataprep_vg3x_daily_health_metrics_extended as daily_metrics
  WHERE
    daily_metrics.date BETWEEN (current_date() - 7) AND (current_date() - 1)  AND -- Get latest state within the previous week, limits records to one per device
    daily_metrics.product_type LIKE STRING("%VG54%")
  );

-- COMMAND ----------

-- DBTITLE 1,Cohort of devices targeted by v2 auth feature flag
CREATE OR REPLACE TABLE firmware_dev.vg54_v2_enrollment_devices AS (
  SELECT
    *,
    daily_metrics.date as last_reported_connection
  FROM firmware_dev.vg54_v2_enrollment_daily_devices as daily_metrics
  -- Only take the latest occurence, we don't want repeat rows of a single device
  QUALIFY ROW_NUMBER() OVER (PARTITION BY daily_metrics.device_id, daily_metrics.org_id ORDER BY daily_metrics.date DESC) = 1
);

-- COMMAND ----------

-- DBTITLE 1,Filter gateways to see all devices under rollout (online + offline)
CREATE OR REPLACE TABLE firmware_dev.vg54_v2_enrollment_all_devices AS (
  SELECT * FROM clouddb.gateways AS A
  WHERE
    a.product_id in (53, 89)
);

-- COMMAND ----------

-- DBTITLE 1,All VG54s in the v2 auth cohort
CREATE OR REPLACE TABLE firmware_dev.vg54_v2_enrollment_tracking_info AS (
  SELECT
    a.last_reported_connection,
    a.status,
    b.id as gateway_id,
    b.device_id,
    b.org_id,
    a.org_name,
    a.org_type,
    a.product_type,
    b.product_id,
    a.latest_build_on_day,
    b.serial,
    b.variant_id,
    b.public_key IS NOT NULL AS key_onboarded,
    b.public_key_type,
    b.enrolled_at,
    b.enrolled,
    b.hubserver_key_expires_at
  FROM
    firmware_dev.vg54_v2_enrollment_devices AS a
  LEFT JOIN
    firmware_dev.vg54_v2_enrollment_all_devices AS b
  ON
    a.device_id=b.device_id AND
    a.org_id=b.org_id
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Latest enrollment Stats

-- COMMAND ----------

-- DBTITLE 1,VG54 devices and the most recent enrollment log for them
CREATE OR REPLACE TABLE firmware_dev.vg54_v2_enrollment_devices_last_enrollment as (
SELECT
  a.device_id,
  a.gateway_id,
  a.org_id,
  a.serial,
  a.variant_id,
  a.enrolled_at,
  a.key_onboarded,
  MAX(b.created_at) as last_enrollment_attempted_at,
  max_by(b.succeeded, b.created_at) == 1 as last_enrollment_succeeded,
  case
    when max_by(b.enrollment_type, b.created_at) == 2 then "v1"
    when max_by(b.enrollment_type, b.created_at) == 4 then "v2"
    when max_by(b.enrollment_type, b.created_at) == 5 then "onboarding"
    when max_by(b.enrollment_type, b.created_at) == 8 then "field onboarding"
    else printf("unexpected value: %d", max_by(b.enrollment_type, b.created_at))
  end as last_enrollment_type
FROM
  firmware_dev.vg54_v2_enrollment_tracking_info a
JOIN
   productsdb.enrollment_logs b
ON
  a.device_id = b.device_id
GROUP BY a.device_id, a.gateway_id, a.org_id, a.serial, a.variant_id, a.enrolled_at, a.key_onboarded
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Anomaly Analysis

-- COMMAND ----------

-- DBTITLE 1,Get anomalies in timeframe of study
-- Use output from table anomaly_counts to log in anomalies CSV
CREATE OR REPLACE TEMP VIEW anomalies_filtered AS (
  SELECT *
  FROM kinesisstats.osdanomalyevent as events
  WHERE
    events.date BETWEEN (current_date() - 7) AND (current_date() - 1)
);

-- COMMAND ----------

-- DBTITLE 1,Filter anomalies by devices in study
CREATE OR REPLACE TEMP VIEW v2_enrollment_anomaly_events_filtered AS (
  SELECT
    events.object_id,
    events.org_id,
    events.time,
    events.date,
    enrollment_devices.key_onboarded,
    enrollment_devices.enrolled,
    events.value.proto_value.anomaly_event.boot_count,
    events.value.proto_value.anomaly_event.service_name as service,
    events.value.proto_value.anomaly_event.description as description,
    events.value.proto_value.anomaly_event.build as build
  FROM
    firmware_dev.vg54_v2_enrollment_tracking_info AS enrollment_devices
  JOIN
    anomalies_filtered AS events
  ON
    events.object_id = enrollment_devices.device_id AND
    events.org_id = enrollment_devices.org_id
);

-- COMMAND ----------

-- DBTITLE 1,Filter anomalies further for only enrollment anomalies
CREATE OR REPLACE TABLE firmware_dev.vg54_v2_enrollment_anomaly_events_filtered as (
  SELECT * FROM v2_enrollment_anomaly_events_filtered
  WHERE
    service LIKE "%enroll%" or
    description LIKE "%V2 enrollment%" or
    description LIKE "%gatewaykey error when checking for key%"
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Device Days Online

-- COMMAND ----------

-- DBTITLE 1,Get device daily reports for the previous week
CREATE OR REPLACE TEMP VIEW vg54_v2_enrollment_previous_week_daily_devices AS (
  SELECT
    date,
    device_id,
    org_name,
    org_id,
    org_type,
    product_type,
    latest_build_on_day,
    status
  FROM data_analytics.dataprep_vg3x_daily_health_metrics_extended as daily_metrics
  WHERE
    -- Latest week is current (current_date() - 7) AND (current_date() - 1), previous week is (current_date() - 15) AND (current_date()-9)
    daily_metrics.date BETWEEN (current_date() - 15) AND (current_date()-9)  AND
    daily_metrics.product_type LIKE STRING("%VG54%")
  );

-- COMMAND ----------

-- DBTITLE 1,Join previous week stats with current week stats to get avg days online comparison
CREATE OR REPLACE TABLE firmware_dev.vg54_v2_enrollment_devices_days_online_in_last_week AS (
SELECT
  a.device_id,
  a.org_id,
  COUNT(*) AS days_online_in_past_week,
  COALESCE(prev_week.days_online_in_the_previous_week, 0) AS days_online_in_the_previous_week,
  COUNT(*) - COALESCE(prev_week.days_online_in_the_previous_week, 0) as days_online_weekly_delta,
  last_enrollment.last_enrollment_type,
  last_enrollment.last_enrollment_attempted_at,
  last_enrollment.last_enrollment_succeeded
FROM
  firmware_dev.vg54_v2_enrollment_daily_devices a
LEFT JOIN (
  SELECT
    device_id,
    org_id,
    COUNT(*) AS days_online_in_the_previous_week
  FROM
    vg54_v2_enrollment_previous_week_daily_devices
  GROUP BY device_id, org_id
) prev_week
ON
  a.device_id = prev_week.device_id AND
  a.org_id = prev_week.org_id
LEFT JOIN
  firmware_dev.vg54_v2_enrollment_devices_last_enrollment last_enrollment
ON
  a.device_id = last_enrollment.device_id AND
  a.org_id = last_enrollment.org_id
GROUP BY a.device_id, a.org_id, prev_week.days_online_in_the_previous_week, last_enrollment.last_enrollment_type, last_enrollment.last_enrollment_attempted_at, last_enrollment.last_enrollment_succeeded
);
