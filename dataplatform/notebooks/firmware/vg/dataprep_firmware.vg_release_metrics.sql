-- Databricks notebook source

-- MAGIC %python
-- MAGIC spark.conf.set("spark.sql.broadcastTimeout", 36000)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW date_table AS (
  SELECT date_sub(current_date, 8) AS date
)

-- COMMAND ----------

SELECT date FROM date_table

-- COMMAND ----------

--Count of how times each device shows up in baxter_devices table
CREATE OR REPLACE TEMP VIEW baxter_devices AS (
  SELECT
    org_id,
    vg_device_id,
    count(*) AS record_count
  FROM data_analytics.baxter_devices
  WHERE date >= date_sub(current_date, 1)
  GROUP BY
    org_id,
    vg_device_id
)

-- COMMAND ----------

--Get device data and explicity resolve null camera_product_id
CREATE OR REPLACE TEMPORARY VIEW vg_devices AS (
  SELECT a.id AS gateway_id,
    a.device_id,
    a.org_id,
    b.camera_serial,
    a.product_id,
    a.variant_id,
    IF(bd.record_count >= 1, True, False) AS has_baxter,
    b.camera_product_id,
    COALESCE(hw.product_version, 'Version 1') AS product_version,
    CASE
      WHEN c.internal_type = 1 THEN "test"
      ELSE "customer"
      -- WHEN c.internal_type = 1 THEN "Internal"
      -- ELSE "Customer"
    END AS org_type,
    d.first_heartbeat_date,
    d.first_heartbeat_ms,
    d.last_heartbeat_date,
    d.last_heartbeat_ms
  FROM clouddb.gateways AS a
  LEFT JOIN clouddb.devices AS b ON
    a.device_id = b.id
  LEFT JOIN clouddb.organizations AS c ON
    a.org_id = c.id
  LEFT JOIN dataprep.device_heartbeats_extended AS d ON
    a.org_id = d.org_id
    AND a.device_id = d.device_id
  LEFT JOIN data_analytics.vg_hardware_variant_info AS hw ON
    a.org_id = hw.org_id
    AND a.device_id = hw.device_id
    AND a.product_id = hw.product_id
  LEFT JOIN baxter_devices AS bd ON
    a.org_id = bd.org_id
    AND a.device_id = bd.vg_device_id
  WHERE a.product_id IN (24,35,53,89,178)
    AND last_heartbeat_date >= date_sub(current_date(), 15)  --- turn this back on after backfill
)

-- COMMAND ----------

--Label product pairs
CREATE OR REPLACE TEMP VIEW device_list AS (
  SELECT
    gateway_id,
    device_id,
    org_id,
    camera_serial,
    product_id,
    CASE
      WHEN a.product_id = 24 AND a.camera_product_id = 30 THEN "VG34 + CM12"
      WHEN a.product_id = 24 AND a.camera_product_id = 31 THEN "VG34 + CM22"
      WHEN a.product_id = 24 AND a.camera_product_id = 44 THEN "VG34 + CM31"
      WHEN a.product_id = 24 AND a.camera_product_id = 43 THEN "VG34 + CM32"
      WHEN a.product_id = 24 AND a.camera_product_id = 167 THEN "VG34 + CM33"
      WHEN a.product_id = 24 AND a.camera_product_id = 155 THEN "VG34 + CM34"
      WHEN a.product_id = 53 AND a.variant_id NOT IN (10) AND a.camera_product_id = 30 THEN "VG54-NA + CM12"
      WHEN a.product_id = 53 AND a.variant_id NOT IN (10) AND a.camera_product_id = 31 THEN "VG54-NA + CM22"
      WHEN a.product_id = 53 AND a.variant_id NOT IN (10) AND a.camera_product_id = 44 THEN "VG54-NA + CM31"
      WHEN a.product_id = 53 AND a.variant_id NOT IN (10) AND a.camera_product_id = 43 THEN "VG54-NA + CM32"
      WHEN a.product_id = 53 AND a.variant_id NOT IN (10) AND a.camera_product_id = 167 THEN "VG54-NA + CM33"
      WHEN a.product_id = 53 AND a.variant_id NOT IN (10) AND a.camera_product_id = 155 THEN "VG54-NA + CM34"
      WHEN a.product_id = 53 AND a.variant_id IN (10) AND a.camera_product_id = 30 THEN "VG54-NAH + CM12"
      WHEN a.product_id = 53 AND a.variant_id IN (10) AND a.camera_product_id = 31 THEN "VG54-NAH + CM22"
      WHEN a.product_id = 53 AND a.variant_id IN (10) AND a.camera_product_id = 44 THEN "VG54-NAH + CM31"
      WHEN a.product_id = 53 AND a.variant_id IN (10) AND a.camera_product_id = 43 THEN "VG54-NAH + CM32"
      WHEN a.product_id = 53 AND a.variant_id IN (10) AND a.camera_product_id = 167 THEN "VG54-NAH + CM33"
      WHEN a.product_id = 53 AND a.variant_id IN (10) AND a.camera_product_id = 155 THEN "VG54-NAH + CM34"
      WHEN a.product_id = 35 AND a.camera_product_id = 30 THEN "VG34-EU + CM12"
      WHEN a.product_id = 35 AND a.camera_product_id = 31 THEN "VG34-EU + CM22"
      WHEN a.product_id = 35 AND a.camera_product_id = 44 THEN "VG34-EU + CM31"
      WHEN a.product_id = 35 AND a.camera_product_id = 43 THEN "VG34-EU + CM32"
      WHEN a.product_id = 35 AND a.camera_product_id = 167 THEN "VG34-EU + CM33"
      WHEN a.product_id = 35 AND a.camera_product_id = 155 THEN "VG34-EU + CM34"
      WHEN a.product_id = 89 AND a.camera_product_id = 30 THEN "VG54-EU + CM12"
      WHEN a.product_id = 89 AND a.camera_product_id = 31 THEN "VG54-EU + CM22"
      WHEN a.product_id = 89 AND a.camera_product_id = 44 THEN "VG54-EU + CM31"
      WHEN a.product_id = 89 AND a.camera_product_id = 43 THEN "VG54-EU + CM32"
      WHEN a.product_id = 89 AND a.camera_product_id = 167 THEN "VG54-EU + CM33"
      WHEN a.product_id = 89 AND a.camera_product_id = 155 THEN "VG54-EU + CM34"
      WHEN a.product_id = 178 AND a.variant_id NOT IN (16, 17) AND a.camera_product_id = 30 THEN "VG55-NA + CM12"
      WHEN a.product_id = 178 AND a.variant_id NOT IN (16, 17) AND a.camera_product_id = 31 THEN "VG55-NA + CM22"
      WHEN a.product_id = 178 AND a.variant_id NOT IN (16, 17) AND a.camera_product_id = 44 THEN "VG55-NA + CM31"
      WHEN a.product_id = 178 AND a.variant_id NOT IN (16, 17) AND a.camera_product_id = 43 THEN "VG55-NA + CM32"
      WHEN a.product_id = 178 AND a.variant_id NOT IN (16, 17) AND a.camera_product_id = 167 THEN "VG55-NA + CM33"
      WHEN a.product_id = 178 AND a.variant_id NOT IN (16, 17) AND a.camera_product_id = 155 THEN "VG55-NA + CM34"
      WHEN a.product_id = 178 AND a.variant_id IN (16) AND a.camera_product_id = 30 THEN "VG55-EU + CM12"
      WHEN a.product_id = 178 AND a.variant_id IN (16) AND a.camera_product_id = 31 THEN "VG55-EU + CM22"
      WHEN a.product_id = 178 AND a.variant_id IN (16) AND a.camera_product_id = 44 THEN "VG55-EU + CM31"
      WHEN a.product_id = 178 AND a.variant_id IN (16) AND a.camera_product_id = 43 THEN "VG55-EU + CM32"
      WHEN a.product_id = 178 AND a.variant_id IN (16) AND a.camera_product_id = 167 THEN "VG55-EU + CM33"
      WHEN a.product_id = 178 AND a.variant_id IN (16) AND a.camera_product_id = 155 THEN "VG55-EU + CM34"
      WHEN a.product_id = 178 AND a.variant_id IN (17) AND a.camera_product_id = 44 THEN "VG55-FN + CM31"
      WHEN a.product_id = 178 AND a.variant_id IN (17) AND a.camera_product_id = 43 THEN "VG55-FN + CM32"
      WHEN a.product_id = 178 AND a.variant_id IN (17) AND a.camera_product_id = 30 THEN "VG55-FN + CM12"
      WHEN a.product_id = 178 AND a.variant_id IN (17) AND a.camera_product_id = 31 THEN "VG55-FN + CM22"
      WHEN a.product_id = 178 AND a.variant_id IN (17) AND a.camera_product_id = 155 THEN "VG55-FN + CM34"
      WHEN a.product_id = 178 AND a.variant_id IN (17) AND a.camera_product_id = 167 THEN "VG55-FN + CM33"
      WHEN a.product_id = 178 AND a.variant_id NOT IN (16, 17) THEN "VG55-NA no CM"
      WHEN a.product_id = 178 AND a.variant_id IN (16) THEN "VG55-EU no CM"
      WHEN a.product_id = 178 AND a.variant_id IN (17) THEN "VG55-FN no CM"
      WHEN a.product_id = 89 THEN "VG54-EU no CM"
      WHEN a.product_id = 35 THEN "VG34-EU no CM"
      WHEN a.product_id = 53 AND a.variant_id NOT IN (10) THEN "VG54-NA no CM"
      WHEN a.product_id = 53 AND a.variant_id IN (10) THEN "VG54-NAH no CM"
      WHEN a.product_id = 24 THEN "VG34 no CM"
      ELSE "error"
    END AS product_type,
    product_version,
    has_baxter,
    org_type,
    first_heartbeat_date,
    first_heartbeat_ms,
    last_heartbeat_date,
    last_heartbeat_ms
  FROM vg_devices a
)

-- COMMAND ----------

--Add CM ID
CREATE OR REPLACE TEMPORARY VIEW devices AS (
 SELECT a.*, b.id AS cm_id
 FROM device_list AS a
 LEFT JOIN clouddb.devices AS b
 ON upper(replace(a.camera_serial,'-','')) = upper(replace(b.serial,'-',''))
);

-- COMMAND ----------

--Create hourly intervals by device
CREATE OR REPLACE TEMPORARY VIEW temp_device_hourly AS (
  SELECT a.*,
  explode(sequence((to_unix_timestamp(date_sub(current_date(), 8))*1000), (unix_timestamp()*1000)-3*60*60*1000, 60*60*1000)) AS current_time   --- turn this back on after backfill *actually just change 30 back to 7
  FROM devices AS a
);


CREATE OR REPLACE TEMPORARY VIEW device_hourly AS (
  SELECT a.*,
  (lag(current_time, 1) OVER (PARTITION BY org_id, gateway_id, device_id ORDER BY current_time DESC)) AS next_time
  FROM temp_device_hourly AS a
);

-- COMMAND ----------

 -- Now that we have our daily intervals table, we need to intersect these date ranges with the ranges of
 -- time that a vg has a modi attached. The object stat used to construct the modi attached intervals is only
 -- reported on change. We shouldn't assume that the modi will always be attached to a device. But to simplify things,
 -- we'll assume that if a modi device attached intervals are greater than 30 min for a given hour then the modi is attached.

CREATE OR REPLACE TEMPORARY VIEW device_groups_ext AS
  --Grab only attached modi intervals, this we'll intersect these with our daily intervals table
  WITH modi_attached_intervals AS (
    SELECT org_id,
      device_id,
      start_ms AS attached_start_ms,
      end_ms AS attached_end_ms
    FROM data_analytics.vg_modi_attached_ranges
    WHERE attached_to_modi = true
    )


  SELECT
    a.gateway_id,
    a.device_id,
    a.org_id,
    a.cm_id,
    a.camera_serial,
    a.product_id,
    a.product_type,
    a.product_version,
    a.has_baxter,
    a.org_type,
    a.first_heartbeat_date,
    a.first_heartbeat_ms,
    a.last_heartbeat_date,
    a.last_heartbeat_ms,
    a.current_time,
    a.next_time,
    CASE
      WHEN
      sum
      (
        CASE
            WHEN b.attached_start_ms <= a.current_time AND b.attached_end_ms >= a.next_time THEN a.next_time - a.current_time
            WHEN b.attached_start_ms >= a.current_time AND b.attached_end_ms <= a.next_time THEN b.attached_end_ms - b.attached_start_ms
            WHEN b.attached_start_ms <= a.current_time AND b.attached_end_ms <= a.next_time THEN b.attached_end_ms - a.current_time
            WHEN b.attached_start_ms >= a.current_time AND b.attached_end_ms >= a.next_time THEN a.next_time - b.attached_start_ms
            ELSE 0
        END
      ) >= 30*60*1000 THEN true
      ELSE false
    END AS has_modi
  FROM device_hourly AS a
  LEFT JOIN modi_attached_intervals AS b ON
    a.org_id = b.org_id AND
    a.device_id = b.device_id AND
    NOT (b.attached_start_ms >= a.next_time OR b.attached_end_ms <= a.current_time)
  GROUP BY a.gateway_id,
    a.device_id,
    a.org_id,
    a.cm_id,
    a.camera_serial,
    a.product_id,
    a.product_type,
    a.product_version,
    a.has_baxter,
    a.org_type,
    a.first_heartbeat_date,
    a.first_heartbeat_ms,
    a.last_heartbeat_date,
    a.last_heartbeat_ms,
    a.current_time,
    a.next_time

-- COMMAND ----------

--Clean up null records
CREATE OR REPLACE TEMPORARY VIEW device_hourly_groups AS
(
  SELECT *
  FROM device_groups_ext
  WHERE first_heartbeat_ms <= current_time AND next_time IS NOT NULL
);

UNCACHE TABLE device_hourly_groups;
CACHE TABLE device_hourly_groups

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW hourly_vg_build AS (
  SELECT a.device_id AS vg_id,
    a.org_id,
    a.product_type,
    a.product_version,
    a.has_modi,
    a.has_baxter,
    a.org_type,
    a.current_time,
    a.next_time,
    a.first_heartbeat_date,
    a.last_heartbeat_date,
    min((b.time, b.value.proto_value.hub_server_device_heartbeat.connection.device_hello.build)).build AS vg_build
  FROM device_hourly_groups AS a
  LEFT JOIN kinesisstats.osdhubserverdeviceheartbeat AS b
    ON a.org_id = b.org_id AND
    a.device_id = b.object_id AND
    b.time >= a.current_time AND
    b.time <= a.current_time + 7*60*60*1000
  WHERE
    b.value.is_databreak = 'false' AND
    b.value.is_end = 'false' AND
    b.date >= (SELECT date FROM date_table LIMIT 1) --- turn this back on after backfill
  GROUP BY a.device_id,
    a.org_id,
    a.product_type,
    a.product_version,
    a.has_modi,
    a.has_baxter,
    a.org_type,
    a.current_time,
    a.next_time,
    a.first_heartbeat_date,
    a.last_heartbeat_date
);

UNCACHE TABLE hourly_vg_build;
CACHE TABLE hourly_vg_build

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW hourly_cm_build AS (
  SELECT a.device_id AS vg_id,
    a.org_id,
    a.product_type,
    a.product_version,
    a.has_modi,
    a.has_baxter,
    a.org_type,
    a.current_time,
    a.next_time,
    a.first_heartbeat_date,
    a.last_heartbeat_date,
    min((c.time, c.value.proto_value.hub_server_device_heartbeat.connection.device_hello.build)).build AS cm_build
  FROM device_hourly_groups AS a
  LEFT JOIN kinesisstats.osdhubserverdeviceheartbeat AS c
    ON a.org_id = c.org_id AND
    a.cm_id = c.object_id AND
    c.time >= a.current_time AND
    c.time <= a.current_time + 7*60*60*1000
  WHERE
    c.value.is_databreak = 'false' AND
    c.value.is_end = 'false' AND
    c.date >= (SELECT date FROM date_table LIMIT 1)  --- --- turn this back on after backfill
  GROUP BY a.device_id,
    a.org_id,
    a.product_type,
    a.product_version,
    a.has_modi,
    a.has_baxter,
    a.org_type,
    a.current_time,
    a.next_time,
    a.first_heartbeat_date,
    a.last_heartbeat_date
);

UNCACHE TABLE hourly_cm_build;
CACHE TABLE hourly_cm_build

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW hourly_vg_bootcount AS (
  SELECT a.device_id AS vg_id,
    a.org_id,
    a.product_type,
    a.product_version,
    a.has_modi,
    a.has_baxter,
    a.org_type,
    a.current_time,
    a.next_time,
    a.first_heartbeat_date,
    a.last_heartbeat_date,
    max(d.value.proto_value.hub_server_device_heartbeat.connection.device_hello.boot_count) - min(d.value.proto_value.hub_server_device_heartbeat.connection.device_hello.boot_count) vg_bootcount
  FROM device_hourly_groups AS a
  LEFT JOIN kinesisstats.osdhubserverdeviceheartbeat AS d ON
    a.org_id = d.org_id AND
    a.device_id = d.object_id AND
    d.time >= a.current_time AND d.time <= a.next_time
  WHERE
    d.value.is_databreak = 'false' AND
    d.value.is_end = 'false' AND
    d.date >= (SELECT date FROM date_table LIMIT 1)   --- turn this back on after backfill
  GROUP BY a.device_id,
    a.org_id,
    a.product_type,
    a.product_version,
    a.has_modi,
    a.has_baxter,
    a.org_type,
    a.current_time,
    a.next_time,
    a.first_heartbeat_date,
    a.last_heartbeat_date
);

UNCACHE TABLE hourly_vg_bootcount;
CACHE TABLE hourly_vg_bootcount


-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW hourly_ae_counts AS (
  SELECT a.device_id AS vg_id,
    a.org_id,
    a.product_type,
    a.product_version,
    a.has_modi,
    a.has_baxter,
    a.org_type,
    a.current_time,
    a.next_time,
    a.first_heartbeat_date,
    a.last_heartbeat_date,
    sum(CASE WHEN e.value.proto_value.anomaly_event.description LIKE '%oom%' THEN 1 ELSE 0 END) AS oom_errors,
    sum(CASE WHEN e.value.proto_value.anomaly_event.description LIKE '%kernel%' OR e.value.proto_value.anomaly_event.service_name LIKE '%kernel%' THEN 1 ELSE 0 END) AS kernel_error, -- all kernel errors
    sum(CASE WHEN e.value.proto_value.anomaly_event.service_name = 'kernel' THEN 1 ELSE 0 END) AS kernel_panics_only,
    sum(CASE WHEN e.value.proto_value.anomaly_event.service_name = 'kernel-panic-oom' THEN 1 ELSE 0 END) AS kernel_oom_only,
    count(e.value.proto_value.anomaly_event.description) AS all_errors,
    count(CASE WHEN e.value.proto_value.anomaly_event.description = 'spiComm: SPI transmit buffer exceeds maximum length in low-power mode and will be truncated' THEN e.value.proto_value.anomaly_event.description ELSE NULL END) AS SPI_transmit_buffer_error,
    count(CASE WHEN e.value.proto_value.anomaly_event.description = 'spiComm: Too many consecutive sequence number mismatches; communication lost while in normal power mode' THEN e.value.proto_value.anomaly_event.description ELSE NULL END) AS SPI_sequence_number_normal_power_error


  FROM device_hourly_groups AS a
  LEFT JOIN kinesisstats.osdanomalyevent AS e
    ON a.org_id = e.org_id AND
    a.device_id = e.object_id AND
    e.time >= a.current_time AND e.time <= a.next_time
  WHERE
    e.value.is_databreak = 'false' AND
    e.value.is_end = 'false' AND
    e.date >= (SELECT date FROM date_table LIMIT 1)   --- turn this back on after backfill
  GROUP BY a.device_id,
    a.org_id,
    a.product_type,
    a.product_version,
    a.has_modi,
    a.has_baxter,
    a.org_type,
    a.current_time,
    a.next_time,
    a.first_heartbeat_date,
    a.last_heartbeat_date
);

UNCACHE TABLE hourly_ae_counts;
CACHE TABLE hourly_ae_counts

-- COMMAND ----------

--Mem utilization
CREATE OR REPLACE TEMPORARY VIEW hourly_memory_util AS (
  SELECT a.device_id AS vg_id,
    a.org_id,
    a.product_type,
    a.product_version,
    a.has_modi,
    a.has_baxter,
    a.org_type,
    a.current_time,
    a.next_time,
    a.first_heartbeat_date,
    a.last_heartbeat_date,
    max(b.value.proto_value.system_stats.memory_util) AS memory_usage
  FROM device_hourly_groups AS a
  LEFT JOIN kinesisstats.osdsystemstats AS b
    ON a.org_id = b.org_id AND
    a.device_id = b.object_id AND
    b.time >= a.current_time AND b.time <= a.next_time
  WHERE
    b.value.is_databreak = 'false' AND
    b.value.is_end = 'false' AND
    b.date >= (SELECT date FROM date_table LIMIT 1)   --- turn this back on after backfill
  GROUP BY a.device_id,
    a.org_id,
    a.product_type,
    a.product_version,
    a.has_modi,
    a.has_baxter,
    a.org_type,
    a.current_time,
    a.next_time,
    a.first_heartbeat_date,
    a.last_heartbeat_date
);

UNCACHE TABLE hourly_memory_util;
CACHE TABLE hourly_memory_util

-- COMMAND ----------

-- This objectstate is reported on change everytime the data usage exceeds 1mb since last reported.
CREATE OR REPLACE TEMPORARY VIEW hourly_data_usage_metrics AS (
  SELECT a.device_id AS vg_id,
    a.org_id,
    a.product_type,
    a.product_version,
    a.org_type,
    a.has_modi,
    a.has_baxter,
    a.current_time,
    a.next_time,
    a.first_heartbeat_date,
    a.last_heartbeat_date,
    sum(value.int_value) / 1000000000 AS total_data_usage_gb
  FROM device_hourly_groups AS a
  LEFT JOIN kinesisstats.osdcellbytes AS b ON
    a.org_id = b.org_id AND
    a.device_id = b.object_id AND
    b.time >= a.current_time AND
    b.time <= a.next_time
  WHERE
    b.value.is_databreak = 'false' AND
    b.value.is_end = 'false' AND
    b.date >= (SELECT date FROM date_table LIMIT 1) --- turn this back on after backfill
  GROUP BY
    a.device_id,
    a.org_id,
    a.product_type,
    a.product_version,
    a.org_type,
    a.has_modi,
    a.has_baxter,
    a.current_time,
    a.next_time,
    a.first_heartbeat_date,
    a.last_heartbeat_date
);

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW hourly_fw_metrics AS (
  SELECT a.device_id AS vg_id,
    a.org_id,
    a.product_type,
    a.product_version,
    coalesce(try_cast(rollout_stages.product_program_id AS INT), -1) AS product_program_id, -- assign -1 if "override"
    rollout_stages.product_program_id_type,
    a.has_modi,
    a.has_baxter,
    CASE
      WHEN octo.linked_octo_id IS NULL
      THEN FALSE
      ELSE TRUE
    END AS has_octo,
    a.org_type,
    date(timestamp(a.current_time / 1000)) AS date,
    a.current_time,
    a.next_time,
    NULL AS org_rollout_stage, -- deprecated
    NULL AS gateway_rollout_stage, -- deprecated
    coalesce(try_cast(rollout_stages.rollout_stage_id AS INT), -1) AS rollout_stage, -- assign -1 if "override"
    b.vg_build,
    c.cm_build,
    d.vg_bootcount,
    e.oom_errors,
    e.kernel_error,
    e.kernel_panics_only,
    e.kernel_oom_only,
    e.all_errors,
    e.SPI_transmit_buffer_error,
    e.SPI_sequence_number_normal_power_error,
    f.memory_usage,
    g.total_data_usage_gb,
    a.first_heartbeat_date,
    a.last_heartbeat_date
  FROM device_hourly_groups AS a
  LEFT JOIN hourly_vg_build AS b ON
    a.device_id = b.vg_id AND
    a.org_id = b.org_id AND
    a.current_time = b.current_time
  LEFT JOIN hourly_cm_build AS c ON
    a.device_id = c.vg_id AND
    a.org_id = c.org_id AND
    a.current_time = c.current_time
  LEFT JOIN hourly_vg_bootcount AS d ON
    a.device_id = d.vg_id AND
    a.org_id = d.org_id AND
    a.current_time = d.current_time
  LEFT JOIN hourly_ae_counts AS e ON
    a.device_id = e.vg_id AND
    a.org_id = e.org_id AND
    a.current_time = e.current_time
  LEFT JOIN hourly_memory_util AS f ON
    a.device_id = f.vg_id AND
    a.org_id = f.org_id AND
    a.current_time = f.current_time
  LEFT JOIN hourly_data_usage_metrics AS g ON
    a.device_id = g.vg_id AND
    a.org_id = g.org_id AND
    a.current_time = g.current_time
  LEFT JOIN data_analytics.octo_linked_vgs octo ON ------------------- octo line
     a.device_id = octo.vg_device_id
     AND a.org_id = octo.org_id
     AND a.current_time BETWEEN octo.start_at_ms AND coalesce(octo.end_at_ms, unix_millis(to_timestamp(now()))) ----------- put octo to those dates only when octo was active
  LEFT JOIN dataprep_firmware.gateway_daily_rollout_stages AS rollout_stages ON
    a.gateway_id = rollout_stages.gateway_id
    AND a.org_id = rollout_stages.org_id
    AND a.product_id = rollout_stages.product_id
    AND date(timestamp(a.current_time / 1000)) = rollout_stages.date
);

UNCACHE TABLE hourly_fw_metrics;
CACHE TABLE hourly_fw_metrics

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW updates AS (
  SELECT * FROM hourly_fw_metrics
  WHERE date >= (SELECT date FROM date_table LIMIT 1)
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dataprep_firmware.vg_release_metrics
USING DELTA
PARTITIONED BY (date)
AS SELECT * FROM updates

-- COMMAND ----------

ALTER TABLE dataprep_firmware.vg_release_metrics SET TBLPROPERTIES ('comment' = 'Contains hourly release qualifier metrics for all VG products. All date is derived between the current_time and next_time unless otherwise specified');
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE vg_id COMMENT 'ID of the Samsara device for the VG';
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE org_id COMMENT 'ID of the Samsara organization';
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE product_type COMMENT 'Combination of VG short name and associated CM product, if any';
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE product_version COMMENT 'Hardware product version number which denotes hardware differences requiring firmware support';
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE product_program_id COMMENT 'The program ID the device was assigned to, or -1 if an org or device firmware override is applied'; -- assign -1 if "override"
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE product_program_id_type COMMENT 'The method of enrollment for the assigned program ID';
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE has_modi COMMENT 'Whether the device had a Modi diagnostic hub (ACC-BDH or ACC-BDHE) attached';
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE has_baxter COMMENT 'Whether the device had a Baxter camera connector (ACC-CM-ANLG) attached';
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE has_octo COMMENT 'Whether the device had an Octo camera connector (HW-CM-AHD1) attached';
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE org_type COMMENT 'The type of organization the device belongs to (customer, internal)';
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE date COMMENT 'The UTC date over which the metrics were derived';
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE current_time COMMENT 'The starting UTC timestamp in milliseconds over which the metrics were derived';
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE next_time COMMENT 'The ending UTC timestamp in milliseconds over which the metrics were derived';
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE org_rollout_stage COMMENT '[DEPRECATED] DO NOT USE. The rollout stage enrollment for the org';
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE gateway_rollout_stage COMMENT '[DEPRECATED] DO NOT USE. The rollout stage enrollment for the device';
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE rollout_stage COMMENT 'The derived rollout stage for the device, respecting the hierarchical rules of enrollment. -1 if the device or org had a firmware override applied'; -- assign -1 if "override"
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE vg_build COMMENT 'The build string of the firmware running on the VG';
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE cm_build COMMENT 'The build string of the firmware running on the associated CM, if any';
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE vg_bootcount COMMENT 'The boot count on the VG (number of times the VG has rebooted)';
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE oom_errors COMMENT 'The number of out of memory (OOM) related anomalies experienced by the VG';
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE kernel_error COMMENT 'The number of kernel-related anomalies experienced by the VG';
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE kernel_panics_only COMMENT 'The number of anomalies from the kernel service experienced by the VG';
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE kernel_oom_only COMMENT 'The number of anomalies from the kernel-panic-oom service experienced by the VG';
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE all_errors COMMENT 'The total number of anomalies experienced by the VG';
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE SPI_transmit_buffer_error COMMENT 'The number of "spiComm: SPI transmit buffer exceeds maximum length in low-power mode and will be truncated" anomalies experienced by the VG';
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE SPI_sequence_number_normal_power_error COMMENT 'The number of "spiComm: Too many consecutive sequence number mismatches; communication lost while in normal power mode" anomalies experienced by the VG';
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE memory_usage COMMENT 'The max amount of system memory usage reported by the VG';
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE total_data_usage_gb COMMENT 'The total amount of cellular data usage reported by the VG';
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE first_heartbeat_date COMMENT 'The date the device first connected to the hub server';
ALTER TABLE dataprep_firmware.vg_release_metrics CHANGE last_heartbeat_date COMMENT 'The date the device last connected to the hub server';

-- COMMAND ----------

-- Using dynamic partition overwrite mode means we don't need to specify which partitions to overwrite when using INSERT OVERWRITE
-- More info: https://spark.apache.org/docs/latest/configuration.html (search spark.sql.sources.partitionOverwriteMode)
SET spark.sql.sources.partitionOverwriteMode = dynamic;

-- COMMAND ----------

INSERT OVERWRITE TABLE dataprep_firmware.vg_release_metrics
SELECT * FROM updates;
