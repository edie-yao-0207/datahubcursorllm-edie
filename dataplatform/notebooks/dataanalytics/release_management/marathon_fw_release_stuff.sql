-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Release

-- COMMAND ----------

-- Active AG51, AG52, AG53, AG51-EU, AG52-EU, AG53-EU that were active on a given day in the last 7 days.

CREATE OR REPLACE TEMP VIEW devices AS (
  SELECT
    ad.date,
    g.org_id,
    o.internal_type as org_type,
    g.device_id,
    g.id AS gateway_id,
    g.product_id,
    p.name AS gateway_type
  FROM clouddb.gateways g
  JOIN clouddb.organizations o
  ON g.org_id = o.id
  JOIN dataprep.active_devices ad
  ON g.org_id = ad.org_id
    AND g.device_id = ad.device_id
    AND g.product_id = ad.product_id
  LEFT JOIN definitions.products p
  ON g.product_id = p.product_id
  WHERE g.product_id IN (124,125,142,143,140,144)
    AND ad.active_heartbeat IS TRUE
    AND ad.date >= DATE_SUB(CURRENT_DATE(),30)
    AND g.org_id != 1
--     AND o.internal_type <> 1
);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW boots_and_build_rank AS (
  SELECT
    date,
    org_id,
    object_id,
    time,
    value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id,
    value.proto_value.hub_server_device_heartbeat.connection.device_hello.boot_count,
    value.proto_value.hub_server_device_heartbeat.connection.device_hello.build,
    ROW_NUMBER() OVER(PARTITION BY date, org_id, object_id ORDER BY time DESC) AS rnk
  FROM kinesisstats.osdhubserverdeviceheartbeat
  WHERE date >= DATE_SUB(CURRENT_DATE(),30)
);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW boots_and_builds AS (
  SELECT
    d.date,
    d.org_id,
    d.org_type,
    d.device_id,
    d.gateway_id,
    br.gateway_id AS gateway_id_from_hb,
    d.product_id,
    d.gateway_type,
    br.boot_count AS curr_boot_count,
    LAG(br.boot_count) OVER(PARTITION BY br.org_id, br.object_id ORDER BY br.date) AS prev_boot_count,
    br.build
  FROM devices d
  LEFT JOIN boots_and_build_rank br
  ON d.date = br.date
    AND d.org_id = br.org_id
    AND d.device_id = br.object_id
    AND br.rnk = 1
);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW rollout_stages AS (
  SELECT
    bab.date,
    bab.org_id,
    bab.org_type,
    bab.device_id,
    bab.gateway_id,
    bab.product_id,
    bab.gateway_type,
    COALESCE(bab.curr_boot_count - bab.prev_boot_count, 0) AS boot_count,
    bab.build as build,
    grs.rollout_stage_id AS rollout_stage,
    COALESCE(gpors.rollout_stage_id, opors.rollout_stage_id, pgrsbo.rollout_stage_id) AS org_rollout_stage
  FROM boots_and_builds bab
  LEFT JOIN firmwaredb.gateway_rollout_stages grs
  ON bab.org_id = grs.org_id
    AND bab.gateway_id = grs.gateway_id
    AND bab.product_id = grs.product_id
  LEFT JOIN firmwaredb.gateway_program_override_rollout_stages gpors
  ON bab.org_id = gpors.org_id
    AND bab.gateway_id = gpors.gateway_id
    AND bab.product_id = gpors.product_id
  LEFT JOIN firmwaredb.org_program_override_rollout_stages opors
  ON bab.org_id = opors.org_id
    AND bab.product_id = opors.product_id
  LEFT JOIN firmwaredb.product_group_rollout_stage_by_org pgrsbo
  ON bab.org_id = pgrsbo.org_id
    AND pgrsbo.product_group_id = 2
);

-- COMMAND ----------

CREATE OR REPLACE TABLE firmware.ag5x_rollout_stages USING DELTA (
  SELECT *
  FROM rollout_stages
);

-- COMMAND ----------

SELECT
  date,
  rollout_stage,
  COUNT(gateway_id) AS num_gateways
FROM firmware.ag5x_rollout_stages
GROUP BY 1,2
ORDER BY 1,2

limit 1000

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Stablity

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW resets AS (
  SELECT
    rs.date,
    rs.org_id,
    rs.org_type,
    rs.device_id,
    rs.gateway_id,
    rs.product_id,
    rs.gateway_type,
    rs.build,
    rs.rollout_stage,
    rs.org_rollout_stage,
    onri.value.proto_value.nordic_reset_info.soc,
    onri.value.proto_value.nordic_reset_info.software_reason, -- Watchdog reset reason ID = 5
    onri.value.proto_value.nordic_reset_info.hardware_reason
  FROM firmware.ag5x_rollout_stages rs
  LEFT JOIN kinesisstats.osdnordicresetinfo onri
  ON rs.date = onri.date
    AND rs.org_id = onri.org_id
    AND rs.device_id = onri.object_id
);

-- COMMAND ----------

CREATE OR REPLACE TABLE firmware.ag5x_resets USING DELTA (
  SELECT *
  FROM resets
);

-- COMMAND ----------

SELECT * FROM firmware.ag5x_resets
limit 100

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## GPS

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW gps AS (
  SELECT
    rs.date,
    rs.org_id,
    rs.org_type,
    rs.device_id,
    rs.gateway_id,
    rs.product_id,
    rs.gateway_type,
    rs.build,
    rs.rollout_stage,
    rs.org_rollout_stage,
    ongd.time,
    ongd.value.is_start,
    ongd.value.is_end,
    ongd.value.is_databreak,
    ongd.value.received_delta_seconds,
    ongd.value.proto_value.nordic_gps_debug.gps_fix_info.latitude_nd AS lat,
    ongd.value.proto_value.nordic_gps_debug.gps_fix_info.longitude_nd AS long,
    ongd.value.proto_value.nordic_gps_debug.gps_fix_info.time_to_fix_ms AS time_to_fix,
    ongd.value.proto_value.nordic_gps_debug.gps_fix_info.execution_time_ms AS execution_time,
    ongd.value.proto_value.nordic_gps_debug.gps_fix_info.fix_index AS fix_index,
    ongd.value.proto_value.nordic_gps_debug.no_fix AS no_fix,
    ongd.value.proto_value.nordic_gps_debug.gps_scan_duration_ms AS gps_scan_duration_ms,
    ongd.value.proto_value.nordic_gps_debug.reported_to_backend AS reported_to_backend,
    ongd.value.proto_value.nordic_gps_debug.filter.num_filtered_fixes AS num_filtered_fixes,
    ongd.value.proto_value.nordic_gps_debug.filter.time_to_first_filtered_fix_ms AS time_to_first_filtered_fix_ms,
    ongd.value.proto_value.nordic_gps_debug.filter.reason AS reason
  FROM firmware.ag5x_rollout_stages rs
  LEFT JOIN kinesisstats.osdnordicgpsdebug ongd
    ON rs.date = ongd.date
    AND rs.org_id = ongd.org_id
    AND rs.device_id = ongd.object_id
  WHERE rs.date >= DATE_SUB(CURRENT_DATE(),30) -- Limit to last 30 days
);

-- COMMAND ----------

CREATE OR REPLACE TABLE firmware.ag5x_gps USING DELTA (
  SELECT *
  FROM gps
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Connectivity

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW connectivity AS (
  SELECT
    rs.date,
    rs.org_id,
    rs.org_type,
    rs.device_id,
    rs.gateway_id,
    rs.product_id,
    rs.gateway_type,
    rs.build,
    rs.rollout_stage,
    rs.org_rollout_stage,
    value.time,
    value.received_delta_seconds,
    value.proto_value.nordic_lte_debug_connection_status.connection_type,
    value.proto_value.nordic_lte_debug_connection_status.rrc_connected_duration_ms
  FROM firmware.ag5x_rollout_stages rs
  LEFT JOIN kinesisstats.osdnordicltedebugconnectionstatus onldcs
    ON rs.date = onldcs.date
    AND rs.org_id = onldcs.org_id
    AND rs.device_id = onldcs.object_id
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW lte_registrations AS (
  SELECT
    rs.date,
    rs.org_id,
    rs.org_type,
    rs.device_id,
    rs.gateway_id,
    rs.product_id,
    rs.gateway_type,
    rs.build,
    rs.rollout_stage,
    rs.org_rollout_stage,
    CASE WHEN onld.value.proto_value.nordic_lte_debug.lte_cell_info IS NOT NULL THEN TRUE
         ELSE FALSE END AS lte_reg
  FROM firmware.ag5x_rollout_stages rs
  LEFT JOIN kinesisstats.osdnordicltedebug onld
  ON rs.date = onld.date
  AND rs.org_id = onld.org_id
  AND rs.device_id = onld.object_id
)

-- WITH ttab AS (
--   SELECT
--     d.device_id,
--     d.product_id,
--     d.org_id
--   FROM (
--     SELECT
--       id AS device_id,
--       org_id,
--       product_id
--     FROM clouddb.devices
--     WHERE product_id IN (124,125,142,143)) as d
--     JOIN clouddb.organizations AS o
--       ON d.org_id = o.id
--     WHERE o.internal_type <> 1), lte as (SELECT lte.lte_reg, count(1) as cnt
--           FROM (SELECT date, object_id,
--                         CASE WHEN value.proto_value.nordic_lte_debug.lte_cell_info IS NOT NULL THEN true ELSE false END AS lte_reg
--                 FROM kinesisstats.osdnordicltedebug as
--                 WHERE date >= '{w2_start_date.strftime('%Y-%m-%d')}' AND date <= '{w2_end_date.strftime('%Y-%m-%d')}') AS lte
--           INNER JOIN ttab ON
--           ttab.device_id = lte.object_id
--           GROUP BY lte.lte_reg)
--           SELECT (success *100/(success+  (case when lfail is null then 0 else lfail end)  )) AS `Success Registration` FROM lte
--           PIVOT (SUM(cnt) AS c
--                  FOR lte_reg IN (true AS success, false AS lfail))

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW data_usage AS (
  SELECT
    rs.date,
    rs.org_id,
    rs.org_type,
    rs.device_id,
    rs.gateway_id,
    rs.product_id,
    rs.gateway_type,
    rs.build,
    rs.rollout_stage,
    rs.org_rollout_stage,
    vdu.data_usage
  FROM firmware.ag5x_rollout_stages rs
  LEFT JOIN clouddb.devices d
  ON rs.org_id = d.org_id
    AND rs.device_id = d.id
  LEFT JOIN dataprep_cellular.vodafone_daily_usage vdu
  ON rs.date = vdu.day
    AND d.iccid = vdu.iccid
)

-- COMMAND ----------

SELECT * FROM data_usage LIMIT 100

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Widgets

-- COMMAND ----------

-- Grab all widgets, activated by customer or not

CREATE OR REPLACE TEMP VIEW widgets AS (
  SELECT
    w.product_id,
    w.id AS widget_id,
    w.serial,
    w.name,
    pinned_device_id
  FROM clouddb.widgets AS w
);

-- COMMAND ----------

-- Grab date widget was first and last connected during time range

CREATE OR REPLACE TEMP VIEW first_last_connected AS (
  SELECT
    date,
    org_id,
    object_id AS widget_id,
    stat_type,
    object_type,
    time,
    value.int_value AS device_id,
    value.is_start,
    value.is_end,
    value.is_databreak,
    value.received_delta_seconds
  FROM kinesisstats.oswconndevicerssi
  WHERE date >= DATE_SUB(CURRENT_DATE(),30) -- Limit to last 30 days
);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW rs_widget_devices AS (
  SELECT
    rs.date,
    rs.org_id,
    rs.org_type,
    rs.device_id,
    rs.gateway_id,
    rs.product_id,
    rs.gateway_type,
    rs.build,
    rs.rollout_stage,
    rs.org_rollout_stage,
    flc.widget_id,
    pw.name AS widget_sku,
    flc.stat_type,
    flc.object_type,
    flc.time,
    flc.is_start,
    flc.is_end,
    flc.is_databreak,
    flc.received_delta_seconds
  FROM firmware.ag5x_rollout_stages rs
  LEFT JOIN first_last_connected flc
    ON rs.date = flc.date
      AND rs.org_id = flc.org_id
      AND rs.device_id = flc.device_id
  LEFT JOIN clouddb.widgets AS w
    ON flc.widget_id = w.id
  LEFT JOIN definitions.products AS pw
    ON w.product_id = pw.product_id
)

-- COMMAND ----------

CREATE OR REPLACE TABLE firmware.ag5x_widgets USING DELTA (
  SELECT *
  FROM rs_widget_devices
);

-- COMMAND ----------

WITH t AS (
  SELECT
    date,
    device_id,
    widget_id,
    time,
    is_start,
    is_end,
    is_databreak
  FROM firmware.ag5x_widgets
  WHERE (is_start IS TRUE AND is_end IS FALSE)
    OR (is_start IS FALSE AND is_end IS TRUE)
  ORDER BY 1,2,3,4
)

SELECT
  *
FROM t
WHERE
