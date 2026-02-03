-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text('org_id', '8936')
-- MAGIC dbutils.widgets.text('start_date', '2021-01-01')
-- MAGIC dbutils.widgets.text('end_date', '2021-01-10')
-- MAGIC dbutils.widgets.text('device_id', '0')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Wifi Analysis Notebook
-- MAGIC (Milestone 1 from https://paper.dropbox.com/doc/PRD-Reduce-quantitycriticality-of-Wifi-Hotspot-related-support-issues--BDlsbSCc_8YcG2mdL2zaJ0J~Ag-Wi0Tcr226BDJBaOncPGbN)
-- MAGIC
-- MAGIC This notebook should help provide insight into organization and device wifi usage.
-- MAGIC
-- MAGIC ## How To Use
-- MAGIC
-- MAGIC 1. Clone this notebook
-- MAGIC 2. Run CMD 1 ONLY. It sets up the widgets.
-- MAGIC 3. Fill in `start_date`, `end_date`, `org_id` and `device_id` on the top bar
-- MAGIC 4. Hit `Run All` (its ok to re-run command 1 now, it won't overwrite your set values)
-- MAGIC 5. Scroll to see the results. Each section will be annotated.
-- MAGIC
-- MAGIC
-- MAGIC ## Questions?
-- MAGIC Please post in #ask-fleet-foundations if you have any questions.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Setup
-- MAGIC No need to modify any of this

-- COMMAND ----------

-- MAGIC %python
-- MAGIC org_id = getArgument('org_id')
-- MAGIC start_date = getArgument('start_date')
-- MAGIC end_date = getArgument('end_date')
-- MAGIC device_id = getArgument('device_id')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #
-- MAGIC # Calculate daily usage per device.
-- MAGIC #
-- MAGIC spark.sql(f"""
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW devices_daily_usage AS
-- MAGIC WITH metered_usage AS (
-- MAGIC   SELECT
-- MAGIC     object_id AS device_id,
-- MAGIC     date,
-- MAGIC     SUM(value.int_value) / (1024*1024) AS metered_usage_mb
-- MAGIC   FROM kinesisstats.osdwifiapbytes
-- MAGIC   WHERE date >= "{start_date}" AND date <= "{end_date}"
-- MAGIC   AND org_id = {org_id}
-- MAGIC   GROUP BY 1, 2
-- MAGIC ),
-- MAGIC
-- MAGIC whitelist_usage AS (
-- MAGIC   SELECT
-- MAGIC     object_id AS device_id,
-- MAGIC     date,
-- MAGIC     SUM(value.int_value) / (1024*1024) AS whitelist_usage_mb
-- MAGIC   FROM kinesisstats.osdwifiapwhitelistbytes
-- MAGIC   WHERE date >= "{start_date}" AND date <= "{end_date}"
-- MAGIC   AND org_id = {org_id}
-- MAGIC   GROUP BY 1, 2
-- MAGIC ),
-- MAGIC
-- MAGIC devices AS (
-- MAGIC   SELECT id AS device_id, name
-- MAGIC   FROM productsdb.devices
-- MAGIC   WHERE org_id = {org_id}
-- MAGIC   AND product_id IN (24, 35, 53, 89, 90) -- vg34, vg34eu, vg54, vg54eu, vg34fn
-- MAGIC )
-- MAGIC
-- MAGIC -- IMPROVEMENT: create a table of all dates in the range as the basis for the left join
-- MAGIC SELECT date, device_id, name, metered_usage_mb, whitelist_usage_mb
-- MAGIC FROM devices
-- MAGIC LEFT JOIN metered_usage
-- MAGIC   USING(device_id)
-- MAGIC FULL OUTER JOIN whitelist_usage
-- MAGIC   USING(device_id, date)
-- MAGIC WHERE date IS NOT NULL
-- MAGIC """)
-- MAGIC
-- MAGIC spark.sql("CACHE TABLE devices_daily_usage")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #
-- MAGIC # For each device+place, calculate usage data.
-- MAGIC # Place data is unfortunately not that useful, but temporarily keeping it this way
-- MAGIC # until we decide to try to put address info in here.
-- MAGIC #
-- MAGIC spark.sql(f"""
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW device_location_usage AS
-- MAGIC WITH stopped_intervals AS (
-- MAGIC   SELECT
-- MAGIC     device_id,
-- MAGIC     proto.end.place AS place,
-- MAGIC     proto.end.time AS start_time,
-- MAGIC     LEAD(proto.start.time) OVER org_device_window AS end_time
-- MAGIC   FROM trips2db_shards.trips
-- MAGIC   WHERE date >= "{start_date}" AND date <= "{end_date}"
-- MAGIC   AND org_id = {org_id}
-- MAGIC   WINDOW org_device_window AS (PARTITION BY device_id ORDER BY proto.start.time ASC)
-- MAGIC ),
-- MAGIC
-- MAGIC metered_usage_mb AS (
-- MAGIC   SELECT
-- MAGIC     ks.device_id,
-- MAGIC     si.place,
-- MAGIC     SUM(bytes) / (1024*1024) AS metered_usage_mb
-- MAGIC   FROM (
-- MAGIC     SELECT object_id AS device_id, time, value.int_value AS bytes
-- MAGIC     FROM kinesisstats.osdwifiapbytes
-- MAGIC     WHERE date >= "{start_date}" AND date <= "{end_date}"
-- MAGIC     AND org_id = {org_id}
-- MAGIC   ) AS ks
-- MAGIC   LEFT JOIN stopped_intervals AS si
-- MAGIC     ON ks.device_id = si.device_id
-- MAGIC     AND si.start_time <= ks.time AND si.end_time >= ks.time
-- MAGIC   GROUP BY 1, 2
-- MAGIC ),
-- MAGIC
-- MAGIC whitelist_usage_mb AS (
-- MAGIC   SELECT
-- MAGIC     ks.device_id,
-- MAGIC     si.place,
-- MAGIC     SUM(bytes) / (1024*1024) AS whitelist_usage_mb
-- MAGIC   FROM (
-- MAGIC     SELECT org_id, object_id AS device_id, date, time, value.int_value AS bytes
-- MAGIC     FROM kinesisstats.osdwifiapwhitelistbytes
-- MAGIC     WHERE date >= "{start_date}" AND date <= "{end_date}"
-- MAGIC     AND org_id = {org_id}
-- MAGIC   ) AS ks
-- MAGIC   LEFT JOIN stopped_intervals AS si
-- MAGIC     ON ks.device_id = si.device_id
-- MAGIC     AND si.start_time <= ks.time AND si.end_time >= ks.time
-- MAGIC   GROUP BY 1, 2
-- MAGIC )
-- MAGIC
-- MAGIC
-- MAGIC SELECT device_id, place, metered_usage_mb, whitelist_usage_mb
-- MAGIC FROM metered_usage_mb
-- MAGIC FULL OUTER JOIN whitelist_usage_mb
-- MAGIC   USING(device_id, place)""")
-- MAGIC
-- MAGIC spark.sql("CACHE TABLE device_location_usage")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #
-- MAGIC # For device+driver (including `null` for driver meaning unassigned), calculate usage data.
-- MAGIC #
-- MAGIC spark.sql(f"""
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW device_drivers_usage AS
-- MAGIC WITH driver_intervals AS (
-- MAGIC   SELECT
-- MAGIC     device_id,
-- MAGIC     driver_id,
-- MAGIC     proto.start.time AS start_time,
-- MAGIC     CASE
-- MAGIC       WHEN (LEAD(driver_id) OVER org_device_window) = driver_id
-- MAGIC         THEN LEAD(proto.start.time) OVER org_device_window
-- MAGIC       ELSE proto.end.time
-- MAGIC     END AS end_time
-- MAGIC   FROM trips2db_shards.trips
-- MAGIC   WHERE date >= "{start_date}" AND date <= "{end_date}"
-- MAGIC   AND org_id = {org_id}
-- MAGIC   WINDOW org_device_window AS (PARTITION BY device_id ORDER BY proto.start.time ASC)
-- MAGIC ),
-- MAGIC
-- MAGIC device_distances AS (
-- MAGIC   SELECT
-- MAGIC     device_id,
-- MAGIC     driver_id,
-- MAGIC     SUM(proto.trip_distance.distance_meters) AS distance_m
-- MAGIC   FROM trips2db_shards.trips
-- MAGIC   WHERE date >= "{start_date}" AND date <= "{end_date}"
-- MAGIC   AND org_id = {org_id}
-- MAGIC   GROUP BY 1, 2
-- MAGIC ),
-- MAGIC
-- MAGIC metered_usage_mb AS (
-- MAGIC   SELECT
-- MAGIC     ks.device_id,
-- MAGIC     di.driver_id,
-- MAGIC     SUM(bytes) / (1024*1024) AS metered_usage_mb
-- MAGIC   FROM (
-- MAGIC     SELECT object_id AS device_id, time, value.int_value AS bytes
-- MAGIC     FROM kinesisstats.osdwifiapbytes
-- MAGIC     WHERE date >= "{start_date}" AND date <= "{end_date}"
-- MAGIC     AND org_id = {org_id}
-- MAGIC   ) AS ks
-- MAGIC   LEFT JOIN driver_intervals AS di
-- MAGIC     ON ks.device_id = di.device_id
-- MAGIC     AND di.start_time <= ks.time AND di.end_time >= ks.time
-- MAGIC   GROUP BY 1, 2
-- MAGIC ),
-- MAGIC
-- MAGIC whitelist_usage_mb AS (
-- MAGIC   SELECT
-- MAGIC     ks.device_id,
-- MAGIC     di.driver_id,
-- MAGIC     SUM(bytes) / (1024*1024) AS whitelist_usage_mb
-- MAGIC   FROM (
-- MAGIC     SELECT org_id, object_id AS device_id, date, time, value.int_value AS bytes
-- MAGIC     FROM kinesisstats.osdwifiapwhitelistbytes
-- MAGIC     WHERE date >= "{start_date}" AND date <= "{end_date}"
-- MAGIC     AND org_id = {org_id}
-- MAGIC   ) AS ks
-- MAGIC   LEFT JOIN driver_intervals AS di
-- MAGIC     ON ks.device_id = di.device_id
-- MAGIC     AND di.start_time <= ks.time AND di.end_time >= ks.time
-- MAGIC   GROUP BY 1, 2
-- MAGIC )
-- MAGIC
-- MAGIC
-- MAGIC SELECT device_id, driver_id, metered_usage_mb, whitelist_usage_mb, distance_m
-- MAGIC FROM metered_usage_mb
-- MAGIC FULL OUTER JOIN whitelist_usage_mb
-- MAGIC   USING(device_id, driver_id)
-- MAGIC LEFT JOIN device_distances
-- MAGIC   USING(device_id, driver_id)
-- MAGIC """)
-- MAGIC
-- MAGIC spark.sql("CACHE TABLE device_drivers_usage")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #
-- MAGIC # For a device+driver+hos status, see usage data.
-- MAGIC #
-- MAGIC spark.sql(f"""
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW device_hos_usage AS
-- MAGIC WITH hos_intervals AS (
-- MAGIC   SELECT
-- MAGIC     date,
-- MAGIC     driver_id,
-- MAGIC     vehicle_id AS device_id,
-- MAGIC     unix_timestamp(LAG(created_at) OVER w) * 1000 AS start_time,
-- MAGIC     unix_timestamp(created_at) * 1000 AS end_time,
-- MAGIC     LAG(status_code) OVER w AS status_code
-- MAGIC   FROM compliancedb_shards.driver_hos_logs
-- MAGIC   WHERE date >= "{start_date}" AND date <= "{end_date}" AND org_id = {org_id}
-- MAGIC   AND log_proto.active_to_inactive_changed_at_ms IS NULL
-- MAGIC   WINDOW w AS (PARTITION BY org_id, vehicle_id ORDER BY created_at ASC)
-- MAGIC ),
-- MAGIC
-- MAGIC hos_statuses AS (
-- MAGIC   SELECT col.col1 AS status_code, col.col2 AS status_name
-- MAGIC   FROM (
-- MAGIC     SELECT explode(
-- MAGIC       array(
-- MAGIC         (0, "offduty"),
-- MAGIC         (1, "sleeper"),
-- MAGIC         (2, "driving"),
-- MAGIC         (3, "onduty"),
-- MAGIC         (4, "autoduty"),
-- MAGIC         (5, "yardmove"),
-- MAGIC         (6, "personalconveyance"),
-- MAGIC         (7, "disconnected"),
-- MAGIC         (8, "waitingtime"),
-- MAGIC         (9, "intermediate")
-- MAGIC       )
-- MAGIC     )
-- MAGIC   )
-- MAGIC )
-- MAGIC
-- MAGIC SELECT
-- MAGIC   ks.date,
-- MAGIC   ks.device_id,
-- MAGIC   hi.driver_id,
-- MAGIC   hi.status_code,
-- MAGIC   status_name,
-- MAGIC   SUM(bytes) / (1024*1024) AS usage_mb
-- MAGIC FROM (
-- MAGIC   SELECT org_id, object_id AS device_id, date, time, value.int_value AS bytes
-- MAGIC   FROM kinesisstats.osdwifiapbytes
-- MAGIC   WHERE date >= "{start_date}" AND date <= "{end_date}" AND org_id = {org_id}
-- MAGIC ) AS ks
-- MAGIC LEFT JOIN hos_intervals AS hi
-- MAGIC   ON ks.device_id = hi.device_id
-- MAGIC   AND hi.start_time <= ks.time AND hi.end_time >= ks.time
-- MAGIC LEFT JOIN hos_statuses
-- MAGIC   USING(status_code)
-- MAGIC GROUP BY 1, 2, 3, 4, 5""")
-- MAGIC
-- MAGIC spark.sql("CACHE TABLE device_hos_usage")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Org Level Metrics

-- COMMAND ----------

-- DBTITLE 1,Usage Over Time
SELECT
  date,
  SUM(metered_usage_mb) / 1024 AS metered_usage_gb,
  SUM(whitelist_usage_mb) / 1024 AS whitelist_usage_gb
FROM devices_daily_usage
GROUP BY 1
ORDER BY 1 ASC

-- COMMAND ----------

-- DBTITLE 1,Statistics Per Device
WITH ap_client_count AS (
  SELECT
    object_id AS device_id,
    MAX(size(value.proto_value.ap_clients.ap_client)) AS max_connected_clients
  FROM kinesisstats.osdapclients
  WHERE date >= getArgument('start_date') AND date <= getArgument('end_date') AND org_id = getArgument('org_id')
  GROUP BY 1
),

usage AS (
  SELECT
    device_id,
    SUM(metered_usage_mb) / 1024 AS metered_usage_gb,
    SUM(whitelist_usage_mb) / 1024 AS whitelist_usage_gb,
    SUM(distance_m) / 1609 AS miles_traveled,
    collect_set(driver_id) AS all_drivers
  FROM device_drivers_usage
  GROUP BY 1
)

SELECT
  device_id,
  metered_usage_gb,
  whitelist_usage_gb,
  miles_traveled,
  max_connected_clients,
  all_drivers
FROM usage
LEFT JOIN ap_client_count
  USING(device_id)
ORDER BY metered_usage_gb DESC

-- COMMAND ----------

-- DBTITLE 1,Statistics Per Driver
-- TODO during stopped periods, we guess who the driver was based on if the previous/next trip are both the same driver
SELECT
  driver_id,
  SUM(metered_usage_mb) / 1024 AS metered_usage_gb,
  SUM(whitelist_usage_mb) / 1024 AS whitelist_usage_gb,
  SUM(distance_m) / 1609 AS miles_traveled,
  collect_set(device_id) AS devices_used
FROM device_drivers_usage
WHERE driver_id IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC

-- COMMAND ----------

-- DBTITLE 1,[wip] Statistics By Location
-- TODO: not as useful as i would like
SELECT
  place,
  SUM(metered_usage_mb) / 1024 AS metered_usage_gb,
  SUM(whitelist_usage_mb) / 1024 AS whitelist_usage_gb,
  collect_set(device_id) AS all_devices_ever_present
FROM device_location_usage
WHERE place IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Single Device Statistics
-- MAGIC These results are meaningful if you input a `device_id` at the top.

-- COMMAND ----------

-- DBTITLE 1,Single Device Usage
SELECT
  device_id,
  SUM(metered_usage_mb) / 1024 AS metered_usage_gb,
  SUM(whitelist_usage_mb) / 1024 AS whitelist_usage_gb,
  collect_set(driver_id) AS all_drivers
FROM device_drivers_usage
WHERE device_id = getArgument('device_id')
GROUP BY 1
ORDER BY 2 DESC

-- COMMAND ----------

-- DBTITLE 1,Single Device HOS Statuses
SELECT
  device_id,
  status_code,
  status_name,
  SUM(usage_mb) / 1024 AS usage_gb
FROM device_hos_usage
WHERE device_id = getArgument('device_id')
GROUP BY 1, 2, 3
