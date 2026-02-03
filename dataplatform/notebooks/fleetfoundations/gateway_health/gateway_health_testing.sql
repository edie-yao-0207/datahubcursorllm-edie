-- Databricks notebook source
-- MAGIC %run ./gateway_health_query $endMs=1597770000000 $orgId=9086

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Testing
-- MAGIC 
-- MAGIC To test the query, I took a snapshot of the health statuses of all the devices at 1597770000000, aka Aug 18, 2020 at 10 AM. I then compared the query results to that snapshot.
-- MAGIC 
-- MAGIC We can't get a perfect match for a few reasons:
-- MAGIC - our computation of battery isolator statuses doesn't work well for the historical case
-- MAGIC - our max lookback window is 90 days so if a vehicle hasn't connected in 90 days we will almost always classify it as "requires investigation" even if the website would have classified it as weak cell signal or low battery

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # File location and type
-- MAGIC file_location = "/FileStore/tables/parth/device_results_1597770000000.csv"
-- MAGIC file_type = "csv"
-- MAGIC 
-- MAGIC # CSV options
-- MAGIC infer_schema = "false"
-- MAGIC first_row_is_header = "true"
-- MAGIC delimiter = ","
-- MAGIC 
-- MAGIC # The applied options are for CSV files. For other file types, these will be ignored.
-- MAGIC df = spark.read.format(file_type) \
-- MAGIC   .option("inferSchema", infer_schema) \
-- MAGIC   .option("header", first_row_is_header) \
-- MAGIC   .option("sep", delimiter) \
-- MAGIC   .load(file_location)
-- MAGIC 
-- MAGIC display(df)
-- MAGIC 
-- MAGIC temp_table_name = "gateway_health_snapshot"
-- MAGIC 
-- MAGIC df.createOrReplaceTempView(temp_table_name)

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW latest_heartbeats_duke AS
SELECT
  org_id,
  device_id,
  (1597770000000 - (latest_heartbeat.proto_value.hub_server_device_heartbeat.last_heartbeat_at_ms + COALESCE(latest_heartbeat.proto_value.hub_server_device_heartbeat.heartbeat_period_sec, 0) * 2 * 1000)) AS active_until_ms
FROM (
  SELECT
    org_id,
    object_id AS device_id,
    MAX(STRUCT(time, value)).value AS latest_heartbeat
  FROM (
    SELECT id AS object_id, org_id
    FROM productsdb.devices
    WHERE org_id = 9086
  )
  LEFT JOIN kinesisstats.osdhubserverdeviceheartbeat
    USING(org_id, object_id)
  WHERE date > date_sub('2020-08-18', 91) AND date <= '2020-08-18' AND time <= 1597770000000 AND org_id = 9086
  GROUP BY 1, 2
);

CACHE TABLE latest_heartbeats_duke

-- COMMAND ----------

-- compare the tables, looking at devices that HAVE connected in the last 90 days
-- EXPECTED: 108

WITH all_devices AS (
  SELECT org_id, id AS device_id
  FROM productsdb.devices
  WHERE org_id = 9086
    AND product_id IN (7, 17, 24, 35, 53, 89, 90)
)

SELECT org_id, device_id, sot.status_enum AS correct_enum, sgh.status_enum AS my_enum, sot.status AS correct_status, sgh.status AS my_status, active_until_ms, sgh.*
FROM all_devices
LEFT JOIN gateway_health_snapshot AS sot
  USING(org_id, device_id)
LEFT JOIN spark_gateway_health_vgs AS sgh
  USING(org_id, device_id)
LEFT JOIN latest_heartbeats_duke
  USING(org_id, device_id)
WHERE sot.status_enum != sgh.status_enum
AND active_until_ms IS NOT NULL


-- COMMAND ----------

-- compare the tables, looking at devices that HAVE NOT connected in the last 90 days
-- EXPECTED: prolonged offline, RI, 377
--           low vehicle battery, RI, 149
--           weak cellular signal, RI, 87
WITH all_devices AS (
  SELECT org_id, id AS device_id
  FROM productsdb.devices
  WHERE org_id = 9086
    AND product_id IN (7, 17, 24, 35, 53, 89, 90)
)

SELECT sot.status AS correct_status, sgh.status AS my_status, COUNT(*) AS count --org_id, device_id, sot.status_enum AS correct_enum, sgh.status_enum AS my_enum, sot.status AS correct_status, sgh.status AS my_status, active_until_ms, sgh.*
FROM all_devices
LEFT JOIN gateway_health_snapshot AS sot
  USING(org_id, device_id)
LEFT JOIN spark_gateway_health_vgs AS sgh
  USING(org_id, device_id)
LEFT JOIN latest_heartbeats_duke
  USING(org_id, device_id)
WHERE sot.status_enum != sgh.status_enum
AND active_until_ms IS NULL
GROUP BY 1, 2
ORDER BY 3 DESC

-- COMMAND ----------

-- EXPECTED: 9 rows
WITH all_devices AS (
  SELECT org_id, id AS device_id
  FROM productsdb.devices
  WHERE org_id = 9086
    AND product_id IN (42, 54, 62, 65) -- ag45/eu, ag46eu
)

SELECT org_id, device_id, sot.status_enum AS correct_enum, sgh.status_enum AS my_enum, sot.status AS correct_status, sgh.status AS my_status, active_until_ms, sgh.*
FROM all_devices
LEFT JOIN gateway_health_snapshot AS sot
  USING(org_id, device_id)
LEFT JOIN spark_gateway_health_ag45_46 AS sgh
  USING(org_id, device_id)
LEFT JOIN latest_heartbeats_duke
  USING(org_id, device_id)
WHERE sot.status_enum != sgh.status_enum

-- COMMAND ----------

-- compare the tables, looking at devices that HAVE NOT connected in the last 90 days
-- EXPECTED: 17 rows
WITH all_devices AS (
  SELECT org_id, id AS device_id
  FROM productsdb.devices
  WHERE org_id = 9086
    AND product_id IN (27, 36, 68, 83, 84, 85) -- ag24/eu, ag26/eu, ag46p/eu
)

SELECT org_id, device_id, sot.status_enum AS correct_enum, sgh.status_enum AS my_enum, sot.status AS correct_status, sgh.status AS my_status, active_until_ms, sgh.*
FROM all_devices
LEFT JOIN gateway_health_snapshot AS sot
  USING(org_id, device_id)
LEFT JOIN spark_gateway_health_ag24_26_46p AS sgh
  USING(org_id, device_id)
LEFT JOIN latest_heartbeats_duke
  USING(org_id, device_id)
WHERE sot.status_enum != sgh.status_enum
