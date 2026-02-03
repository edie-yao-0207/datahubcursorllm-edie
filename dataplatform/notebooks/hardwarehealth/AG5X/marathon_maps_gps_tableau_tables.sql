-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Update Tables

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW gps_loc_map AS (
WITH ttab as (select id as device_id, product_id
FROM productsdb.devices
WHERE product_id =124)

SELECT gps.org_id, gps.object_id, gps.date, gps.time as time,
       gps.gps_fix_info.latitude_nd/1000000000 as lat,
       gps.gps_fix_info.longitude_nd/1000000000 as lon
FROM (SELECT org_id, object_id, date, value.time, explode(value.proto_value.nordic_gps_debug.gps_fix_info) as gps_fix_info
      FROM kinesisstats.osdnordicgpsdebug as gps
      WHERE date == date_sub(current_date(), 1) and
            value.proto_value.nordic_gps_debug.gps_fix_info.latitude_nd is not null) AS gps
INNER JOIN ttab ON
ttab.device_id = gps.object_id
);

-- COMMAND ----------

INSERT INTO hardware.tableau_ag51_gps_map 
SELECT * FROM gps_loc_map

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW gps_loc_map_ag52 AS (
WITH ttab as (select id as device_id, product_id
FROM productsdb.devices
WHERE product_id = 125)

SELECT gps.org_id, gps.object_id, gps.date, gps.time as time, 
       gps.gps_fix_info.latitude_nd/1000000000 as lat, 
       gps.gps_fix_info.longitude_nd/1000000000 as lon
FROM (SELECT org_id, object_id, date, value.time, explode(value.proto_value.nordic_gps_debug.gps_fix_info) as gps_fix_info
      FROM kinesisstats.osdnordicgpsdebug as gps
      WHERE date == date_sub(current_date(), 1) and 
            value.proto_value.nordic_gps_debug.gps_fix_info.latitude_nd is not null) AS gps
INNER JOIN ttab ON
ttab.device_id = gps.object_id
);

-- COMMAND ----------

INSERT INTO hardware.tableau_ag52_gps_map 
SELECT * FROM gps_loc_map_ag52

-- COMMAND ----------


