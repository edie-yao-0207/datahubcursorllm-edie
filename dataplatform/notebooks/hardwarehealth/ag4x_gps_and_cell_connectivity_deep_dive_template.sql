-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### READ ME
-- MAGIC The purpose of this notebook is to allow users to quickly indentify devices that have poor GPS and cellular connections.
-- MAGIC
-- MAGIC **Please make a copy of this notebook if you'd like to make any edits or additional cuts to the data**
-- MAGIC
-- MAGIC **Instructions:**
-- MAGIC 1. Input parameters in `CMD3` and `CMD5`
-- MAGIC 2. Click "Run All" at the top of the notebook
-- MAGIC 3. Scroll down or click on a link in the table of contents to see list of devices
-- MAGIC
-- MAGIC **Table of Contents:**
-- MAGIC * Device Aggregate GPS and Cell Connectivity Stats
-- MAGIC * Daily GPS and Cell Percentiles by Product
-- MAGIC * Daily GPS and Cell Percentiles by Firmware
-- MAGIC * Daily Top 50 Worst GPS Devices by Product
-- MAGIC * Daily Top 50 Worst GPS Devices by Firmware
-- MAGIC * Daily Top 50 Worst Cell Connectivity by Product
-- MAGIC * Daily Top 50 Worst Cell Connectivity Devices by Firmware
-- MAGIC
-- MAGIC **Addtional Resources:**
-- MAGIC * [WIP] Documentation on playground tables can be referenced [here](https://samsara.atlassian-us-gov-mod.net/wiki/spaces/SHE/pages/4567906/Computed+Tables+and+Databricks+Resources)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Step 1: Grab Devices
-- MAGIC In the cell below are several conditions on in the `WHERE` clause for you to input your parameters of interest:
-- MAGIC * Line 16: start_date as `"YYYY-MM-DD"`| *date as str*
-- MAGIC * Line 17: end_date as `"YYYY-MM-DD"` | *date as str*
-- MAGIC * Line 18: org_type as `('Customer','Internal')`|  *tuple of one or more str values*
-- MAGIC * Line 19: product_id as `(65`,`62`,`42`,`54)`|      *tuple of one or more int values*
-- MAGIC * Line 20: latest_build_on_day as `("","")`| *tuple of one or more string values*

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW devices AS (
  SELECT
    d.date,
    d.org_id,
    d.org_type,
    p.name AS product_name,
    d.device_id,
    d.serial,
    d.latest_build_on_day
  FROM playground.dataprep_ag4x_daily_summary AS d
  JOIN definitions.products AS p ON
    d.product_id = p.product_id
  WHERE date >= "YYYY-MM-DD"
    AND date <= "YYYY-MM-DD"
    AND d.org_type IN ("")
    AND p.product_id IN ()
    AND d.latest_build_on_day IN ("")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Step 2: Grab Connectivity Data
-- MAGIC In the cell below are several conditions on in the `WHERE` clause for you to input your parameters of interest:
-- MAGIC * Line 14: start_date as `"YYYY-MM-DD"`| *date as str*
-- MAGIC * Line 15: end_date as `"YYYY-MM-DD"` | *date as str*

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW osdgatewaymicrobackendconnectivity AS (
  SELECT
    date,
    org_id,
    object_id AS device_id,
    count(1) AS total_msg_received_count,
    SUM(CASE WHEN value.proto_value.gateway_micro_backend_connectivity.gps_fix = True THEN 1 ELSE 0 END) AS msg_has_gps_count,
    SUM(CASE WHEN value.proto_value.gateway_micro_backend_connectivity.is_store_and_forward = True THEN 1 ELSE 0 END) AS msg_backlogged_count,
    SUM(CASE WHEN value.proto_value.gateway_micro_backend_connectivity.is_store_and_forward = True THEN 0 ELSE 1 END) AS msg_live_count,
    SUM(CASE WHEN value.proto_value.gateway_micro_backend_connectivity.gps_fix = True and value.proto_value.gateway_micro_backend_connectivity.is_store_and_forward is null THEN 1 ELSE 0 END) AS msg_live_and_has_gps_count
  FROM kinesisstats.osdgatewaymicrobackendconnectivity
  WHERE value.is_databreak = 'false'
    AND value.is_end = 'false'
    AND date >= "YYYY-MM-DD"
    AND date <= "YYYY-MM-DD"
  GROUP BY
    date,
    org_id,
    object_id
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Step 2: Join Tables

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW device_gps_metrics AS (
  SELECT
    a.date,
    a.org_id,
    a.org_type,
    a.product_name,
    a.device_id,
    a.serial,
    a.latest_build_on_day,
    COALESCE(b.total_msg_received_count, 0) AS total_msg_received_count,
    COALESCE(b.msg_has_gps_count, 0) AS msg_has_gps_count,
    COALESCE(b.msg_backlogged_count, 0) AS msg_backlogged_count,
    COALESCE(b.msg_live_count, 0) AS msg_live_count,
    COALESCE(b.msg_live_and_has_gps_count, 0) AS msg_live_and_has_gps_count
  FROM devices AS a
  LEFT JOIN osdgatewaymicrobackendconnectivity AS b ON
    a.date = b.date
    AND a.org_id = b.org_id
    AND a.device_id = b.device_id
  ORDER BY a.date
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Step 3: Calculate GPS and Cell Connectivity Metrics

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW device_connectivity_metrics AS (
  SELECT
    date,
    org_id,
    org_type,
    product_name,
    device_id,
    serial,
    latest_build_on_day,
    msg_has_gps_count,
    msg_live_count,
    total_msg_received_count,
    ROUND((msg_has_gps_count / total_msg_received_count) * 100.00, 2) AS gps_success_rate,
    ROUND((msg_live_count / total_msg_received_count) * 100.00, 2) AS cell_success_rate
  FROM device_gps_metrics
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Step 4: Exploratory Data
-- MAGIC Below are some example queries that can be used to surface our worst offenders

-- COMMAND ----------

-- DBTITLE 1,Device Aggregate GPS and Cell Connectivity Stats
WITH agg_device_metrics AS (
  SELECT
    org_id,
    org_type,
    product_name,
    device_id,
    serial,
    LAST(latest_build_on_day) AS last_reported_build,
    SUM(msg_has_gps_count) AS total_msg_w_gps,
    SUM(msg_live_count) AS total_msg_live,
    SUM(msg_live_and_has_gps_count) AS total_msg_live_and_has_gps_count,
    SUM(total_msg_received_count) AS total_msg_received,
    (SUM(msg_live_count) / SUM(total_msg_received_count)) * 100 AS agg_cell_success_rate,
    (SUM(msg_has_gps_count) / SUM(total_msg_received_count)) * 100 AS agg_gps_success_rate,
    (SUM(total_msg_received_count) / SUM(total_msg_received_count)) * 100 AS agg_cell_and_gps_success_rate
  FROM device_gps_metrics
  GROUP BY
    org_id,
    org_type,
    product_name,
    device_id,
    serial
  )

  SELECT
    org_id,
    org_type,
    product_name,
    device_id,
    serial,
    last_reported_build,
    total_msg_w_gps,
    total_msg_live,
    total_msg_live_and_has_gps_count,
    total_msg_received,
    agg_cell_success_rate,
    agg_gps_success_rate,
    agg_cell_and_gps_success_rate
  FROM agg_device_metrics
  ORDER BY agg_cell_and_gps_success_rate ASC
--See sample output below

-- COMMAND ----------

-- DBTITLE 1, Daily GPS and Cell Percentiles by Product
WITH product_percentiles AS (
  SELECT
    date,
    product_name,
    percentile_approx(gps_success_rate,0.95) AS gps_success_rate_95th_percentile,
    percentile_approx(gps_success_rate,0.50) AS median_gps_success_rate,
    percentile_approx(gps_success_rate,0.25) AS gps_success_rate_25th_percentile,
    percentile_approx(cell_success_rate,0.95) AS cell_success_rate_95th_percentile,
    percentile_approx(cell_success_rate,0.50) AS median_cell_success_rate,
    percentile_approx(cell_success_rate,0.25) AS cell_success_rate_25th_percentile
  FROM device_connectivity_metrics
  GROUP BY
    date,
    product_name
)

  SELECT
    date,
    product_name,
    gps_success_rate_95th_percentile,
    median_gps_success_rate,
    gps_success_rate_25th_percentile,
    cell_success_rate_95th_percentile,
    median_cell_success_rate,
    cell_success_rate_25th_percentile
  FROM product_percentiles
--See sample output below

-- COMMAND ----------

-- DBTITLE 1, Daily GPS and Cell Percentiles by Firmware
WITH firmware_percentiles AS (
  SELECT
    date,
    latest_build_on_day,
    percentile_approx(gps_success_rate,0.95) AS gps_success_rate_95th_percentile,
    percentile_approx(gps_success_rate,0.50) AS median_gps_success_rate,
    percentile_approx(gps_success_rate,0.25) AS gps_success_rate_25th_percentile,
    percentile_approx(cell_success_rate,0.95) AS cell_success_rate_95th_percentile,
    percentile_approx(cell_success_rate,0.50) AS median_cell_success_rate,
    percentile_approx(cell_success_rate,0.25) AS cell_success_rate_25th_percentile
  FROM device_connectivity_metrics
  GROUP BY
    date,
    latest_build_on_day
)

  SELECT
    date,
    latest_build_on_day,
    gps_success_rate_95th_percentile,
    median_gps_success_rate,
    gps_success_rate_25th_percentile,
    cell_success_rate_95th_percentile,
    median_cell_success_rate,
    cell_success_rate_25th_percentile
  FROM firmware_percentiles
--See sample output below

-- COMMAND ----------

-- DBTITLE 1,Daily Top 50 Worst GPS Devices by Product
WITH ranked_gps_by_product AS (
  SELECT
    date,
    org_id,
    org_type,
    product_name,
    device_id,
    serial,
    latest_build_on_day,
    msg_has_gps_count,
    total_msg_received_count,
    gps_success_rate,
    row_number() OVER (PARTITION BY date, product_name ORDER BY gps_success_rate ASC) AS rank
  FROM device_connectivity_metrics
  )

  SELECT
    date,
    rank,
    product_name,
    org_id,
    org_type,
    device_id,
    serial,
    latest_build_on_day,
    msg_has_gps_count,
    total_msg_received_count,
    gps_success_rate
  FROM ranked_gps_by_product
  WHERE rank <= 50
  ORDER BY rank ASC
--See sample output below

-- COMMAND ----------

-- DBTITLE 1,Daily Top 50 Worst GPS Devices by Firmware
WITH ranked_gps_by_firmware AS (
  SELECT
    date,
    org_id,
    org_type,
    product_name,
    device_id,
    serial,
    latest_build_on_day,
    msg_has_gps_count,
    total_msg_received_count,
    gps_success_rate,
    row_number() OVER (PARTITION BY date, latest_build_on_day ORDER BY gps_success_rate ASC) AS rank
  FROM device_connectivity_metrics
  )

  SELECT
    date,
    rank,
    latest_build_on_day,
    product_name,
    org_id,
    org_type,
    device_id,
    serial,
    msg_has_gps_count,
    total_msg_received_count,
    gps_success_rate
  FROM ranked_gps_by_firmware
  WHERE rank <= 50
  ORDER BY rank ASC
--See sample output below

-- COMMAND ----------

-- DBTITLE 1,Daily Top 50 Worst Cell Connectivity by Product
WITH ranked_cell_by_product AS (
  SELECT
    date,
    org_id,
    org_type,
    product_name,
    device_id,
    serial,
    latest_build_on_day,
    msg_live_count,
    total_msg_received_count,
    cell_success_rate,
    row_number() OVER (PARTITION BY date, product_name ORDER BY cell_success_rate ASC) AS rank
  FROM device_connectivity_metrics
  )

  SELECT
    date,
    rank,
    product_name,
    org_id,
    org_type,
    device_id,
    serial,
    latest_build_on_day,
    msg_live_count,
    total_msg_received_count,
    cell_success_rate
  FROM ranked_cell_by_product
  WHERE rank <= 50
  ORDER BY rank ASC
--See sample output below

-- COMMAND ----------

-- DBTITLE 1,Daily Top 50 Worst Cell Connectivity Devices by Firmware
WITH ranked_cell_by_firmware AS (
  SELECT
    date,
    org_id,
    org_type,
    product_name,
    device_id,
    serial,
    latest_build_on_day,
    msg_live_count,
    total_msg_received_count,
    cell_success_rate,
    row_number() OVER (PARTITION BY date, latest_build_on_day ORDER BY cell_success_rate ASC) as rank
  FROM device_connectivity_metrics
  )

  SELECT
    date,
    rank,
    latest_build_on_day,
    product_name,
    org_id,
    org_type,
    device_id,
    serial,
    msg_live_count,
    total_msg_received_count,
    cell_success_rate
  FROM ranked_cell_by_firmware
  WHERE rank <= 50
  ORDER BY rank ASC
--See sample output below
