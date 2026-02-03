-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### READ ME
-- MAGIC The purpose of this notebook is to allow users to quickly indentify devices with the highest anomaly event occurances and most frequent anomalies.
-- MAGIC
-- MAGIC **Please make a copy of this notebook if you'd like to make any edits or additional cuts to the data**
-- MAGIC
-- MAGIC **Instructions:**
-- MAGIC 1. Input parameters in `CMD2` and `CMD4`
-- MAGIC 2. Click "Run All" at the top of the notebook
-- MAGIC 3. Scroll down or click on a link in the table of contents to see list of devices
-- MAGIC
-- MAGIC **Table of Contents:**
-- MAGIC * Daily Total Anomaly Events per Device - 95th Percentile
-- MAGIC * Top 50 Anomalies by Volume by Product
-- MAGIC * Top 50 Anomalies by Volume by Firmware
-- MAGIC * Worst Devices by Anomaly Event Volume per Product Over Date Range
-- MAGIC * Worst Anomaly Events by Volume per Product Over Date Range
-- MAGIC
-- MAGIC **Addtional Resources:**
-- MAGIC * Documentation on dataprep table can be referenced [here](https://paper.dropbox.com/folder/show/dataprep-e.1gg8YzoPEhbTkrhvQwJ2zzo0Pf5jdQGrqzhAk1JsEMbjt2c5R2u1)
-- MAGIC * `definitions.products`: Table containing all product id <> product name mappings
-- MAGIC * `clouddb.firmware_build_infos`: Table containing all firmware build strings in the `edison_build_name` col

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Step 1: Grab Devices
-- MAGIC In the cell below are several conditions on in the `WHERE` clause for you to input your parameters of interest:
-- MAGIC * Line 21: start_date as `"YYYY-MM-DD"`| *date as str*
-- MAGIC * Line 22: end_date as `"YYYY-MM-DD"` | *date as str*
-- MAGIC * Line 23: internal_type as `(1,0)`|  *tuple of one or more int values*
-- MAGIC * Line 24: product_name as `("","")`|      *tuple of one or more string values*
-- MAGIC * Line 25: latest_build_on_day as `("","")`| *tuple of one or more string values*

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW devices AS (
  SELECT
    a.date,
    CASE
      WHEN o.internal_type = 1 THEN 'Internal'
      ELSE 'Customer'
    END AS org_type,
    a.org_id,
    o.name AS org_name,
    p.name AS product_name,
    a.device_id,
    a.latest_build_on_day
  FROM dataprep.device_builds AS a
  JOIN clouddb.organizations AS o ON
    a.org_id = o.id
  JOIN productsdb.devices AS d ON
    a.org_id = d.org_id
    AND a.device_id = d.id
  JOIN definitions.products AS p ON
    d.product_id = p.product_id
  WHERE a.date >= "YYYY-MM-DD"
    AND a.date <= "YYYY-MM-DD"
    AND o.internal_type IN (1,0)  -- tuple of int values
    AND p.name IN ("") -- tuple of one or more string values
    AND a.latest_build_on_day IN ("","")  -- tuple of one or more string values
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Step 2: Grab Anomalies
-- MAGIC In the cell below are several conditions on in the `WHERE` clause for you to input your parameters of interest:
-- MAGIC * Line 9: start_date as `"YYYY-MM-DD"`| *date as str*
-- MAGIC * Line 10: end_date as `"YYYY-MM-DD"` | *date as str*
-- MAGIC * Line 11: anomaly_event_service as ("")| *tuple of one or more string values*

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW anomalies AS (
  SELECT
    date,
    org_id,
    device_id,
    anomaly_event_service,
    ae_count AS event_count
  FROM dataprep.device_anomalies
  WHERE date >= "YYYY-MM-DD"
    AND date <= "YYYY-MM-DD"
    --AND anomaly_event_service LIKE "%%" --uncomment this to search for specific anomalies
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Step 3: Join Devices and Anomalies

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW device_anomalies AS (
  SELECT
    d.date,
    d.org_type,
    d.org_id,
    d.org_name,
    d.product_name,
    d.device_id,
    d.latest_build_on_day,
    a.anomaly_event_service,
    a.event_count
  FROM devices AS d
  JOIN anomalies AS a ON
    d.date = a.date
    AND d.org_id = a.org_id
    AND d.device_id = a.device_id
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Step 4: Exploratory Data
-- MAGIC Below are some example queries that can be used to surface our worst offenders

-- COMMAND ----------

-- DBTITLE 1,Daily Total Anomaly Events per Device - 95th Percentile
WITH total_anomaly_count AS (
  SELECT
    date,
    org_type,
    org_id,
    org_name,
    product_name,
    device_id,
    latest_build_on_day,
    SUM(event_count) AS daily_event_count
  FROM device_anomalies
  GROUP BY
    date,
    org_type,
    org_id,
    org_name,
    product_name,
    device_id,
    latest_build_on_day
),

  percentile AS (
    SELECT
      date,
      percentile_approx(daily_event_count,0.95) AS nth_percentile
    FROM total_anomaly_count
    GROUP BY date
  )

SELECT
  a.date,
  a.org_type,
  a.org_id,
  a.org_name,
  a.product_name,
  a.device_id,
  a.latest_build_on_day,
  a.daily_event_count
FROM total_anomaly_count AS a
JOIN percentile AS p ON
  a.date = p.date
WHERE daily_event_count >= nth_percentile
ORDER BY daily_event_count DESC
--See sample output below

-- COMMAND ----------

-- DBTITLE 1,Daily Top 50 Anomalies by Volume per Product
WITH anomaly_volume AS (
  SELECT
    date,
    product_name,
    anomaly_event_service,
    SUM(event_count) AS daily_event_count
  FROM device_anomalies
  GROUP BY
    date,
    product_name,
    anomaly_event_service
),

  ranked_anomlies AS (
  SELECT
    date,
    product_name,
    anomaly_event_service,
    daily_event_count,
    row_number() OVER (PARTITION BY date, product_name ORDER BY daily_event_count DESC) AS rank
  FROM anomaly_volume
  )

  SELECT
    date,
    rank,
    product_name,
    anomaly_event_service,
    daily_event_count
  FROM ranked_anomlies
  WHERE rank <= 50
  ORDER BY rank ASC
--See sample output below

-- COMMAND ----------

-- DBTITLE 1,Worst Devices by Anomaly Event Volume per Product Over Date Range
WITH anomaly_volume AS (
  SELECT
    device_id,
    product_name,
    anomaly_event_service,
    SUM(event_count) AS total_event_count
  FROM device_anomalies
  GROUP BY
    device_id,
    product_name,
    anomaly_event_service
),

  ranked_anomlies AS (
  SELECT
    device_id,
    product_name,
    anomaly_event_service,
    total_event_count,
    row_number() OVER (PARTITION BY product_name ORDER BY total_event_count DESC) AS rank
  FROM anomaly_volume
  )

  SELECT
    rank,
    device_id,
    product_name,
    anomaly_event_service,
    total_event_count
  FROM ranked_anomlies
  WHERE rank <= 50
  ORDER BY rank ASC
--See sample output below

-- COMMAND ----------

-- DBTITLE 1,Daily Top 50 Anomalies by Volume per by Firmware
WITH anomaly_volume AS (
  SELECT
    date,
    latest_build_on_day,
    anomaly_event_service,
    SUM(event_count) AS daily_event_count
  FROM device_anomalies
  GROUP BY
    date,
    latest_build_on_day,
    anomaly_event_service
),

  ranked_anomlies AS (
  SELECT
    date,
    latest_build_on_day,
    anomaly_event_service,
    daily_event_count,
    row_number() OVER (PARTITION BY date, latest_build_on_day ORDER BY daily_event_count DESC) AS rank
  FROM anomaly_volume
  )

  SELECT
    date,
    rank,
    latest_build_on_day,
    anomaly_event_service,
    daily_event_count
  FROM ranked_anomlies
  WHERE rank <= 50
  ORDER BY rank ASC
--See sample output below

-- COMMAND ----------

-- DBTITLE 1,Worst Anomaly Events by Volume per Product Over Date Range
WITH anomaly_volume AS (
  SELECT
    product_name,
    anomaly_event_service,
    SUM(event_count) AS total_event_count
  FROM device_anomalies
  GROUP BY
    product_name,
    anomaly_event_service
),

  ranked_anomlies AS (
  SELECT
    product_name,
    anomaly_event_service,
    total_event_count,
    row_number() OVER (PARTITION BY product_name ORDER BY total_event_count DESC) AS rank
  FROM anomaly_volume
  )

  SELECT
    rank,
    product_name,
    anomaly_event_service,
    total_event_count
  FROM ranked_anomlies
  WHERE rank <= 50
  ORDER BY rank ASC
--See sample output below
