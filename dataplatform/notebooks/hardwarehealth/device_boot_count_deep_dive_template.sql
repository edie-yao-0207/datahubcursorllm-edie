-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### READ ME
-- MAGIC The purpose of this notebook is to allow users to quickly indentify devices that are experiencing the highest boot count.
-- MAGIC
-- MAGIC **Please make a copy of this notebook if you'd like to make any edits or additional cuts to the data**
-- MAGIC
-- MAGIC **Instructions:**
-- MAGIC 1. Input parameters in
-- MAGIC 2. Click "Run All" at the top of the notebook
-- MAGIC 3. Scroll down or click on a link in the table of contents to see list of devices
-- MAGIC
-- MAGIC **Table of Contents:**
-- MAGIC * Daily Total Boot Counts per Device - 95th Percentile
-- MAGIC * Daily Top 50 Devices by Product
-- MAGIC * Daily Top 50 Devices by Firmware
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
-- MAGIC * Line 23: internal_type as  `(1,0)`|  *tuple of one or more int values*
-- MAGIC * Line 24: product_name as `("","")`|      *tuple of one or more string values*
-- MAGIC * Line 25: latest_build_on_day as `("","")`| *tuple of one or more string values*

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW devices_with_boots AS (
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
    a.latest_build_on_day,
    a.boot_counts_on_day
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
-- MAGIC ##### Step 2: Exploratory Data
-- MAGIC Below are some example queries that can be used to surface our worst offenders

-- COMMAND ----------

-- DBTITLE 1,Daily Total Boot Counts per Device - 95th Percentile
WITH percentile AS (
    SELECT
      date,
      percentile_approx(boot_counts_on_day,0.95) AS nth_percentile
    FROM devices_with_boots
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
  a.boot_counts_on_day
FROM devices_with_boots AS a
JOIN percentile AS p ON
  a.date = p.date
WHERE boot_counts_on_day >= nth_percentile
ORDER BY boot_counts_on_day DESC
--See sample output below

-- COMMAND ----------

-- DBTITLE 1,Daily Top 50 Devices by Product
WITH ranked_devices AS (
  SELECT
    date,
    org_type,
    org_id,
    org_name,
    product_name,
    device_id,
    latest_build_on_day,
    boot_counts_on_day,
    row_number() OVER (PARTITION BY date, product_name ORDER BY boot_counts_on_day DESC) AS rank
  FROM devices_with_boots
  )

  SELECT
    date,
    rank,
    product_name,
    org_type,
    org_id,
    org_name,
    device_id,
    latest_build_on_day,
    boot_counts_on_day
  FROM ranked_devices
  WHERE rank <= 50
  ORDER BY date ASC, rank ASC
--See sample output below

-- COMMAND ----------

-- DBTITLE 1,Daily Top 50 Devices by Firmware
WITH ranked_devices AS (
  SELECT
    date,
    org_type,
    org_id,
    org_name,
    product_name,
    device_id,
    latest_build_on_day,
    boot_counts_on_day,
    row_number() OVER (PARTITION BY date, latest_build_on_day ORDER BY boot_counts_on_day DESC) as rank
  FROM devices_with_boots
  )

  SELECT
    date,
    rank,
    latest_build_on_day,
    org_type,
    org_id,
    org_name,
    device_id,
    boot_counts_on_day
  FROM ranked_devices
  WHERE rank <= 50
  ORDER BY date ASC, rank ASC
--See sample output below
