-- Databricks notebook source
-- MAGIC %md
-- MAGIC Step 1: Grab Tacho Devices
-- MAGIC - Tacho devices can be identified by looking at the osDTachographVUDownload, osDTachographDriverInfo, osDTachographDriverState and seeing which devices have reported one of these objectStats within the last 6 months
-- MAGIC - Don't look at 2019 data as devices reported tacho graph info even when not connected

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW devices AS (
  SELECT gw.id AS gateway_id, 
    gw.device_id, 
    gw.org_id
  FROM clouddb.gateways AS gw
  WHERE gw.product_id IN (24,35,53,89,178)
) 

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW tacho_download_devices AS (
  SELECT
    org_id,
    object_id AS device_id,
    count(*) AS download_count
  FROM kinesisstats.osDTachographVUDownload
  WHERE date >= to_date(now() - interval 6 months)
  GROUP BY
    org_id,
    object_id
);

CREATE OR REPLACE TEMP VIEW tacho_dinfo_devices AS (
  SELECT
    org_id,
    object_id AS device_id,
    count(*) AS dinfo_count
  FROM kinesisstats.osDTachographDriverInfo
  WHERE date >= to_date(now() - interval 6 months)
  GROUP BY
    org_id,
    object_id
);

CREATE OR REPLACE TEMP VIEW tacho_dstate_devices AS (
  SELECT
    org_id,
    object_id AS device_id,
    count(*) AS dstate_count
  FROM kinesisstats.osDTachographDriverState
  WHERE date >= to_date(now() - interval 6 months)
  GROUP BY
    org_id,
    object_id
);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW devices_joined AS (
  SELECT
    a.org_id,
    a.device_id,
    COALESCE(b.download_count, 0) AS download_count,
    COALESCE(c.dinfo_count, 0) AS dinfo_count,
    COALESCE(d.dstate_count, 0) AS dstate_count
  FROM devices AS a
  LEFT JOIN tacho_download_devices AS b ON
    a.org_id = b.org_id
    AND a.device_id = b.device_id
  LEFT JOIN tacho_dinfo_devices AS c ON
    a.org_id = c.org_id
    AND a.device_id = c.device_id
  LEFT JOIN tacho_dstate_devices AS d ON
    a.org_id = d.org_id
    AND a.device_id = d.device_id
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW tacho_devices AS (
  SELECT *
  FROM devices_joined
  WHERE download_count > 0 
    OR dinfo_count > 0 
    OR dstate_count > 0
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Step 2: Join Tacho Devices with additional info

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW tacho_devices_extended AS (
  SELECT
    a.date,
    a.org_id,
    a.product_id,
    a.device_id,
    a.trip_count
  FROM data_analytics.vg3x_daily_summary AS a
  JOIN tacho_devices AS ta ON
    a.org_id = ta.org_id
    AND a.device_id = ta.device_id
  WHERE a.date >= to_date(now() - interval 6 months)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Step 3: Tacho Graph Download Success Rate

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW daily_tacho_download_count AS (
  SELECT
    a.date,
    a.org_id,
    a.product_id,
    a.device_id,
    a.trip_count,
    count(b.time) AS download_count
  FROM tacho_devices_extended AS a
  LEFT JOIN kinesisstats.osDTachographVUDownload AS b ON
    a.date = b.date
    AND a.org_id = b.org_id
    AND a.device_id = b.object_id
  WHERE b.date >= to_date(now() - interval 6 months)
  GROUP BY
    a.date,
    a.org_id,
    a.product_id,
    a.device_id,
    a.trip_count
);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW device_dowload_metrics AS (
  WITH daily_download_count AS (
    SELECT
      date,
      org_id,
      object_id AS device_id,
      count(*) AS download_count
    FROM kinesisstats.osDTachographVUDownload
    WHERE date >= to_date(now() - interval 6 months)
    GROUP BY
      date,
      org_id,
      object_id
  ),
  
  download_hist AS (
  SELECT
    date,
    org_id,
    device_id,
    download_count,
    lag(date) OVER (PARTITION BY org_id, device_id ORDER BY date) AS last_download_date
  FROM daily_download_count
  ),
  
  download_datediff (
  SELECT
    date,
    org_id,
    device_id,
    download_count,
    last_download_date,
    (to_unix_timestamp(date, 'yyyy-MM-dd') - to_unix_timestamp(last_download_date, 'yyyy-MM-dd')) / 86400 AS days_since_last_download
  FROM download_hist
  )

  SELECT
    date,
    org_id,
    device_id,
    download_count,
    days_since_last_download
  FROM download_datediff
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW daily_tacho_download_metrics AS (
  SELECT
    a.date,
    a.org_id,
    a.product_id,
    a.device_id,
    a.trip_count,
    b.download_count,
    b.date as download_date
  FROM tacho_devices_extended AS a
  LEFT JOIN device_dowload_metrics AS b ON
    a.date = b.date
    AND a.org_id = b.org_id
    AND a.device_id = b.device_id
);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW daily_tacho_download_metrics_extended AS (
SELECT
  date,
  org_id,
  product_id,
  device_id,
  trip_count,
  download_count,
  download_date,
  last_value(download_date, true) over (partition by org_id, device_id order by date rows between unbounded preceding and 1 preceding) as last_download_date,
  (to_unix_timestamp(date, 'yyyy-MM-dd') - to_unix_timestamp(last_value(download_date, true) over (partition by org_id, device_id order by date rows between unbounded preceding and 1 preceding), 'yyyy-MM-dd')) / 86400 AS days_since_last_download
FROM daily_tacho_download_metrics
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW tacho_download_status AS (
  SELECT
    date,
    org_id,
    product_id,
    device_id,
    trip_count,
    CASE
      WHEN trip_count > 0 AND days_since_last_download > 7 AND download_count IS NULL THEN true
      ELSE false 
    END AS download_failed
  FROM daily_tacho_download_metrics_extended
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Step 4: Diagnostics Data

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW daily_dstate_count AS (
  SELECT
    date,
    org_id,
    object_id AS device_id,
    count(*) AS dstate_count
  FROM kinesisstats.osDTachographDriverState
  WHERE date >= to_date(now() - interval 6 months)
  GROUP BY
    date,
    org_id,
    object_id
);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW diagnostics_status AS (
  SELECT
    a.date,
    a.org_id,
    a.product_id,
    a.device_id, 
    a.trip_count,
    b.dstate_count,
    CASE 
      WHEN a.trip_count > 0 AND COALESCE(b.dstate_count,0) > 0 THEN false
      ELSE true
      END AS diagnostics_failed
  FROM tacho_devices_extended AS a
  LEFT JOIN daily_dstate_count AS b ON
    a.date = b.date
    AND a.org_id = b.org_id
    AND a.device_id = b.device_id
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Step 5: Join to Final Table

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW tacho_metrics AS (
  SELECT
    a.date,
    a.org_id,
    a.product_id,
    a.device_id,
    a.trip_count,
    b.download_failed,
    c.diagnostics_failed,
    true AS has_tacho
  FROM tacho_devices_extended AS a
  LEFT JOIN tacho_download_status AS b ON
    a.date = b.date
    AND a.org_id = b.org_id
    AND a.product_id = b.product_id
    AND a.device_id = b.device_id
  LEFT JOIN diagnostics_status AS c ON
    a.date = c.date
    AND a.org_id = c.org_id
    AND a.product_id = c.product_id
    AND a.device_id = c.device_id

)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_analytics.vg_tacho_metrics USING DELTA
PARTITIONED BY (date)
SELECT * FROM tacho_metrics

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW tacho_metrics_updates AS (
  SELECT *
  FROM tacho_metrics
  WHERE date >= date_sub(current_date(),7)
);


MERGE INTO data_analytics.vg_tacho_metrics AS target
USING tacho_metrics_updates AS updates ON
target.date = updates.date
AND target.org_id = updates.org_id
AND target.product_id = updates.product_id
AND target.device_id = updates.device_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
