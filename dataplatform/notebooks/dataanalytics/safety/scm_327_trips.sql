-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Overview
-- MAGIC [SCM-327](https://samsaradev.atlassian.net/browse/SCM-327) is a bug where CM devices are incorrectly enumerating to VGs because they are in Qualcomm EDL Mode. In this mode, CM devices are not able to work properly. The goal of this notebook is to be able to identify when devices are entering this mode and how trip/recording time is affected.
-- MAGIC
-- MAGIC ## Logic To Use
-- MAGIC - When in EDL mode, CMs will enumerate as the 96899080 `usb_id`.
-- MAGIC - We need to find the intervals of time that VG's are reporting this EDL Mode `usb_id`.
-- MAGIC - Based on those intervals, we should check whether the correctly enumerating CM prior to that interval is the same as the correctly enumerating after the EDL mode interval. This will tell us whether the customer had to swap CM devices or if the CM recovered on its own.
-- MAGIC - Finally, we should overlap these EDL Mode intervals with any trips to understand how much trip/recording time was affected.
-- MAGIC
-- MAGIC ## Sign Offs &check; &cross;
-- MAGIC | Name | Area | Sign Off |Date |
-- MAGIC | ----------- | ----------- | ----------- | ----------- |
-- MAGIC | Michael Howard | SQL Creator | &check; | 2023-05-25 |
-- MAGIC | Eli Dykaar | FW Engineer | &check; | 2023-05-25 |
-- MAGIC | Tony Sergi | Support Engineer | &check; | 2023-05-25 |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Find Connection Records from VG (limit to CM and EDL usb_id's)
-- MAGIC This will allow us to track when CMs are in EDL mode

-- COMMAND ----------

-- DBTITLE 1,Find Connection Records from VG (limit to CM and EDL usb_id's)
CREATE OR REPLACE TEMP VIEW scm_base_data AS (
  SELECT object_id AS vg_device_id,
        value.proto_value.attached_usb_devices.serial[ARRAY_POSITION(value.proto_value.attached_usb_devices.usb_id, 1668105037) - 1] AS cm_device_serial, -- usb_id = 1668105037 indicates a valid enumerating CM
        EXISTS(value.proto_value.attached_usb_devices.usb_id, x -> x == 96899080) AS scm_issue, -- if usb_id = 96899080 is in the array, a CM is reporting EDL mode
        time
  FROM kinesisstats_history.osdattachedusbdevices
  WHERE true -- arbitrary for readability
    AND (value.proto_value.attached_usb_devices.serial[array_position(value.proto_value.attached_usb_devices.usb_id, 1668105037) - 1] IS NOT NULL 
        OR exists(value.proto_value.attached_usb_devices.usb_id, x -> x == 96899080)) -- filter to only records where a known CM (usb_id = 1668105037) is attached or the VG is reporting EDL mode (usb_id = 96899080)
  GROUP BY 1,2,3,4 -- like SELECT DISTINCT, but faster
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Find Intervals of Time VG is reporting EDL mode from CM
-- MAGIC This will allow us to find how long CMs get stuck in this mode

-- COMMAND ----------

-- DBTITLE 1,Find Intervals of Time VG is reporting EDL mode from CM
CREATE OR REPLACE TEMP VIEW swap_recover_v2 AS (
  WITH pairs AS (
    SELECT vg_device_id,
          LAG(time) OVER(PARTITION BY vg_device_id ORDER BY time) AS prev_time,
          LAG(cm_device_serial) OVER(PARTITION BY vg_device_id ORDER BY time) AS prev_cm_device_serial,
          LAG(scm_issue) OVER(PARTITION BY vg_device_id ORDER BY time) AS prev_scm_issue,
          time AS cur_time,
          cm_device_serial AS cur_cm_device_serial,
          scm_issue AS cur_scm_issue
    FROM scm_base_data
  ),

  -- filter down to points with recording change
  transitions AS (
  SELECT
    vg_device_id,
    prev_time,
    prev_cm_device_serial,
    prev_scm_issue,
    cur_time,
    cur_cm_device_serial,
    cur_scm_issue
  FROM pairs
  WHERE
    COALESCE(STRING(cur_scm_issue),'null') != COALESCE(STRING(prev_scm_issue),'null') -- When these are different, there is a transition
    OR prev_time IS NULL -- when this is null, this is the first record of edl/cm connection on VG
  ),

  intervals AS (
    SELECT
      vg_device_id,
      cur_time AS start_ms,
      COALESCE(LEAD(cur_time) OVER (PARTITION BY vg_device_id ORDER BY cur_time), UNIX_MILLIS(NOW())) as end_ms,
      prev_scm_issue,
      cur_scm_issue AS scm_issue,
      cur_cm_device_serial AS cm_device_serial,
      prev_cm_device_serial,
      LEAD(cur_cm_device_serial) OVER (PARTITION BY vg_device_id ORDER BY cur_time) as next_cm_device_serial
    FROM
      transitions
  ),

  intervals_final AS (
    SELECT
      vg_device_id,
      scm_issue,
      start_ms,
      end_ms,
      prev_scm_issue,
      prev_cm_device_serial,
      cm_device_serial,
      next_cm_device_serial
    FROM intervals
    WHERE (prev_scm_issue = false OR prev_scm_issue IS null) AND scm_issue = true -- filtering to only EDL mode occurrences
  )

  SELECT *, 
        CASE    
          WHEN COALESCE(prev_cm_device_serial,'null') != 'null' and COALESCE(next_cm_device_serial,'null') = 'null' -- If we have a known CM previously and an unknown cm next, we can say that the previously known CM is still stuck in EDL mode. We need more data to understand if that CM just got moved somewhere else and started to work again.
          THEN 'Still Broken'
          WHEN COALESCE(prev_cm_device_serial,'null') = COALESCE(next_cm_device_serial,'null') AND COALESCE(prev_cm_device_serial,'null') != 'null' -- If we are in EDL mode and the previous CM is the same as the next CM, we can assume the previous CM was in EDL mode then recovered.
          THEN 'Recovered'
          WHEN COALESCE(prev_cm_device_serial,'null') = COALESCE(next_cm_device_serial,'null') AND COALESCE(prev_cm_device_serial,'null') = 'null' -- If we don't know what CM was attached previously and don't know what CM is attached next, we can assume the CM currently in EDL mode has always been broken.
          THEN 'Always Broken'
          ELSE 'Swapped' -- If we are in EDL mode and the previous CM is different from the next CM, we can assume the previous CM was in EDL mode and the customer swapped to get the vehicle recording again.
        END AS cm_status,
        (end_ms - start_ms) / (1000 * 60 * 60 * 24) AS time_to_recover_days
  FROM intervals_final
  WHERE scm_issue = true -- filtering to only EDL mode occurrences
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Find Trips occuring during EDL mode occurrences
-- MAGIC This will allow us to see the impact EDL mode has on Customers

-- COMMAND ----------

-- DBTITLE 1,Find Trips occuring during EDL mode occurrences
CREATE OR REPLACE TEMP VIEW scm_trips AS (
  WITH base AS (
    SELECT a.vg_device_id,
          a.prev_cm_device_serial,
          a.next_cm_device_serial,
          a.cm_status,
          a.time_to_recover_days,
          a.start_ms AS scm_start_ms,
          a.end_ms AS scm_end_ms,
          SUM(LEAST(a.end_ms, b.end_ms) - GREATEST(a.start_ms, b.proto.start.time)) AS total_trip_scm_duration_ms, -- Find the duration of the overlap
          SUM(b.end_ms - b.proto.start.time) AS total_trip_overlap_duration_ms, -- find the duration of the total trip
          COUNT(b.end_ms) AS num_trips -- total trips that EDL mode affected
    FROM swap_recover_v2 a
    LEFT JOIN trips2db_shards.trips b  
      ON a.vg_device_id = b.device_id
      AND NOT (a.end_ms < b.proto.start.time OR a.start_ms > b.end_ms)
      AND (b.proto.end.time-b.proto.start.time) < (24*60*60*1000) --- Filter out very long trips
      AND b.proto.start.time != b.proto.end.time --- legit trips only 
      AND (b.proto.trip_distance.distance_meters) * 0.000621371 <= 1440 --- Filter out impossibly long distance trips
      AND b.version = 101
    GROUP BY 1,2,3,4,5,6,7
  )

  SELECT vg_device_id,
         prev_cm_device_serial,
         next_cm_device_serial,
         cm_status,
         time_to_recover_days,
         DATE(TIMESTAMP_MILLIS(scm_start_ms)) AS scm_start_date,
         scm_start_ms,
         scm_end_ms,
         CASE -- Find total trip time EDL mode directly impacted
          WHEN num_trips = 0
          THEN 0
          ELSE total_trip_scm_duration_ms
         END AS total_trip_scm_duration_ms,
         COALESCE(total_trip_overlap_duration_ms,0) AS total_trip_overlap_duration_ms, -- find the duration of the total trip time that edl impacted at least a portion of 
         num_trips
  FROM base
);

-- COMMAND ----------

-- DBTITLE 1,Save final table
CREATE OR REPLACE TABLE data_analytics.scm_327_trips
USING DELTA
PARTITIONED BY (scm_start_date)
AS (
  SELECT *
  FROM scm_trips 
);

-- COMMAND ----------


