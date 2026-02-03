-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Overview
-- MAGIC We have long-running issues with our camera calibration, which is necessary for forward collision warning and following distance detection to work accurately. We are currently working on new versions of these detection models that don't require calibration, but in the meantime need to support customers who are negatively impacted by cameras not calibrating.
-- MAGIC
-- MAGIC ## Analysis Objectives
-- MAGIC - What % of cameras in the field are still calibrating despite meeting the stated criteria for automated calibration?
-- MAGIC - Report for implementation team to recognize devices that need manual calibration
-- MAGIC - Identify "at risk" cameras so our teams can investigate and resolve any inaccurate manual calibrations
-- MAGIC
-- MAGIC ## Logic To Use
-- MAGIC When CMs lose calibration, they begin try to recalibrate on trip. In order to calibrate, the vehicle must be traveling 45mph or more for at least 30 minutes. Once that happens, a CM **should** be able to regain calibration.
-- MAGIC
-- MAGIC ## Sign Offs &check; &cross;
-- MAGIC | Name | Area | Sign Off |Date |
-- MAGIC | ----------- | ----------- | ----------- | ----------- |
-- MAGIC | Michael Howard | SQL Creator| &check; | 2023-06-15 |
-- MAGIC | Nnamdi Offor | SQL Code Reviewer| &check; | 2023-06-15 |
-- MAGIC | Salil Gupta | Business & Code Logic Reviewer| &check; | 2023-06-20 |
-- MAGIC | Cole Jurden | Business Logic Reviewer| &check; | 2023-06-20 |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Create CM Calibration State Intervals
-- MAGIC This will help us determine how long devices have been in each calibration state (and if they are currently stuck).

-- COMMAND ----------

-- DBTITLE 1,Need to narrow down the data to relevant orgs
-- we only want orgs that have FCW or Following Distance turned on as these are the only features that are affected by these calibration issues
create or replace temp view org_level_settings as (
  SELECT id,
         name,
         /* AI Events */
         settings_proto.forward_collision_warning_enabled, -- AI
         settings_proto.forward_collision_warning_audio_alerts_enabled, -- AI
         settings_proto.following_distance_enabled, -- AI
         settings_proto.following_distance_audio_alerts_enabled -- AI
  FROM clouddb.organizations
  WHERE settings_proto.forward_collision_warning_enabled = true -- this determines if FCW safety inbox is enabled
    or settings_proto.forward_collision_warning_audio_alerts_enabled = true -- this determines if FCW in-cab alerts is enabled
    or settings_proto.following_distance_enabled = true -- this determines if tailgating safety inbox is enabled
    or settings_proto.following_distance_audio_alerts_enabled = true -- this determines if tailgating in-cab alerts is enabled
)

-- COMMAND ----------

-- DBTITLE 1,Find calibration state records to be basis for intervals
-- Find calibration state records to be basis for intervals
create or replace temp view calibration_states as (
  SELECT a.date,
        a.time,
        a.object_id as cm_device_id,
        a.org_id,
        coalesce(value.proto_value.vanishing_point_state.calibrated,false) as calibrated,
        case -- Determination of calibration states in this discussion with Salil, JDel, and Kavya: https://samsara-rd.slack.com/archives/C024V8ES69Z/p1686869826973269?thread_ts=1686583847.203289&cid=C024V8ES69Z
          WHEN value.proto_value.vanishing_point_state.calibration_state = 0 THEN 'Calibrating'
          WHEN value.proto_value.vanishing_point_state.calibration_state is null THEN 'Calibrating'
          WHEN value.proto_value.vanishing_point_state.calibration_state = 2 THEN 'Calibrating'
          WHEN value.proto_value.vanishing_point_state.calibration_state = 3 THEN 'Calibrated'
        end as calibration_state
  FROM kinesisstats_history.osdvanishingpointstate a -- this is where we get calibration state information
  INNER JOIN org_level_settings org -- we find only the orgs with relevant event types enabled
    on a.org_id = org.id
  WHERE true
);

-- COMMAND ----------

-- DBTITLE 1,Need to create intervals of time that devices are spending calibrating vs. not calibrating
-- create calibration state intervals

--find previous calibration states to better find points of state change
create or replace temporary view find_previous_cal_state as (
  SELECT cm_device_id,
        lag(time) over(partition by cm_device_id order by time) as prev_time,
        lag(calibration_state) over(partition by cm_device_id order by time) as prev_calibration_state,
        time as cur_time,
        calibration_state as cur_calibration_state
  FROM calibration_states
);

-- filter down to points with state change
create or replace temporary view transitions as (
select
  cm_device_id,
  prev_time,
  prev_calibration_state,
  cur_time,
  cur_calibration_state
from find_previous_cal_state
where
  cur_calibration_state != prev_calibration_state -- transitions
  or prev_time is null -- first occurrence
);

-- create intervals
create or replace temporary view intervals AS (
  SELECT
    cm_device_id,
    cur_calibration_state as calibration_state,
    date(timestamp_millis(cur_time)) as start_date,
    cur_time AS start_ms,
    coalesce(lead(cur_time) OVER (PARTITION BY cm_device_id ORDER BY cur_time), unix_millis(now())) AS end_ms,
    (coalesce(lead(cur_time) OVER (PARTITION BY cm_device_id ORDER BY cur_time), unix_millis(now())) - cur_time) / (1000*60) as duration_mins
  FROM
    transitions
);

-- COMMAND ----------

-- DBTITLE 1,Save the calibration intervals
CREATE OR REPLACE TABLE data_analytics.cm_calibration_intervals
USING DELTA
PARTITIONED BY (start_date)
AS
SELECT * FROM intervals

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Filter to Most Recent Calibration Status
-- MAGIC This is important because we only care about the calibration status that the CM is **currently** in.

-- COMMAND ----------

-- DBTITLE 1,Find the Most Recent Calibration Status Interval per Device
CREATE OR REPLACE TEMP VIEW most_recent_state AS (
  SELECT *
  FROM data_analytics.cm_calibration_intervals
  QUALIFY RANK() OVER(PARTITION BY cm_device_id ORDER BY end_ms DESC) = 1 -- most recent interval
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Create 45mph+ Intervals
-- MAGIC In order to successfully recalibrate, a device must be travelling at least 45mph for at least 30 minutes. These intervals help us get that qualifying information.

-- COMMAND ----------

-- DBTITLE 1,Find when VG's Linked to CMs are travelling at least 45mph
-- need to find instances of VGs traveling at least 45mph
CREATE OR REPLACE TEMP VIEW mph_base AS (
    SELECT a.org_id,
        a.object_id AS vg_device_id,
        a.date,
        a.time,
        (a.value.int_value / 1000) * 1.151 AS mph, -- conversion to mph
        CASE -- flag when travelling at least 45mph
          WHEN ((a.value.int_value / 1000) * 1.151) >= 45
          THEN TRUE
          ELSE FALSE
        END AS over_45
  FROM kinesisstats.osdenginemilliknots a -- this gets our speed information
  INNER JOIN dataprep_safety.cm_linked_vgs clv -- this finds the link between the VG reporting speed and a CM
    ON a.object_id = clv.vg_device_id
    AND a.org_id = clv.org_id
    AND clv.linked_cm_id is not null
    AND date >= DATE_SUB(NOW(), 183) --last 6 months
  INNER JOIN org_level_settings org -- this narrows the data to orgs with the relevant event types enabled
    ON a.org_id = org.id
);

-- COMMAND ----------

-- DBTITLE 1,Create intervals of time devices are traveling at least 45mph
-- create speed intervals
CREATE OR REPLACE TEMP VIEW find_previous_mph_bucket AS (
  SELECT vg_device_id,
        LAG(time) OVER(PARTITION BY vg_device_id ORDER BY time) AS prev_time,
        LAG(over_45) OVER(PARTITION BY vg_device_id ORDER BY time) AS prev_over_45,
        time AS cur_time,
        over_45 AS cur_over_45
  FROM mph_base
);

-- filter down to points with state change
CREATE OR REPLACE TEMP VIEW mph_transitions AS (
SELECT
  vg_device_id,
  prev_time,
  prev_over_45,
  cur_time,
  cur_over_45
FROM find_previous_mph_bucket
WHERE
  cur_over_45 != prev_over_45 -- transitions
  OR prev_time IS NULL -- first occurrence
);

-- create intervals
CREATE OR REPLACE TEMP VIEW mph_intervals AS (
  SELECT
    vg_device_id,
    cur_over_45 AS over_45,
    DATE(TIMESTAMP_MILLIS(cur_time)) AS start_date,
    cur_time AS start_ms,
    COALESCE(LEAD(cur_time) OVER (PARTITION BY vg_device_id ORDER BY cur_time), UNIX_MILLIS(NOW())) AS end_ms, -- coalesce with now to close most recent open intervals
    (COALESCE(LEAD(cur_time) OVER (PARTITION BY vg_device_id ORDER BY cur_time), UNIX_MILLIS(NOW())) - cur_time) / (1000*60) AS duration_mins -- coalesce with now to close most recent open intervals
  FROM
    mph_transitions
);

-- COMMAND ----------

-- DBTITLE 1,Save the MPH Intervals
CREATE OR REPLACE TABLE data_analytics.vg_over45_intervals
USING DELTA
PARTITIONED BY (start_date)
AS
SELECT * FROM mph_intervals

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Find CMs that should be calibrated
-- MAGIC Now we combine our calibration states with time spent 45mph+ to understand which devices that are calibrating, should be calibrated.

-- COMMAND ----------

-- DBTITLE 1,Need to find intervals of time CMs are powered and traveling at least 45mph.
-- Find mph intervals when the CM is getting powered (CM is plugged in and can calibrate)
CREATE OR REPLACE TEMP VIEW powered_mph_intervals AS (
  SELECT mph.vg_device_id,
         pwr.linked_cm_id AS cm_device_id,
         mph.over_45,
         pwr.has_power,
         GREATEST(pwr.start_ms,mph.start_ms) AS start_ms, -- this finds overlapping start time
         LEAST(pwr.end_ms,mph.end_ms) AS end_ms, -- this finds overlapping end time
         (LEAST(pwr.end_ms,mph.end_ms) - GREATEST(pwr.start_ms,mph.start_ms)) / (1000*60) AS duration_min -- difference between overlapping end time and overlapping start time
  FROM data_analytics.vg_over45_intervals mph -- here we get our speed information
  INNER JOIN dataprep_safety.cm_linked_vgs clv -- we find our vg <> cm mapping
    ON mph.vg_device_id = clv.vg_device_id
  INNER JOIN dataprep_safety.cm_power_ranges pwr -- we find when our CMs are powered on
    ON mph.vg_device_id = pwr.device_id
    AND clv.linked_cm_id = pwr.linked_cm_id
    AND NOT (pwr.start_ms >= mph.end_ms OR pwr.end_ms <= mph.start_ms)
  INNER JOIN org_level_settings org -- we narrow to only the devices in orgs with the correct settings
    ON clv.org_id = org.id
  WHERE true -- arbitrary for readability
    AND pwr.has_power = 1 -- need to make sure our CMs have power when the VGs are traveling at least 45mph
    AND mph.over_45 = true -- need to make sure we are looking at time periods that qualify the CMs for calibration
);

-- COMMAND ----------

-- DBTITLE 1,Need to find how long CMs have been traveling 45+ MPH while in Calibrating status
-- Find how long CMs have been traveling 45+ MPH while in Calibrating status
CREATE OR REPLACE TEMP VIEW cal_mph_intervals AS (
  SELECT cal.cm_device_id,
        mph.vg_device_id,
        cal.calibration_state,
        cal.start_ms AS cal_start_ms,
        cal.end_ms AS cal_end_ms,
        mph.over_45,
        CASE -- if the calibrating interval overlaps with an interval of traveling 45mph+, find when the overlap starts
          WHEN mph.cm_device_id IS NULL THEN NULL
          ELSE GREATEST(cal.start_ms,mph.start_ms)
        END AS over45_start_ms,
        CASE -- if the calibrating interval overlaps with an interval of traveling 45mph+, find when the overlap ends
          WHEN mph.cm_device_id IS NULL THEN NULL
          ELSE LEAST(cal.end_ms,mph.end_ms)
        END AS over45_end_ms,
        CASE -- if the calibrating interval overlaps with an interval of traveling 45mph+, find time between overlap start and overlap end
          WHEN mph.cm_device_id IS NULL THEN NULL
          ELSE (LEAST(cal.end_ms,mph.end_ms) - GREATEST(cal.start_ms,mph.start_ms)) / (1000*60)
        END AS duration_min,
        CASE -- if the calibrating interval overlaps with an interval of traveling 45mph+, create a flag if the overlapping interval is at least 30 minutes
          WHEN mph.cm_device_id IS NULL THEN 0
          ELSE (CASE
                  WHEN (LEAST(cal.end_ms,mph.end_ms) - GREATEST(cal.start_ms,mph.start_ms)) / (1000*60) >= 30 THEN 1
                  ELSE 0
                END
               )
        END AS over_45_over_30
  FROM most_recent_state cal -- this gets us our current calibration state per CM device
  LEFT JOIN powered_mph_intervals mph -- this helps us understand whether the CM qualifies to be recalibrated or not
    ON cal.cm_device_id = mph.cm_device_id
    AND NOT (mph.start_ms >= cal.end_ms OR mph.end_ms <= cal.start_ms)
  WHERE true -- arbitrary for readability
    AND cal.calibration_state = 'Calibrating' -- only care about when the CM is currently calibrating
);

-- COMMAND ----------

-- DBTITLE 1,Find how many chances the CM had to calibrate
-- Aggregate 45+ MPH travel time by each Calibrating status interval
CREATE OR REPLACE TEMP VIEW cal_mph_agg AS (
  SELECT cm_device_id,
         vg_device_id,
         calibration_state,
         DATE(TIMESTAMP_MILLIS(cal_start_ms)) AS cal_start_date,
         cal_start_ms AS calibrating_start,
         cal_end_ms AS calibrating_end,
         SUM(CASE -- only want to sum the duration if it is a qualify event (30+ minutes of 45mph)
              WHEN over_45_over_30 = 1 THEN duration_min
              ELSE 0
             END) AS total_continuous_45mph_30min_duration,
         SUM(over_45_over_30) AS num_continuous_45mph_30min
  FROM cal_mph_intervals
  GROUP BY cm_device_id,
         vg_device_id,
         calibration_state,
         cal_start_ms,
         cal_end_ms
);

-- COMMAND ----------

-- DBTITLE 1,Save final calibration data
CREATE OR REPLACE TABLE data_analytics.calibrating_cm_status
USING DELTA
PARTITIONED BY (cal_start_date)
AS
SELECT * FROM cal_mph_agg

-- COMMAND ----------

-- DBTITLE 1,Find daily counts
CREATE OR REPLACE TEMP VIEW daily_agg AS (
  SELECT DATE(NOW()) AS date,
        dev.org_id,
        org.name AS org_name, 
        CASE -- described in https://amundsen.internal.samsara.com/table_detail/cluster/delta/clouddb/organizations
          WHEN org.internal_type = 0 THEN 'Customer Org'
          WHEN org.internal_type = 1 THEN 'Internal Org'
          WHEN org.internal_type = 2 THEN 'Lab Org'
          WHEN org.internal_type = 3 THEN 'Developer Portal Test Org'
          WHEN org.internal_type = 4 THEN 'Developer Portal Dev Org'
          ELSE org.internal_type
        END AS org_type,
        COUNT(a.cm_device_id) AS num_cms_calibrating
  FROM data_analytics.calibrating_cm_status a -- find calibration data
  LEFT JOIN productsdb.devices dev -- find device information
    ON a.cm_device_id = dev.id
  LEFT JOIN clouddb.organizations org -- find org information
    ON dev.org_id = org.id
  WHERE true -- arbitrary for readability
    AND a.num_continuous_45mph_30min > 0 -- find calibration eligible devices
  GROUP BY 1,2,3,4
);

-- COMMAND ----------

-- DBTITLE 1,Update daily agg table
MERGE INTO data_analytics.calibrating_cm_status_daily_agg AS target
USING daily_agg AS updates ON
target.date = updates.date
AND target.org_id = updates.org_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------


