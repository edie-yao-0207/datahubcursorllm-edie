-- Databricks notebook source

CREATE OR REPLACE TEMP VIEW framerates_raw AS (
  SELECT
    explode(
      value.proto_value.dashcam_stream_status.stream_status
    ) as stream_status,
    *
  FROM kinesisstats.osDDashcamStreamStatus
  WHERE `date` BETWEEN coalesce(nullif(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),5)) AND
    coalesce(nullif(getArgument("end_date"), ''),  date_sub(CURRENT_DATE(),1))
);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW framerates AS (
  SELECT
    stream_status.recorded_frames / 60 as framerate,
    CASE
      WHEN stream_status.stream_source_enum IS null
        OR stream_status.stream_source_enum = 0 THEN 'primary'
      WHEN stream_status.stream_source_enum = 1 THEN 'secondary'
      WHEN stream_status.stream_source_enum = 7 THEN 'primary_low_res'
      WHEN stream_status.stream_source_enum = 8 THEN 'secondary_low_res'
      ELSE 'exclude'
    END AS stream_source,
    object_id, -- cm_device_id
    org_id,
    time,
    from_unixtime(time / 1000, 'yyyy-MM-dd HH:mm:ss') as date_time,
    date
  FROM framerates_raw
  WHERE stream_status.stream_source_enum IS null
    OR stream_status.stream_source_enum IN (0, 1, 7, 8)
);

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW framerates_joined AS (
  SELECT
    f.object_id AS cm_device_id,
    f.framerate,
    f.stream_source,
    f.time,
    f.date,
    sm.org_id,
    oh.is_overheated,
    sm.is_safe_mode
  FROM framerates f
  JOIN dataprep_safety.cm_overheated_intervals oh ON
    f.object_id = oh.cm_device_id AND
    f.time >= oh.start_ms AND
    f.time < oh.end_ms
  JOIN dataprep_safety.cm_safe_mode_intervals sm ON
    f.object_id = sm.cm_device_id AND
    f.time >= sm.start_ms AND
    f.time < sm.end_ms
);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW average_framerates_normal AS (
  SELECT
    cm_device_id,
    round(avg(framerate), 3) average_framerate,
    stream_source,
    date,
    org_id,
    "normal" AS thermal_state
  FROM framerates_joined
  WHERE is_overheated = 0 AND is_safe_mode = 0
  GROUP BY cm_device_id, stream_source, date, org_id
);

CREATE OR REPLACE TEMP VIEW average_framerates_overheated AS (
  SELECT
    cm_device_id,
    round(avg(framerate), 3) average_framerate,
    stream_source,
    date,
    org_id,
    "overheated" AS thermal_state
  FROM framerates_joined
  WHERE is_overheated = 1
  GROUP BY cm_device_id, stream_source, date, org_id
);

CREATE OR REPLACE TEMP VIEW average_framerates_safe_mode AS (
  SELECT
    cm_device_id,
    round(avg(framerate), 3) average_framerate,
    stream_source,
    date,
    org_id,
    "safe_mode" AS thermal_state
  FROM framerates_joined
  WHERE is_safe_mode = 1
  GROUP BY cm_device_id, stream_source, date, org_id
);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW average_framerates_union AS (
  SELECT * FROM average_framerates_normal
  UNION
  SELECT * FROM average_framerates_overheated
  UNION
  SELECT * FROM average_framerates_safe_mode
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dataprep_safety.cm_video_fps_daily
USING DELTA
PARTITIONED BY (date)
COMMENT 'Calculates the daily average framerate for each stream source (primary, primary_low_res, secondary, secondary_low_res) for each thermal state (normal, overheated, safe mode).'
AS SELECT * FROM average_framerates_union;

-- COMMAND ----------

MERGE INTO dataprep_safety.cm_video_fps_daily AS target
USING average_framerates_union AS updates
ON target.cm_device_id = updates.cm_device_id AND
  target.stream_source = updates.stream_source AND
  target.date = updates.date AND
  target.org_id = updates.org_id AND
  target.thermal_state = updates.thermal_state
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
