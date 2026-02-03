-- Databricks notebook source

CREATE OR REPLACE TEMP VIEW livestreams_raw AS (
  SELECT
    date,
    org_id,
    object_id,
    time,
    value.proto_value.livestream_webrtc_telemetry.connection_succeeded AS success,
    value.proto_value.livestream_webrtc_telemetry.connection_establishment_ms AS establishment_ms,
    value.proto_value.livestream_webrtc_telemetry.connection_duration_ms AS duration_ms,
    value.proto_value.livestream_webrtc_telemetry.end_reason AS end_reason
  FROM kinesisstats.osdlivestreamwebrtctelemetry
  WHERE value.is_databreak = false AND
    `date` BETWEEN coalesce(nullif(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),5)) AND
    coalesce(nullif(getArgument("end_date"), ''),  date_sub(CURRENT_DATE(),1))
);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW livestreams_by_day AS (
  SELECT
    date,
    org_id,
    object_id AS cm_device_id,
    count(time) AS livestreams,
    sum(CASE WHEN success IS true THEN 1 ELSE 0 END
    ) AS successful_livestreams,
    sum(duration_ms) AS livestream_duration_ms
  FROM livestreams_raw
  GROUP BY date, org_id, object_id
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dataprep_safety.cm_livestreams_daily
USING DELTA
PARTITIONED BY (date)
COMMENT 'Calculates the total number of livestreams, number of successful livestreams, and total duration of successfull livestreams by day and device.'
AS SELECT * FROM livestreams_by_day;

-- COMMAND ----------

MERGE INTO dataprep_safety.cm_livestreams_daily AS target
USING livestreams_by_day AS updates
ON target.cm_device_id = updates.cm_device_id AND
  target.date = updates.date AND
  target.org_id = updates.org_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
