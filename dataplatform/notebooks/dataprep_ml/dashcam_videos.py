from datetime import datetime, timedelta
import ffmpeg
import json
import pandas as pd
from pyspark.sql import functions as F, types as T


@F.pandas_udf(returnType=T.StringType())
def ffprobe(series):
    rows = []
    for i, val in series.iteritems():
        try:
            metadata = ffmpeg.probe(filename=val.replace("s3://", "/dbfs/mnt/"))
        except ffmpeg.Error as e:
            metadata = {}
        rows.append(json.dumps(metadata))
    return pd.Series(rows, index=series.index)


spark.udf.register(ffprobe.__name__, ffprobe)


START_DATE = dbutils.widgets.get("start_date") or str(
    (datetime.utcnow() - timedelta(days=3)).date()
)
END_DATE = dbutils.widgets.get("end_date") or str(datetime.utcnow().date())


spark.sql(
    f"""SELECT
    org_id,
    object_id,
    value.proto_value.uploaded_file_set.gateway_id AS gateway_id,
    date,
    time as uploaded_time,
    value.proto_value.uploaded_file_set.event_id AS event_id,
    value.proto_value.uploaded_file_set.report_type AS report_type,
    value.proto_value.uploaded_file_set.file_timestamp AS asset_ms,
    EXPLODE(value.proto_value.uploaded_file_set.s3urls) AS s3url,
    value
  FROM kinesisstats.osduploadedfileset
  WHERE
    string(value.proto_value.uploaded_file_set.s3urls) LIKE "%.mp4%"
    AND date >= '{START_DATE}'
    AND date <= '{END_DATE}'
"""
).createOrReplaceTempView("all_urls")

spark.sql(
    f"""
  SELECT
      t1.org_id,
      t1.object_id,
      t1.gateway_id,
      t1.date,
      t1.uploaded_time,
      t1.event_id,
      t1.s3url AS video_url,
      t2.s3url AS metadata_file,
      CASE
        WHEN t1.s3url LIKE "%multicamera%" THEN "any"
        WHEN t1.s3url LIKE "%driver%" THEN "inward"
        WHEN t1.s3url LIKE "%rear%" THEN "backward"
        ELSE "forward"
      END AS direction,
      t1.report_type,
      t1.asset_ms
    FROM all_urls t1
    JOIN all_urls t2
      ON t1.org_id = t2.org_id
      AND t1.object_id = t2.object_id
      AND t1.event_id = t2.event_id
      AND t1.uploaded_time = t2.uploaded_time
      AND t1.s3url LIKE "%.mp4"
      AND t2.s3url LIKE "%metadata.json%"
"""
).createOrReplaceTempView("video_urls_and_metadata_1")

spark.sql(
    """
  SELECT
    t1.org_id,
    t1.date,
    t1.uploaded_time,
    t1.event_id,
    t1.object_id,
    CASE
      WHEN product_id IN (25,30,31,43,44,46,91)
      THEN "CM"
      WHEN product_id IN (7,17,24,35,53,89,90)
      THEN "VG"
    END AS product_type,
    COALESCE(t1.gateway_id, object_id) AS vg_id,
    t1.video_url,
    t1.metadata_file,
    t1.asset_ms,
    t1.report_type,
    t1.direction
  FROM video_urls_and_metadata_1 t1
  LEFT JOIN productsdb.devices t2
    ON t1.object_id = t2.id
"""
).createOrReplaceTempView("video_urls_and_metadata")

spark.sql(
    f"""
  SELECT
    org_id,
    object_id,
    value.proto_value.accelerometer_event.event_id AS event_id,
    MIN(time) AS time,
    MIN(value) AS value
  FROM kinesisstats.osdaccelerometer
  WHERE date >= '{START_DATE}'
    AND date <= '{END_DATE}'
  GROUP BY org_id, object_id, value.proto_value.accelerometer_event.event_id
"""
).createOrReplaceTempView("osdaccelerometer")

spark.sql(
    f"""
  SELECT
    t1.org_id,
    t1.date,
    t1.uploaded_time,
    t1.event_id,
    t1.object_id,
    CASE
      WHEN t1.product_type = "CM" THEN t1.object_id
      ELSE null
    END AS cm_id,
    t1.vg_id,
    REPLACE(REPLACE(t1.video_url, ".s3.us-west-2.amazonaws.com", ""), "https://", "s3://") AS video_url,
    REPLACE(REPLACE(t1.metadata_file, ".s3.us-west-2.amazonaws.com", ""), "https://", "s3://") AS metadata_file,
    t1.asset_ms,
    t2.time AS event_ms,
    t1.report_type,
    t2.value.proto_value.accelerometer_event.harsh_accel_type AS harsh_accel_type,
    t1.direction
  FROM video_urls_and_metadata t1
  LEFT JOIN osdaccelerometer t2
    ON t1.event_id = t2.value.proto_value.accelerometer_event.event_id
    AND t2.value.proto_value.accelerometer_event.event_id IS NOT NULL
    AND t1.org_id = t2.org_id
    AND t1.object_id = t2.object_id
"""
).createOrReplaceTempView("video_urls_and_metadata_with_accel_info")

spark.sql(
    f"""
    SELECT
        org_id,
        device_id,
        date,
        time,
        value
    FROM kinesisstats.location
    WHERE date >= '{START_DATE}' AND date <= '{END_DATE}'
"""
).createOrReplaceTempView("location")


# Joins kinesisstats.location table to video urls CTEs.
# Note:
# 1) Joins on unix epoch timestamps rounded down to the minute
# 2) Ranks rows within each org, device, minute grouping based on nearest occuring image & location events
# 3) Select the row within each org, device, minute grouping with the nearest occuring image & location events
#
spark.sql(
    """
    SELECT
        *,
        CASE
            WHEN ABS(longitude) BETWEEN 0 AND 15 THEN asset_ms + (0 * 3600000 * SIGN(longitude))
            WHEN ABS(longitude) BETWEEN 15 AND 30 THEN asset_ms + (1 * 3600000 * SIGN(longitude))
            WHEN ABS(longitude) BETWEEN 30 AND 45 THEN asset_ms + (2 * 3600000 * SIGN(longitude))
            WHEN ABS(longitude) BETWEEN 45 AND 60 THEN asset_ms + (3 * 3600000 * SIGN(longitude))
            WHEN ABS(longitude) BETWEEN 60 AND 75 THEN asset_ms + (4 * 3600000 * SIGN(longitude))
            WHEN ABS(longitude) BETWEEN 75 AND 90 THEN asset_ms + (5 * 3600000 * SIGN(longitude))
            WHEN ABS(longitude) BETWEEN 90 AND 105 THEN asset_ms + (6 * 3600000 * SIGN(longitude))
            WHEN ABS(longitude) BETWEEN 105 AND 120 THEN asset_ms + (7 * 3600000 * SIGN(longitude))
            WHEN ABS(longitude) BETWEEN 105 AND 135 THEN asset_ms + (8 * 3600000 * SIGN(longitude))
            WHEN ABS(longitude) BETWEEN 135 AND 150 THEN asset_ms + (9 * 3600000 * SIGN(longitude))
            WHEN ABS(longitude) BETWEEN 150 AND 165 THEN asset_ms + (10 * 3600000 * SIGN(longitude))
            WHEN ABS(longitude) BETWEEN 165 AND 180 THEN asset_ms + (11 * 3600000 * SIGN(longitude))
        END as asset_local_ms
    FROM (
        SELECT
        t1.org_id,
        t1.date,
        t1.uploaded_time,
        t1.event_id,
        t1.object_id,
        t1.cm_id,
        t1.vg_id,
        t1.video_url,
        t1.metadata_file,
        t1.asset_ms,
        t1.event_ms,
        t1.report_type,
        t1.harsh_accel_type,
        t1.direction,
        t2.value.latitude,
        t2.value.longitude,
        COALESCE(t2.value.ecu_speed_meters_per_second, t2.value.gps_speed_meters_per_second) as speed_meters_per_second,
        t2.value.revgeo_country as country,
        t2.value.revgeo_state as state,
        t2.value.revgeo_city as city,
        ROW_NUMBER() OVER (PARTITION BY t1.date, t1.org_id, t1.vg_id, t1.cm_id, t1.direction, t1.asset_ms ORDER BY ABS(t1.asset_ms - t2.time) ASC NULLS LAST) as rank
        FROM video_urls_and_metadata_with_accel_info t1
        LEFT JOIN location t2
        ON t1.date = t2.date
        AND t1.org_id = t2.org_id
        AND t1.vg_id = t2.device_id
        AND (t1.asset_ms / 1000) = (t2.time /1000)
    ) a
    WHERE a.rank = 1
"""
).createOrReplaceTempView("video_urls_with_location")


df_dashcam_videos = spark.sql(
    """
    SELECT
        anon1.org_id,
        anon1.date,
        anon1.uploaded_time,
        anon1.event_id,
        anon1.object_id,
        anon1.cm_id,
        anon1.vg_id,
        anon1.video_url,
        anon1.metadata_file,
        anon1.asset_ms,
        anon1.event_ms,
        anon1.report_type,
        anon1.harsh_accel_type,
        anon1.direction,
        anon1.latitude,
        anon1.longitude,
        anon1.speed_meters_per_second,
        anon1.country,
        anon1.state,
        anon1.city,
        anon1.asset_local_ms,
        CAST(GET_JSON_OBJECT(ffprobe_metadata, '$.format.duration') AS DOUBLE) AS duration_s,
        NVL2(
            GET_JSON_OBJECT(ffprobe_metadata, '$.streams[0].r_frame_rate'),
            CAST(SPLIT(GET_JSON_OBJECT(ffprobe_metadata, '$.streams[0].r_frame_rate'), '[/]')[0] AS DOUBLE) / CAST(SPLIT(GET_JSON_OBJECT(ffprobe_metadata, '$.streams[0].r_frame_rate'), '[/]')[1] AS DOUBLE),
            NULL) AS framerate,
        CAST(GET_JSON_OBJECT(ffprobe_metadata, '$.format.bit_rate') AS BIGINT) AS bitrate,
        CONCAT(GET_JSON_OBJECT(ffprobe_metadata, '$.streams[0].width'), 'x', get_json_object(ffprobe_metadata, '$.streams[0].height')) as resolution,
        GET_JSON_OBJECT(ffprobe_metadata, '$.streams[0].codec_name') AS codec,
        CAST(GET_JSON_OBJECT(ffprobe_metadata, '$.format.size') AS BIGINT) AS size_bytes
    FROM (
        SELECT
            *,
            ffprobe(video_url) as ffprobe_metadata
        FROM video_urls_with_location
        ) anon1
    """
)

df_dashcam_videos.write.format("delta").mode("overwrite").option(
    "replaceWhere", f"date >= '{START_DATE}' and date <= '{END_DATE}'"
).option("mergeSchema", "true").saveAsTable(
    "dataprep_ml.dashcam_videos", partitionBy=["date"]
)
