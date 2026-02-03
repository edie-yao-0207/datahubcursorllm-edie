WITH image_urls (org_id, object_id, gateway_id, date, uploaded_time, event_id, report_type, asset_ms, s3url) AS (
  SELECT
    org_id,
    object_id,
    value.proto_value.uploaded_file_set.gateway_id AS gateway_id,
    date,
    time as uploaded_time,
    value.proto_value.uploaded_file_set.event_id AS event_id,
    value.proto_value.uploaded_file_set.report_type AS report_type,
    value.proto_value.uploaded_file_set.file_timestamp AS asset_ms,
    EXPLODE(value.proto_value.uploaded_file_set.s3urls) AS s3url
  FROM kinesisstats.osduploadedfileset
  WHERE
    (string(value.proto_value.uploaded_file_set.s3urls) LIKE "%.h264%"
    OR string(value.proto_value.uploaded_file_set.s3urls) LIKE "%.h265%")
    AND date >= ${start_date}
    AND date < ${end_date}
),

image_urls_derived_1 (org_id, object_id, gateway_id, date, uploaded_time, event_id, image_url, metadata_file, direction, report_type, asset_ms) AS (
  SELECT
    t1.org_id,
    t1.object_id,
    t1.gateway_id,
    t1.date,
    t1.uploaded_time,
    t1.event_id,
    t1.s3url AS image_url,
    t2.s3url AS metadata_file,
    CASE
      WHEN t1.s3url LIKE "%driver%" THEN "inward"
      WHEN t1.s3url LIKE "%rear%" THEN "backward"
      ELSE "forward"
    END AS direction,
    t1.report_type,
    t1.asset_ms
  FROM image_urls t1
  JOIN image_urls t2
    ON t1.org_id = t2.org_id
    AND t1.object_id = t2.object_id
    AND t1.event_id = t2.event_id
    AND t1.uploaded_time = t2.uploaded_time
    AND (t1.s3url LIKE "%.h264" OR t1.s3url LIKE "%.h265")
    AND t2.s3url LIKE "%metadata.json%"
),

image_urls_derived_2 (org_id, date, uploaded_time, event_id, object_id, product_type, vg_id, image_url, metadata_file, asset_ms, report_type, direction) AS (
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
    t1.image_url,
    t1.metadata_file,
    t1.asset_ms,
    t1.report_type,
    t1.direction
  FROM image_urls_derived_1 t1
  LEFT JOIN productsdb.devices t2
    ON t1.object_id = t2.id
),

osdaccelerometer (org_id, object_id, event_id, time, value) AS (
  SELECT
    org_id,
    object_id,
    value.proto_value.accelerometer_event.event_id AS event_id,
    MIN(time) AS time,
    MIN(value) AS value
  FROM kinesisstats.osdaccelerometer
  WHERE date >= ${start_date} AND date < ${end_date}
  GROUP BY org_id, object_id, value.proto_value.accelerometer_event.event_id
),

image_urls_derived_3 (org_id, date, uploaded_time, event_id, object_id, cm_id, vg_id, image_url, metadata_file, asset_ms, event_ms, report_type, harsh_accel_type, direction) AS (
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
    CONCAT(REPLACE(REPLACE(t1.image_url, ".s3.us-west-2.amazonaws.com", ""), "https://", "s3://"), ".jpeg") AS image_url,
    CONCAT(REPLACE(REPLACE(t1.metadata_file, ".s3.us-west-2.amazonaws.com", ""), "https://", "s3://"), ".jpeg") AS metadata_file,
    t1.asset_ms,
    COALESCE(t2.time, "") AS event_ms,
    t1.report_type,
    t2.value.proto_value.accelerometer_event.harsh_accel_type AS harsh_accel_type,
    t1.direction
  FROM image_urls_derived_2 t1
  LEFT JOIN osdaccelerometer t2
    ON t1.event_id = t2.value.proto_value.accelerometer_event.event_id
    AND t2.value.proto_value.accelerometer_event.event_id IS NOT NULL
    AND t1.org_id = t2.org_id
    AND t1.object_id = t2.object_id
),

location (org_id, device_id, date, time, value) AS (
  SELECT
    org_id,
    device_id,
    date,
    time,
    value
  FROM kinesisstats.location
  WHERE date >= ${start_date} AND date < ${end_date}
),

/* Joins kinesisstats.location table to image_urls CTE.
* Note:
* 1) Joins on unix epoch timestamps rounded down to the minute
* 2) Ranks rows within each org, device, minute grouping based on nearest occuring image & location events
* 3) Select the row within each org, device, minute grouping with the nearest occuring image & location events
 */
stg_dashcam_images (
      org_id,
      date,
      uploaded_time,
      event_id,
      object_id,
      cm_id,
      vg_id,
      image_url,
      metadata_file,
      asset_ms,
      event_ms,
      report_type,
      harsh_accel_type,
      direction,
      latitude,
      longitude,
      speed_meters_per_second,
      country,
      state,
      city,
      rank
) AS (
  SELECT *
  FROM (
    SELECT
      t1.org_id,
      t1.date,
      t1.uploaded_time,
      t1.event_id,
      t1.object_id,
      t1.cm_id,
      t1.vg_id,
      t1.image_url,
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
    FROM image_urls_derived_3 t1
    LEFT JOIN location t2
    ON t1.date = t2.date
    AND t1.org_id = t2.org_id
    AND t1.vg_id = t2.device_id
    AND (t1.asset_ms / 1000) = (t2.time /1000)
  ) a
  WHERE a.rank = 1
)

SELECT
      org_id,
      date,
      uploaded_time,
      event_id,
      object_id,
      cm_id,
      vg_id,
      image_url,
      metadata_file,
      asset_ms,
      event_ms,
      report_type,
      harsh_accel_type,
      direction,
      latitude,
      longitude,
      speed_meters_per_second,
      country,
      state,
      city
FROM stg_dashcam_images
WHERE
    date >= ${start_date}
    AND date < ${end_date}
