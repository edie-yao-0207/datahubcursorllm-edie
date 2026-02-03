WITH
  date_table AS (
    SELECT EXPLODE(SEQUENCE(DATE(${start_date}), DATE(${end_date}), INTERVAL 1 DAY)) as date
  ),
  osddashcamconnected_timeframe AS (
    SELECT DISTINCT date_table.date, osddashcamconnected.org_id, osddashcamconnected.object_id
    FROM kinesisstats.osddashcamconnected INNER JOIN date_table
      ON unix_millis(timestamp(date_sub(date_table.date, 30))) < osddashcamconnected.time  -- within 30 days before
      AND osddashcamconnected.time <= unix_millis(timestamp(date_table.date))
  ),
  camera_driver_facing AS (
    SELECT productsdb.devices.id, productsdb.devices.org_id
    FROM productsdb.devices
    WHERE
      productsdb.devices.camera_product_id = 43 OR productsdb.devices.camera_product_id = 155  -- CM32/34
      AND productsdb.devices.id IS NOT NULL
      AND productsdb.devices.org_id IS NOT NULL
  )

SELECT
  camera_driver_facing.id,
  camera_driver_facing.org_id,
  osddashcamconnected_timeframe.date
FROM camera_driver_facing INNER JOIN osddashcamconnected_timeframe
ON camera_driver_facing.id = osddashcamconnected_timeframe.object_id
AND camera_driver_facing.org_id = osddashcamconnected_timeframe.org_id
