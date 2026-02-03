WITH calibrated_dates AS (
  SELECT
    a.org_id,
    a.object_id as device_id,
    a.date,
    a.time,
    IF(a.value.is_databreak, "uncalibrated", "calibrated") as status,
    CASE
      WHEN coalesce(
        a.value.proto_value.imu_orientation.is_loaded_from_disk,
        false
      ) THEN 'loaded_from_disk'
      WHEN a.value.is_databreak THEN 'uncalibrated'
      ELSE "derived_on_this_boot"
    END AS source
  FROM
    kinesisstats.osdimuorientation AS a
  WHERE
    a.date >= "2021-01-01"
),
reset_dates AS (
  SELECT
    r.org_id,
    r.object_id as device_id,
    r.date,
    r.time,
    "uncalibrated" as status, --both databreaks and non-databreaks indicate loss of calibration/start of calibration
    NULL AS source
  FROM
    kinesisstats.osdimucalibrationreset AS r
  WHERE
    r.date >= "2021-01-01"
),
joined_dates AS (
  SELECT
    *
  FROM
    calibrated_dates
  UNION ALL
  SELECT
    *
  FROM
    reset_dates
)
SELECT
  org_id,
  device_id,
  status,
  source,
  time as start_ms,
  COALESCE(
    LEAD(time) OVER (
      PARTITION BY
        org_id,
        device_id
      ORDER BY
        time
    ),
    (TO_UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) + (48*60*60)) * 1000 -- plus two days for last event
  ) AS end_ms,
  COALESCE(
    LEAD(status) OVER (
      PARTITION BY
        org_id,
        device_id
      ORDER BY
        time
    )
  ) AS next_status,
  COALESCE(
    LEAD(source) OVER (
      PARTITION BY
        org_id,
        device_id
      ORDER BY
        time
    )
  ) AS next_source
FROM
  joined_dates
