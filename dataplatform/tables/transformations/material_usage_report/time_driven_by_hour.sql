WITH spreader_device_trips AS (
  SELECT
    *
  FROM
    (
      SELECT
        DATE(date) AS date,
        (org_id, device_id) AS org_device,
        proto.start.time AS trip_start_ms,
        proto.end.time AS trip_end_ms
      FROM
        trips2db_shards.trips
      WHERE
        date >= DATE_SUB(${start_date}, 1)
        AND date < ${end_date}
        AND (
          proto.ongoing IS NULL
          OR NOT proto.ongoing
        )
        AND version = 101
        AND org_id NOT IN (1, 562949953421343)
    )
  WHERE
    org_device IN (
      SELECT
        *
      FROM
        material_usage_report.spreader_devices
    )
),
exploded_trips AS (
  SELECT
    org_device.org_id,
    org_device.device_id,
    trip_start_ms,
    trip_end_ms,
    hour_start,
    hour_start + INTERVAL 1 HOUR AS hour_end
  FROM
    (
      SELECT
        *,
        EXPLODE(
          SEQUENCE(
            DATE_TRUNC('hour', FROM_UNIXTIME(trip_start_ms / 1000)),
            DATE_TRUNC('hour', FROM_UNIXTIME(trip_end_ms / 1000)),
            INTERVAL 1 HOUR
          )
        ) AS hour_start
      FROM
        spreader_device_trips
    )
),
exploded_trips_by_hour AS (
  SELECT
    org_id,
    device_id,
    trip_start_ms,
    trip_end_ms,
    hour_start,
    hour_start + INTERVAL 1 HOUR AS hour_end,
    CASE
      WHEN trip_start_ms < to_unix_timestamp(hour_start) * 1000 THEN to_unix_timestamp(hour_start) * 1000
      ELSE trip_start_ms
    END AS utilization_start_ms,
    CASE
      WHEN trip_end_ms > to_unix_timestamp(hour_end) * 1000 THEN to_unix_timestamp(hour_end) * 1000
      ELSE trip_end_ms
    END AS utilization_end_ms
  FROM
    exploded_trips
),
aggregated_trip_time_by_hour as (
  SELECT
    date(hour_start) as date,
    org_id,
    device_id,
    hour_start,
    hour_end,
    sum(utilization_end_ms - utilization_start_ms) AS duration_driven_ms
  FROM
    exploded_trips_by_hour
  GROUP BY
    date,
    org_id,
    device_id,
    hour_start,
    hour_end
)
SELECT * from aggregated_trip_time_by_hour
WHERE date >= ${start_date}
AND date < ${end_date}
