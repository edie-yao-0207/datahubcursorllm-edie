WITH cm_obstructed_last_trip AS (
  SELECT
    org_id,
    device_id,
    MAX(proto.start.time) AS trip_start_ms
  FROM
    trips2db_shards.trips
  WHERE
    date >= date_sub(${start_date}, 5)
    AND date < ${end_date}
    AND proto.end.time IS NOT NULL
    AND version = 101
    AND device_id IN(
      SELECT
        device_id
      FROM
        cm_health_report.cm_obstructions
    )
  GROUP BY
    org_id,
    device_id
)

SELECT * FROM cm_obstructed_last_trip
