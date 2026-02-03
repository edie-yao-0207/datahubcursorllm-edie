-- Join with trips so we can find the last contiguous segment that overlaps with a trip, which we can call a leg end
SELECT
  date,
  org_id,
  device_id,
  start_ms,
  end_ms,
  start_value,
  end_value,
  start_odo_meters,
  end_odo_meters,
  canonical_distance_meters,
  CASE
  WHEN trip IS NOT NULL AND LEAD(trip) OVER(w) IS NULL THEN True
  WHEN trip != LEAD(trip) OVER(w) THEN True
  ELSE False
  END AS leg_end
FROM (
  SELECT
    -- Use FIRST to include location columns not in the GROUP BY
    loc.date,
    loc.org_id,
    loc.device_id,
    loc.start_ms,
    FIRST(loc.end_ms) as end_ms,
    FIRST(loc.start_value) as start_value,
    FIRST(loc.end_value) as end_value,
    FIRST(loc.start_odo_meters) as start_odo_meters,
    FIRST(loc.end_odo_meters) as end_odo_meters,
    COALESCE(FIRST(loc.canonical_distance_meters), 0) as canonical_distance_meters,
    COALESCE(FIRST(trips.proto), NULL) as trip
  FROM ifta_report.annotated_location_segments loc
  LEFT JOIN trips2db_shards.trips trips
    ON loc.start_ms <= trips.proto.end.time
    AND loc.end_ms >= trips.proto.start.time
    AND trips.org_id = loc.org_id
    AND trips.device_id = loc.device_id
    AND trips.date >= ${start_date}
    AND trips.date < ${end_date}
    AND trips.version = 101
  WHERE loc.date >= ${start_date} AND loc.date < ${end_date}
  GROUP BY loc.date, loc.org_id, loc.device_id, loc.start_ms
)
WINDOW w AS (PARTITION BY org_id, device_id ORDER BY end_ms)
