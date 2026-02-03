WITH location_segments AS (
  SELECT *
  FROM ifta_report.location_segments
  WHERE date >= DATE_SUB(${start_date}, 1) AND date < ${end_date}
),

cd_total_distance AS (
  SELECT *
  FROM canonical_distance.total_distance
  WHERE date >= DATE_SUB(${start_date}, 1) AND date < ${end_date}
),

-- combine segments with canonical distance
location_segments_with_canonical_distance AS (
  SELECT
    loc.date,
    loc.start_ms,
    loc.end_ms,
    loc.org_id,
    loc.device_id,
    SUM(COALESCE(cd.distance, 0)) AS canonical_distance_meters,
    MAX(cd.osd_odometer) as odometer_end
  FROM location_segments AS loc
  LEFT JOIN cd_total_distance AS cd
    ON cd.time_ms >= loc.start_ms AND cd.time_ms < loc.end_ms
    AND cd.date = loc.date
    AND cd.org_id = loc.org_id
    AND cd.device_id = loc.device_id
  GROUP BY
    loc.date, loc.start_ms, loc.end_ms, loc.org_id, loc.device_id
),

-- fills as many null odo_ends as possible by getting the max value up to each row
location_segments_with_filled_odo_lookback AS (
  SELECT
    date,
    start_ms,
    end_ms,
    org_id,
    device_id,
    canonical_distance_meters,
    MAX(odometer_end) OVER (
      PARTITION BY org_id,
      device_id
      ORDER BY
        end_ms ROWS BETWEEN UNBOUNDED PRECEDING
        AND CURRENT ROW
    ) AS odometer_end
  FROM location_segments_with_canonical_distance
),

-- fills the remaining null odo_ends by getting the min value after each row
location_segments_with_filled_odo AS (
  SELECT
    date,
    start_ms,
    end_ms,
    org_id,
    device_id,
    canonical_distance_meters,
    MIN(odometer_end) OVER (
      PARTITION BY org_id,
      device_id
      ORDER BY
        end_ms ROWS BETWEEN CURRENT ROW
        AND UNBOUNDED FOLLOWING
    ) AS odometer_end
  FROM
    location_segments_with_filled_odo_lookback
),

-- assigns the start and end cumulative gps and odometer values
-- to each segment.
annotated_location_segments AS (
  SELECT
    date,
    org_id,
    device_id,
    start_ms,
    end_ms,
    value AS start_value,
    LEAD(value) OVER(w) AS end_value,
    DOUBLE(NULL) AS start_gps_meters,
    DOUBLE(NULL) AS end_gps_meters,
    -- if no row exists to get previous odo value, use this rows end as the start
    -- so we don't drop the row in our Ryder/Penske integration
    COALESCE(LAG(end_odo_meters) over (w), end_odo_meters) as start_odo_meters,
    end_odo_meters,
    canonical_distance_meters
   FROM (
    SELECT
      loc.*,
      COALESCE(cd.canonical_distance_meters, 0) AS canonical_distance_meters,
      cd.odometer_end as end_odo_meters
    FROM location_segments loc
    LEFT JOIN location_segments_with_filled_odo cd
      ON loc.date = cd.date
      AND loc.org_id = cd.org_id
      AND loc.device_id = cd.device_id
      AND loc.start_ms = cd.start_ms
      AND loc.end_ms = cd.end_ms
   )
   WINDOW w AS (PARTITION BY org_id, device_id ORDER BY end_ms)
 )

 SELECT *, CURRENT_TIMESTAMP() AS updated_at FROM annotated_location_segments
 WHERE date >= ${start_date} AND date < ${end_date}
