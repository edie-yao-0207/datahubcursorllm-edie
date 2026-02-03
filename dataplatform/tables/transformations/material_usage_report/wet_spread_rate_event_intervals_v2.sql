WITH spread_rates AS (
  SELECT
    org_id,
    object_id AS device_id,
    date,
    time,
    value.int_value AS spread_rate
  FROM
    kinesisstats.osdsaltspreadernormalizedspreadratewetmilliliterspermeter
  WHERE
    date >= DATE_SUB(${start_date}, 1)
    AND date < ${end_date}
    AND NOT value.is_databreak
    AND NOT value.is_end
    AND value.int_value >= 0
    AND value.int_value <= 282000
),
spreader_event_intervals AS (
  SELECT
    *
  FROM material_usage_report.spreader_event_intervals_v2
  WHERE date >= ${start_date}
  AND date < ${end_date}
),
spread_rates_within_events AS (
  SELECT
    sei.org_id,
    sei.device_id,
    sei.date,
    sei.spreader_start_ms,
    sei.spreader_end_ms,
    sei.mode,
    spread_rates.time AS spread_rate_time,
    spread_rate
  FROM
    spreader_event_intervals AS sei
    LEFT JOIN spread_rates ON spread_rates.time BETWEEN sei.spreader_start_ms
    AND sei.spreader_end_ms
    AND spread_rates.org_id = sei.org_id
    AND spread_rates.device_id = sei.device_id
),
-- Get all spreader events that don't have a spread rate at its start time,
spreader_event_intervals_without_starting_spread_rate AS (
  SELECT
    date,
    org_id,
    device_id,
    mode,
    spreader_start_ms,
    spreader_end_ms
  FROM
    (
      SELECT
        *,
        (org_id, device_id, spreader_start_ms) AS start_ms_pk
      FROM
        material_usage_report.spreader_event_intervals_v2
      WHERE
        date >= ${start_date}
        AND date < ${end_date}
    )
  WHERE
    start_ms_pk NOT IN (
      SELECT
        DISTINCT((org_id, device_id, spreader_start_ms))
      FROM
        spread_rates_within_events
      WHERE
        spreader_start_ms = spread_rate_time
    )
),
/*
 Find the latest spread rate before the spreader event start
 and set that as the spread rate at spreader event start
 */
spread_rates_before_events AS (
  SELECT
    intervals.org_id,
    intervals.device_id,
    intervals.date,
    spreader_start_ms,
    spreader_end_ms,
    mode,
    spread_rates.time AS spread_rate_time,
    spread_rate
  FROM
    spreader_event_intervals_without_starting_spread_rate intervals
    LEFT JOIN spread_rates ON spread_rates.time < intervals.spreader_start_ms
    AND spread_rates.time >= (
      intervals.spreader_start_ms - 1000 * 60
    )
    AND spread_rates.org_id = intervals.org_id
    AND spread_rates.device_id = intervals.device_id
),
spread_rates_at_event_start AS (
  SELECT
    org_id,
    device_id,
    date,
    spreader_start_ms,
    spreader_end_ms,
    mode,
    spreader_start_ms AS spread_rate_time,
    max((spread_rate_time, spread_rate)).spread_rate AS spread_rate
  FROM
    spread_rates_before_events
  GROUP BY
    org_id,
    device_id,
    date,
    spreader_start_ms,
    spreader_end_ms,
    mode
),
spreader_event_spread_rates AS (
  SELECT
    *
  FROM
    spread_rates_within_events
  UNION
  SELECT
    *
  FROM
    spread_rates_at_event_start
),
/*
 Build up individual spread rate intervals which represent
 time intervals within a spreader event interval when the spread
 rate was a certain value
 */
spread_rate_intervals AS (
  SELECT
    org_id,
    device_id,
    date,
    spreader_start_ms,
    spreader_end_ms,
    mode,
    spread_rate,
    spread_rate_start_ms AS spread_rate_event_start_ms,
    CASE
      WHEN next_spread_rate_start_ms < spreader_end_ms THEN next_spread_rate_start_ms
      ELSE spreader_end_ms
    END AS spread_rate_event_end_ms
  FROM
    (
      SELECT
        org_id,
        device_id,
        date,
        spreader_start_ms,
        spreader_end_ms,
        mode,
        spread_rate,
        spread_rate_time AS spread_rate_start_ms,
        LEAD(spread_rate_time) OVER (
          PARTITION by org_id,
          device_id
          ORDER BY
            spread_rate_time ASC
        ) AS next_spread_rate_start_ms
      FROM
        spreader_event_spread_rates
    )
    -- Exclude intervals where either computed spread rate is
    -- 0, as those are intervals where no material is used
    WHERE spread_rate > 0
),
spread_rate_intervals_with_hours AS (
  SELECT
    org_id,
    device_id,
    date,
    spreader_start_ms,
    spreader_end_ms,
    mode,
    spread_rate,
    spread_rate_event_start_ms,
    spread_rate_event_end_ms,
    HOUR AS hour_start,
    HOUR + INTERVAL 1 HOUR AS hour_end
  FROM
    (
      SELECT
        *,
        EXPLODE(
          SEQUENCE(
            date_trunc(
              'hour',
              from_unixtime(spread_rate_event_start_ms / 1000)
            ),
            date_trunc(
              'hour',
              from_unixtime(spread_rate_event_end_ms / 1000)
            ),
            INTERVAL 1 HOUR
          )
        ) AS HOUR
      FROM
        spread_rate_intervals
    )
),
-- Explode each spreader rate event by hour, so we can aggregate metrics by hour
spread_rate_intervals_exploded AS (
  SELECT
    org_id,
    device_id,
    date(hour_start) as date,
    spreader_start_ms,
    spreader_end_ms,
    mode,
    spread_rate,
    spread_rate_event_start_ms,
    spread_rate_event_end_ms,
    CASE
      WHEN spread_rate_event_start_ms < to_unix_timestamp(hour_start) * 1000 THEN to_unix_timestamp(hour_start) * 1000
      ELSE spread_rate_event_start_ms
    END AS spread_rate_start_ms,
    CASE
      WHEN spread_rate_event_end_ms > to_unix_timestamp(hour_end) * 1000 THEN to_unix_timestamp(hour_end) * 1000
      ELSE spread_rate_event_end_ms
    END AS spread_rate_end_ms
  FROM
    spread_rate_intervals_with_hours
),
canonical_distance AS (
  SELECT
    org_device.org_id,
    org_device.device_id,
    time_ms,
    date,
    distance
  FROM
    (
      SELECT
        (org_id, device_id) AS org_device,
        time_ms,
        date,
        distance
      FROM
        canonical_distance.total_distance
      WHERE
        date >= ${start_date}
        AND date < ${end_date}
    )
  WHERE
    org_device IN (
      SELECT
        *
      FROM
        material_usage_report.spreader_devices
    )
),
/*
 Join spread rate intervals with distance driven by device
 during those intervals
 */
spread_rate_intervals_exploded_with_distance AS (
  SELECT
    intervals.org_id,
    intervals.device_id,
    intervals.date,
    spreader_start_ms,
    spreader_end_ms,
    mode,
    spread_rate_event_start_ms,
    spread_rate_event_end_ms,
    spread_rate,
    spread_rate_start_ms,
    spread_rate_end_ms,
    COALESCE(SUM(distance), 0) AS distance_covered_meters,
    spread_rate_end_ms - spread_rate_start_ms AS time_spread_ms
  FROM
    spread_rate_intervals_exploded intervals
    LEFT JOIN canonical_distance ON time_ms >= spread_rate_start_ms
    AND time_ms < spread_rate_end_ms
    AND canonical_distance.org_id = intervals.org_id
    AND canonical_distance.device_id = intervals.device_id
    AND canonical_distance.date >= to_date(from_unixtime(spread_rate_start_ms / 1000))
    AND canonical_distance.date <= to_date(from_unixtime(spread_rate_end_ms / 1000))
  GROUP BY
    intervals.org_id,
    intervals.device_id,
    intervals.date,
    spreader_start_ms,
    spreader_end_ms,
    mode,
    spread_rate,
    spread_rate_event_start_ms,
    spread_rate_event_end_ms,
    spread_rate_start_ms,
    spread_rate_end_ms
)
SELECT
  date,
  org_id,
  device_id,
  spreader_start_ms,
  spreader_end_ms,
  mode,
  spread_rate_event_start_ms,
  spread_rate_event_end_ms,
  spread_rate,
  spread_rate_start_ms,
  spread_rate_end_ms,
  distance_covered_meters,
  CASE
    WHEN spread_rate IS NULL THEN NULL
    ELSE spread_rate * distance_covered_meters
  END AS wet_material_spread_ml,
  time_spread_ms
FROM
  spread_rate_intervals_exploded_with_distance
WHERE
  date >= ${start_date}
  AND date < ${end_date}
