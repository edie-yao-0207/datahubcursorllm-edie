WITH fuel_stats AS (
  SELECT *
  FROM kinesisstats.osdderivedfuelconsumed
  WHERE date >= DATE_SUB(${start_date}, 14)
  AND date < ${end_date}
  AND org_id NOT IN (SELECT org_id FROM helpers.ignored_org_ids)
)

-- Calculate difference between current and previous cumulative fuel consumption.
SELECT
  date,
  org_id,
  object_id,
  end_ms,
  -- If the duration of the interval is greater than a day, set the max duration of a fuel interval to be a day
  -- long. One case when this can happen is when a vehicle is turned on after a few days. This way
  -- we can limit the timerange of the engine state intervals with which the fuel intervals overlap to a day.
  GREATEST(start_ms, end_ms - 3600000 * 24) as start_ms,
  end_ms - start_ms as original_interval_duration_ms,
  fuel_consumed_ml
FROM (
  SELECT
    DATE(date) AS date,
    org_id,
    object_id,
    time AS end_ms,
    LAG(time) OVER (PARTITION BY org_id, object_id ORDER BY time ASC) AS start_ms,
    value.int_value - COALESCE(
      LAG(value.int_value) OVER (PARTITION BY org_id, object_id ORDER BY time ASC),
      0
    ) AS fuel_consumed_ml
  FROM fuel_stats
)
WHERE
  -- filter out rows with start_ms as null meaning there is no previous value so the interval overlaps with the day outside our range
  -- and we wouldn't be able to extract the fuel consumed delta.
  start_ms IS NOT NULL
  AND date >= ${start_date}
  AND date < ${end_date}
  AND fuel_consumed_ml >= 0


