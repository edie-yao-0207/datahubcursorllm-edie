-- TLDR these queries will produce a average weighted weight on 1 hour interval and the amount of time when the weight has been measure,
-- With this data we can produce the average weight of the vehicle in that 1 hour interval or do some aggregation
-- weight_windows will fetch the weights that we have data for it and add the next time to each row as a end_time, this will allow us to calculate the intervals
with weight_windows AS (
  SELECT
    vw.org_id,
    vw.object_id,
    COALESCE (vw.value.int_value, 0) AS weight,
    vw.time AS start_ms,
    vw.value.is_end,
    vw.value.is_databreak,


    LEAD(vw.time) OVER (PARTITION BY vw.org_id, vw.object_id ORDER BY vw.time) AS end_ms

  FROM kinesisstats.osdj1939cvwgrosscombinationvehicleweightkilograms AS vw
  WHERE
    vw.value.is_end = false
    AND vw.value.is_databreak = false -- we ignore data points that are end or databreaks since they don't have valid data.
    AND vw.value.int_value > 0 -- we just care about measurements where the weight is greater than zero
    AND vw.value.int_value < 100000 -- we filter out clearly invalid numbers (firmware will work on improving the reported stats under EVEC-284)
    AND vw.date  >= DATE_SUB(${start_date}, 1)
    AND vw.date  < ${end_date}
),

-- engine_on_intervals selects the periods when the engine state was ON
engine_on_intervals AS (
  SELECT
    ed.org_id,
    ed.object_id,
    ed.start_ms,
    ed.end_ms,
    ed.state
  FROM engine_state.intervals AS ed
  WHERE
    ed.end_ms IS NOT NULL
    AND ed.state = "ON" -- we just care about engine ON
    AND ed.date  >= DATE_SUB(${start_date}, 1)
    AND ed.date  < ${end_date}
  ),

-- weight_intervals joins the weight_windows (ww) with the engine_on_intervals (eoi) matching the time when we have overlap
-- with the engine intervals and the weight data.
weight_intervals AS (
  SELECT DISTINCT -- We use DISTINCT since our join can create duplicates
    ww.org_id,
    ww.object_id,
    ww.weight,
    greatest(ww.start_ms,eoi.start_ms) AS start_time_ms,
    least(ww.end_ms, eoi.end_ms) AS end_time_ms
  FROM
    weight_windows AS ww JOIN engine_on_intervals AS eoi
    ON ww.object_id = eoi.object_id
      AND ww.org_id = eoi.org_id
      AND (
        (ww.start_ms <= eoi.start_ms AND ww.end_ms >= eoi.end_ms) -- ww larger window than ed
        OR (ww.start_ms >= eoi.start_ms AND ww.end_ms <= eoi.end_ms) -- ww started and finished before ed
        OR (ww.start_ms <= eoi.start_ms AND ww.end_ms <= eoi.end_ms AND ww.end_ms >= eoi.start_ms) -- ww started before ed
        OR (ww.start_ms >= eoi.start_ms AND ww.end_ms >= eoi.end_ms AND ww.start_ms <= eoi.end_ms ) -- ww started after ed started but before it has finished
      )
  WHERE
    ww.end_ms IS NOT NULL -- We ignore rows that we don't have a next value and the end of the interval is NULL.
    ),

-- weight_intervals_by_hour is the table that explodes the intervals into 1 hour buckets and make the weight of that bucket the value measured on the interval. This will help us to aggregate the data into 1 hour buckets.
 weight_intervals_by_hour AS (
  SELECT
    wi.org_id,
    wi.object_id,
    wi.weight,
    wi.start_time_ms AS start_ms,
    wi.end_time_ms AS end_ms,
    EXPLODE(
      SEQUENCE(
        DATE_TRUNC('hour', from_unixtime(wi.start_time_ms / 1000)),
        DATE_TRUNC('hour', from_unixtime(wi.end_time_ms / 1000)),
        INTERVAL 1 hour
      )
    ) AS hour_start
  FROM weight_intervals as wi),

-- weight_intervals_by_hour_with_correct_dates os the table that will check the duration of weight measured in each interval.
-- We have the start and end times of that interval, but we should consider the correct start and end dates based on the measured ones.
-- In this table we select the start and end dates based on the 1 hour bucket times.
-- We also create a temporary column with the duration of the period when we had the weight (this will be used later for the average weighet weight.
weight_intervals_by_hour_with_correct_dates AS (
  SELECT
    wibh.org_id,
    wibh.object_id,
    wibh.weight,
    greatest(wibh.start_ms, unix_timestamp(hour_start) * 1000) AS start_ms, -- start should be the greatest between the interval start or the 1 hour bucket start.
    least(wibh.end_ms, ( unix_timestamp(hour_start + interval 1 hour) )*1000) AS end_ms, -- end should be the least between the interval end or the 1 hour bucket end.
    least(wibh.end_ms, ( unix_timestamp(hour_start + interval 1 hour) )*1000) - greatest(wibh.start_ms, unix_timestamp(hour_start)*1000) AS interval_duration,
    hour_start
  FROM weight_intervals_by_hour as wibh)

-- We group the weights measured in each bucket into only one value per bucket:
-- weighted_weight is SUM((duration of period) *(weight in the period))
-- weight_time is SUM ((duration of period)
-- to have Average weighted weight of that period we can calculate with weighted_weight/weight_time
SELECT
  DATE(wibhwcd.hour_start) AS date,
  wibhwcd.org_id,
  wibhwcd.object_id AS device_id,
  wibhwcd.hour_start interval_start,
  wibhwcd.hour_start + interval 1 hour AS interval_end,
  SUM(COALESCE (wibhwcd.weight,0) * COALESCE (wibhwcd.interval_duration,0)) AS weighted_weight,
  SUM(COALESCE (wibhwcd.interval_duration,0)) AS weight_time
FROM weight_intervals_by_hour_with_correct_dates as wibhwcd
WHERE
  DATE(wibhwcd.hour_start) >= ${start_date}
  AND DATE(wibhwcd.hour_start) < ${end_date}

GROUP BY wibhwcd.org_id, wibhwcd.object_id, wibhwcd.hour_start

