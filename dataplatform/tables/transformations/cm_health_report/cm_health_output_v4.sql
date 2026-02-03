WITH health_setup AS (
  -- Use partition functions to generate a cumulative sum
  -- of times during the desired time frame.
  -- For each segment of drive time, filter down to rows in
  -- that segment and re-count.
  SELECT
    *,
    SUM(duration_ms) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS tot_trip_ms,
    SUM(interval_connected_duration_ms) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS tot_connected_ms,
    SUM(grace_recording_duration_ms) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS tot_recording_ms,
    SUM(vg_bootcount) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS tot_vg_bootcount,
    vg_build,
    SUM(cm_bootcount) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS tot_cm_bootcount,
    cm_build
  FROM
    cm_health_report.cm_recording_intervals_v4
  WHERE
    date >= date_sub(${end_date}, 30)
    AND date < ${end_date}
),
-- Compute segment-specific times for segments 1-5 and overall,
-- Note that segment 1 is closest to end_date and segment 5
-- is farthest in the past from end_date.
trip_time_overall AS(
  SELECT
    *
  FROM
    health_setup
  WHERE
    tot_trip_ms <= 50 * 3600000
),
trip_time_50 AS(
  SELECT
    *,
    SUM(duration_ms) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS seg_trip_ms,
    SUM(interval_connected_duration_ms) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS seg_connected_ms,
    SUM(grace_recording_duration_ms) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS seg_recording_ms,
    SUM(vg_bootcount) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS seg_vg_bootcount,
    vg_build,
    SUM(cm_bootcount) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS seg_cm_bootcount,
    cm_build
  FROM
    health_setup
  WHERE
    tot_trip_ms <= 50 * 3600000
    AND tot_trip_ms > 40 * 3600000
),
trip_time_40 AS(
  SELECT
    *,
    SUM(duration_ms) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS seg_trip_ms,
    SUM(interval_connected_duration_ms) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS seg_connected_ms,
    SUM(grace_recording_duration_ms) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS seg_recording_ms,
    SUM(vg_bootcount) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS seg_vg_bootcount,
    vg_build,
    SUM(cm_bootcount) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS seg_cm_bootcount,
    cm_build
  FROM
    health_setup
  WHERE
    tot_trip_ms <= 40 * 3600000
    AND tot_trip_ms > 30 * 3600000
),
trip_time_30 AS(
  SELECT
    *,
    SUM(duration_ms) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS seg_trip_ms,
    SUM(interval_connected_duration_ms) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS seg_connected_ms,
    SUM(grace_recording_duration_ms) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS seg_recording_ms,
    SUM(vg_bootcount) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS seg_vg_bootcount,
    vg_build,
    SUM(cm_bootcount) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS seg_cm_bootcount,
    cm_build
  FROM
    health_setup
  WHERE
    tot_trip_ms <= 30 * 3600000
    AND tot_trip_ms > 20 * 3600000
),
trip_time_20 AS(
  SELECT
    *,
    SUM(duration_ms) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS seg_trip_ms,
    SUM(interval_connected_duration_ms) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS seg_connected_ms,
    SUM(grace_recording_duration_ms) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS seg_recording_ms,
    SUM(vg_bootcount) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS seg_vg_bootcount,
    vg_build,
    SUM(cm_bootcount) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS seg_cm_bootcount,
    cm_build
  FROM
    health_setup
  WHERE
    tot_trip_ms <= 20 * 3600000
    AND tot_trip_ms > 10 * 3600000
),
trip_time_10 AS(
  SELECT
    *,
    SUM(duration_ms) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS seg_trip_ms,
    SUM(interval_connected_duration_ms) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS seg_connected_ms,
    SUM(grace_recording_duration_ms) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS seg_recording_ms,
    SUM(vg_bootcount) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS seg_vg_bootcount,
    vg_build,
    SUM(cm_bootcount) OVER (
      PARTITION BY org_id,
      device_id,
      cm_device_id
      ORDER BY
        start_ms DESC
    ) AS seg_cm_bootcount,
    cm_build
  FROM
    health_setup
  WHERE
    tot_trip_ms <= 10 * 3600000
),
-- For each of the 5 segments and overall,
-- Get the total times, which correspond to the max row.
-- Also determine when a segment starts/ends. We right join
-- with devices to capture devices that may not have had drive time.
overall_start_end_stats AS(
  SELECT
    org_id,
    device_id,
    -- Since segments are backwards looking, the start of a segment corresponds
    -- with the end_ms of the latest trip and the end of a segment corresponds
    -- with the start_ms of the earliest trip.
    MAX(end_ms) AS start_ms,
    MIN(start_ms) AS end_ms
  FROM
    trip_time_overall
  GROUP BY
    org_id,
    device_id
),
overall_table AS(
  SELECT
    vg_cm.org_id,
    vg_cm.vg_device_id AS device_id,
    vg_cm.linked_cm_id AS cm_device_id,
    vg_cm.cm_product_id,
    vg_cm.upper_camera_serial,
    s.start_ms,
    s.end_ms,
    tot_trip_ms,
    tot_connected_ms,
    tot_recording_ms,
    tot_recording_ms / tot_trip_ms AS uptime_pct,
    tot_vg_bootcount,
    FIRST(vg_build, true) OVER (
      PARTITION BY o.org_id,
      o.device_id,
      cm_device_id
      ORDER BY
        o.start_ms ASC
    ) AS vg_build_earliest,
    LAST(vg_build, true) OVER (
      PARTITION BY o.org_id,
      o.device_id,
      cm_device_id
      ORDER BY
        o.start_ms ASC
    ) AS vg_build_latest,
    tot_cm_bootcount,
    FIRST(cm_build, true) OVER (
      PARTITION BY o.org_id,
      o.device_id,
      cm_device_id
      ORDER BY
        o.start_ms ASC
    ) AS cm_build_earliest,
    LAST(cm_build, true) OVER (
      PARTITION BY o.org_id,
      o.device_id,
      cm_device_id
      ORDER BY
        o.start_ms ASC
    ) AS cm_build_latest
  FROM
    trip_time_overall AS o
    INNER JOIN overall_start_end_stats AS s ON o.device_id = s.device_id
    AND o.org_id = s.org_id
    RIGHT JOIN cm_health_report.cm_2x_3x_linked_vgs AS vg_cm ON vg_cm.vg_device_id = o.device_id
    AND vg_cm.org_id = o.org_id
  WHERE
    (o.org_id, o.device_id, tot_trip_ms) IN (
      SELECT
        org_id,
        device_id,
        MAX(tot_trip_ms)
      FROM
        trip_time_overall
      GROUP BY
        org_id,
        device_id
    )
    OR o.device_id IS null
),
50_start_end_stats AS(
  SELECT
    org_id,
    device_id,
    MAX(end_ms) AS start_ms,
    MIN(start_ms) AS end_ms
  FROM
    trip_time_50
  GROUP BY
    org_id,
    device_id
),
50_table AS(
  SELECT
    vg_cm.org_id,
    vg_cm.vg_device_id AS device_id,
    vg_cm.linked_cm_id AS cm_device_id,
    vg_cm.cm_product_id,
    vg_cm.upper_camera_serial,
    s.start_ms,
    s.end_ms,
    seg_trip_ms,
    seg_connected_ms,
    seg_recording_ms,
    seg_recording_ms / seg_trip_ms AS uptime_pct,
    seg_vg_bootcount,
    FIRST(vg_build, true) OVER (
      PARTITION BY o.org_id,
      o.device_id,
      cm_device_id
      ORDER BY
        o.start_ms ASC
    ) AS vg_build_earliest,
    LAST(vg_build, true) OVER (
      PARTITION BY o.org_id,
      o.device_id,
      cm_device_id
      ORDER BY
        o.start_ms ASC
    ) AS vg_build_latest,
    seg_cm_bootcount,
    FIRST(cm_build, true) OVER (
      PARTITION BY o.org_id,
      o.device_id,
      cm_device_id
      ORDER BY
        o.start_ms ASC
    ) AS cm_build_earliest,
    LAST(cm_build, true) OVER (
      PARTITION BY o.org_id,
      o.device_id,
      cm_device_id
      ORDER BY
        o.start_ms ASC
    ) AS cm_build_latest
  FROM
    trip_time_50 AS o
    INNER JOIN 50_start_end_stats AS s ON o.device_id = s.device_id
    AND o.org_id = s.org_id
    RIGHT JOIN cm_health_report.cm_2x_3x_linked_vgs AS vg_cm ON vg_cm.vg_device_id = o.device_id
    AND vg_cm.org_id = o.org_id
  WHERE
    (o.org_id, o.device_id, seg_trip_ms) IN (
      SELECT
        org_id,
        device_id,
        MAX(seg_trip_ms)
      FROM
        trip_time_50
      GROUP BY
        org_id,
        device_id
    )
    OR o.device_id IS null
),
40_start_end_stats AS(
  SELECT
    org_id,
    device_id,
    MAX(end_ms) AS start_ms,
    MIN(start_ms) AS end_ms
  FROM
    trip_time_40
  GROUP BY
    org_id,
    device_id
),
40_table AS(
  SELECT
    vg_cm.org_id,
    vg_cm.vg_device_id AS device_id,
    vg_cm.linked_cm_id AS cm_device_id,
    vg_cm.cm_product_id,
    vg_cm.upper_camera_serial,
    s.start_ms,
    s.end_ms,
    seg_trip_ms,
    seg_connected_ms,
    seg_recording_ms,
    seg_recording_ms / seg_trip_ms AS uptime_pct,
    seg_vg_bootcount,
    FIRST(vg_build, true) OVER (
      PARTITION BY o.org_id,
      o.device_id,
      cm_device_id
      ORDER BY
        o.start_ms ASC
    ) AS vg_build_earliest,
    LAST(vg_build, true) OVER (
      PARTITION BY o.org_id,
      o.device_id,
      cm_device_id
      ORDER BY
        o.start_ms ASC
    ) AS vg_build_latest,
    seg_cm_bootcount,
    FIRST(cm_build, true) OVER (
      PARTITION BY o.org_id,
      o.device_id,
      cm_device_id
      ORDER BY
        o.start_ms ASC
    ) AS cm_build_earliest,
    LAST(cm_build, true) OVER (
      PARTITION BY o.org_id,
      o.device_id,
      cm_device_id
      ORDER BY
        o.start_ms ASC
    ) AS cm_build_latest
  FROM
    trip_time_40 AS o
    INNER JOIN 40_start_end_stats AS s ON o.device_id = s.device_id
    AND o.org_id = s.org_id
    RIGHT JOIN cm_health_report.cm_2x_3x_linked_vgs AS vg_cm ON vg_cm.vg_device_id = o.device_id
    AND vg_cm.org_id = o.org_id
  WHERE
    (o.org_id, o.device_id, seg_trip_ms) IN (
      SELECT
        org_id,
        device_id,
        MAX(seg_trip_ms)
      FROM
        trip_time_40
      GROUP BY
        org_id,
        device_id
    )
    OR o.device_id IS null
),
30_start_end_stats AS(
  SELECT
    org_id,
    device_id,
    MAX(end_ms) AS start_ms,
    MIN(start_ms) AS end_ms
  FROM
    trip_time_30
  GROUP BY
    org_id,
    device_id
),
30_table AS(
  SELECT
    vg_cm.org_id,
    vg_cm.vg_device_id AS device_id,
    vg_cm.linked_cm_id AS cm_device_id,
    vg_cm.cm_product_id,
    vg_cm.upper_camera_serial,
    s.start_ms,
    s.end_ms,
    seg_trip_ms,
    seg_connected_ms,
    seg_recording_ms,
    seg_recording_ms / seg_trip_ms AS uptime_pct,
    seg_vg_bootcount,
    FIRST(vg_build, true) OVER (
      PARTITION BY o.org_id,
      o.device_id,
      cm_device_id
      ORDER BY
        o.start_ms ASC
    ) AS vg_build_earliest,
    LAST(vg_build, true) OVER (
      PARTITION BY o.org_id,
      o.device_id,
      cm_device_id
      ORDER BY
        o.start_ms ASC
    ) AS vg_build_latest,
    seg_cm_bootcount,
    FIRST(cm_build, true) OVER (
      PARTITION BY o.org_id,
      o.device_id,
      cm_device_id
      ORDER BY
        o.start_ms ASC
    ) AS cm_build_earliest,
    LAST(cm_build, true) OVER (
      PARTITION BY o.org_id,
      o.device_id,
      cm_device_id
      ORDER BY
        o.start_ms ASC
    ) AS cm_build_latest
  FROM
    trip_time_30 AS o
    INNER JOIN 30_start_end_stats AS s ON o.device_id = s.device_id
    AND o.org_id = s.org_id
    RIGHT JOIN cm_health_report.cm_2x_3x_linked_vgs AS vg_cm ON vg_cm.vg_device_id = o.device_id
    AND vg_cm.org_id = o.org_id
  WHERE
    (o.org_id, o.device_id, seg_trip_ms) IN (
      SELECT
        org_id,
        device_id,
        MAX(seg_trip_ms)
      FROM
        trip_time_30
      GROUP BY
        org_id,
        device_id
    )
    or o.device_id IS null
  ORDER BY
    seg_trip_ms DESC
),
20_start_end_stats AS(
  SELECT
    org_id,
    device_id,
    MAX(end_ms) AS start_ms,
    MIN(start_ms) AS end_ms
  FROM
    trip_time_20
  GROUP BY
    org_id,
    device_id
),
20_table AS(
  SELECT
    vg_cm.org_id,
    vg_cm.vg_device_id AS device_id,
    vg_cm.linked_cm_id AS cm_device_id,
    vg_cm.cm_product_id,
    vg_cm.upper_camera_serial,
    s.start_ms,
    s.end_ms,
    seg_trip_ms,
    seg_connected_ms,
    seg_recording_ms,
    seg_recording_ms / seg_trip_ms AS uptime_pct,
    seg_vg_bootcount,
    FIRST(vg_build, true) OVER (
      PARTITION BY o.org_id,
      o.device_id,
      cm_device_id
      ORDER BY
        o.start_ms ASC
    ) AS vg_build_earliest,
    LAST(vg_build, true) OVER (
      PARTITION BY o.org_id,
      o.device_id,
      cm_device_id
      ORDER BY
        o.start_ms ASC
    ) AS vg_build_latest,
    seg_cm_bootcount,
    FIRST(cm_build, true) OVER (
      PARTITION BY o.org_id,
      o.device_id,
      cm_device_id
      ORDER BY
        o.start_ms ASC
    ) AS cm_build_earliest,
    LAST(cm_build, true) OVER (
      PARTITION BY o.org_id,
      o.device_id,
      cm_device_id
      ORDER BY
        o.start_ms ASC
    ) AS cm_build_latest
  FROM
    trip_time_20 AS o
    INNER JOIN 20_start_end_stats AS s ON o.device_id = s.device_id
    AND o.org_id = s.org_id
    RIGHT JOIN cm_health_report.cm_2x_3x_linked_vgs AS vg_cm ON vg_cm.vg_device_id = o.device_id
    AND vg_cm.org_id = o.org_id
  WHERE
    (o.org_id, o.device_id, seg_trip_ms) IN (
      SELECT
        org_id,
        device_id,
        MAX(seg_trip_ms)
      FROM
        trip_time_20
      GROUP BY
        org_id,
        device_id
    )
    or o.device_id IS null
),
10_start_end_stats AS(
  SELECT
    org_id,
    device_id,
    MAX(end_ms) AS start_ms,
    MIN(start_ms) AS end_ms
  FROM
    trip_time_10
  GROUP BY
    org_id,
    device_id
),
10_table AS(
  SELECT
    vg_cm.org_id,
    vg_cm.vg_device_id AS device_id,
    vg_cm.linked_cm_id AS cm_device_id,
    vg_cm.cm_product_id,
    vg_cm.upper_camera_serial,
    s.start_ms,
    s.end_ms,
    seg_trip_ms,
    seg_connected_ms,
    seg_recording_ms,
    seg_recording_ms / seg_trip_ms AS uptime_pct,
    seg_vg_bootcount,
    FIRST(vg_build, true) OVER (
      PARTITION BY o.org_id,
      o.device_id,
      cm_device_id
      ORDER BY
        o.start_ms ASC
    ) AS vg_build_earliest,
    LAST(vg_build, true) OVER (
      PARTITION BY o.org_id,
      o.device_id,
      cm_device_id
      ORDER BY
        o.start_ms ASC
    ) AS vg_build_latest,
    seg_cm_bootcount,
    FIRST(cm_build, true) OVER (
      PARTITION BY o.org_id,
      o.device_id,
      cm_device_id
      ORDER BY
        o.start_ms ASC
    ) AS cm_build_earliest,
    LAST(cm_build, true) OVER (
      PARTITION BY o.org_id,
      o.device_id,
      cm_device_id
      ORDER BY
        o.start_ms ASC
    ) AS cm_build_latest
  FROM
    trip_time_10 AS o
    INNER JOIN 10_start_end_stats AS s ON o.device_id = s.device_id
    AND o.org_id = s.org_id
    RIGHT JOIN cm_health_report.cm_2x_3x_linked_vgs AS vg_cm ON vg_cm.vg_device_id = o.device_id
    AND vg_cm.org_id = o.org_id
  WHERE
    (o.org_id, o.device_id, seg_trip_ms) IN (
      SELECT
        org_id,
        device_id,
        MAX(seg_trip_ms)
      FROM
        trip_time_10
      GROUP BY
        org_id,
        device_id
    )
    or o.device_id IS null
),
-- Add a 5 minute buffer since there's a chance that the still is generated
-- before the backend has a chance to mark the trip as started.
cm_obstructed_on_last_trip AS (
  SELECT
    ob.org_id,
    ob.device_id,
    ob.latest_outward_detection_ms AS outward_detection_ms,
    ob.latest_inward_detection_ms AS inward_detection_ms,
    ob.latest_misaligned_detection_ms AS misaligned_detection_ms
  FROM
    cm_health_report.cm_obstructions AS ob
    LEFT JOIN cm_health_report.cm_obstructed_last_trip AS t ON t.org_id = ob.org_id AND t.device_id = ob.device_id
    JOIN cm_health_report.cm_recording_intervals_v4 ri ON ri.org_id = ob.org_id AND ri.device_id = ob.device_id
  WHERE
    (
      ri.cm_product_id IN (30, 31) -- cm2x
      OR ri.cm_product_id IN (43, 44, 167, 155) --cm3x
    )
    AND (
      ob.latest_outward_detection_ms >= t.trip_start_ms - (5 * 60000)
      OR ob.latest_inward_detection_ms >= t.trip_start_ms - (5 * 60000)
      OR ob.latest_misaligned_detection_ms >= t.trip_start_ms - (5 * 60000)
    )
),
-- Join each individual segment and overall data into a single output table.
cm_health_output_staging AS (
  SELECT
    f.org_id,
    f.device_id,
    f.cm_device_id,
    f.cm_product_id,
    f.upper_camera_serial,
    -- With overwrite mode, we can only modify partitions where start_date <= date < end_date.
    -- Because end_date is exclusive, subtract 1 to make it inclusive.
    cast(date_sub(${end_date}, 1) as date) as date,
    COALESCE(s1.seg_trip_ms, 0) AS s1_trip_ms,
    COALESCE(s1.seg_connected_ms, 0) AS s1_connected_ms,
    COALESCE(s1.seg_recording_ms, 0) AS s1_recording_ms,
    COALESCE(s1.uptime_pct, 0) AS s1_uptime_pct,
    COALESCE(s2.seg_trip_ms, 0) AS s2_trip_ms,
    COALESCE(s2.seg_connected_ms, 0) AS s2_connected_ms,
    COALESCE(s2.seg_recording_ms, 0) AS s2_recording_ms,
    COALESCE(s2.uptime_pct, 0) AS s2_uptime_pct,
    COALESCE(s3.seg_trip_ms, 0) AS s3_trip_ms,
    COALESCE(s3.seg_connected_ms, 0) AS s3_connected_ms,
    COALESCE(s3.seg_recording_ms, 0) AS s3_recording_ms,
    COALESCE(s3.uptime_pct, 0) AS s3_uptime_pct,
    COALESCE(s4.seg_trip_ms, 0) AS s4_trip_ms,
    COALESCE(s4.seg_connected_ms, 0) AS s4_connected_ms,
    COALESCE(s4.seg_recording_ms, 0) AS s4_recording_ms,
    COALESCE(s4.uptime_pct, 0) AS s4_uptime_pct,
    COALESCE(s5.seg_trip_ms, 0) AS s5_trip_ms,
    COALESCE(s5.seg_connected_ms, 0) AS s5_connected_ms,
    COALESCE(s5.seg_recording_ms, 0) AS s5_recording_ms,
    COALESCE(s5.uptime_pct, 0) AS s5_uptime_pct,
    COALESCE(f.tot_trip_ms, 0) AS overall_trip_ms,
    COALESCE(f.tot_connected_ms, 0) AS overall_connected_ms,
    COALESCE(f.tot_recording_ms, 0) AS overall_recording_ms,
    COALESCE(f.uptime_pct, 0) AS overall_uptime_pct,
    COALESCE(s1.seg_vg_bootcount, 0) AS s1_vg_bootcount,
    s1.vg_build_earliest AS s1_vg_build_earliest,
    s1.vg_build_latest AS s1_vg_build_latest,
    COALESCE(s1.seg_cm_bootcount, 0) AS s1_cm_bootcount,
    s1.cm_build_earliest AS s1_cm_build_earliest,
    s1.cm_build_latest AS s1_cm_build_latest,
    COALESCE(s2.seg_vg_bootcount, 0) AS s2_vg_bootcount,
    s2.vg_build_earliest AS s2_vg_build_earliest,
    s2.vg_build_latest AS s2_vg_build_latest,
    COALESCE(s2.seg_cm_bootcount, 0) AS s2_cm_bootcount,
    s2.cm_build_earliest AS s2_cm_build_earliest,
    s2.cm_build_latest AS s2_cm_build_latest,
    COALESCE(s3.seg_vg_bootcount, 0) AS s3_vg_bootcount,
    s3.vg_build_earliest AS s3_vg_build_earliest,
    s3.vg_build_latest AS s3_vg_build_latest,
    COALESCE(s3.seg_cm_bootcount, 0) AS s3_cm_bootcount,
    s3.cm_build_earliest AS s3_cm_build_earliest,
    s3.cm_build_latest AS s3_cm_build_latest,
    COALESCE(s4.seg_vg_bootcount, 0) AS s4_vg_bootcount,
    s4.vg_build_earliest AS s4_vg_build_earliest,
    s4.vg_build_latest AS s4_vg_build_latest,
    COALESCE(s4.seg_cm_bootcount, 0) AS s4_cm_bootcount,
    s4.cm_build_earliest AS s4_cm_build_earliest,
    s4.cm_build_latest AS s4_cm_build_latest,
    COALESCE(s5.seg_vg_bootcount, 0) AS s5_vg_bootcount,
    s5.vg_build_earliest AS s5_vg_build_earliest,
    s5.vg_build_latest AS s5_vg_build_latest,
    COALESCE(s5.seg_cm_bootcount, 0) AS s5_cm_bootcount,
    s5.cm_build_earliest AS s5_cm_build_earliest,
    s5.cm_build_latest AS s5_cm_build_latest,
    COALESCE(f.tot_vg_bootcount, 0) AS overall_vg_bootcount,
    f.vg_build_earliest AS overall_vg_build_earliest,
    f.vg_build_latest AS overall_vg_build_latest,
    COALESCE(f.tot_cm_bootcount, 0) AS overall_cm_bootcount,
    f.cm_build_earliest AS overall_cm_build_earliest,
    f.cm_build_latest AS overall_cm_build_latest,
    COALESCE(ob.outward_detection_ms, 0) AS outward_detection_ms,
    COALESCE(ob.inward_detection_ms, 0) AS inward_detection_ms,
    COALESCE(ob.misaligned_detection_ms, 0) AS misaligned_detection_ms,
    COALESCE(s1.start_ms, 0) AS s1_start_ms,
    COALESCE(s1.end_ms, 0) AS s1_end_ms,
    COALESCE(s2.start_ms, 0) AS s2_start_ms,
    COALESCE(s2.end_ms, 0) AS s2_end_ms,
    COALESCE(s3.start_ms, 0) AS s3_start_ms,
    COALESCE(s3.end_ms, 0) AS s3_end_ms,
    COALESCE(s4.start_ms, 0) AS s4_start_ms,
    COALESCE(s4.end_ms, 0) AS s4_end_ms,
    COALESCE(s5.start_ms, 0) AS s5_start_ms,
    COALESCE(s5.end_ms, 0) AS s5_end_ms,
    COALESCE(f.start_ms, 0) AS overall_start_ms,
    COALESCE(f.end_ms, 0) AS overall_end_ms,
    ROW_NUMBER() OVER (PARTITION BY f.org_id, f.device_id ORDER BY COALESCE(f.start_ms, 0) DESC) AS rnk
  FROM
    overall_table f
    INNER JOIN 40_table s4 ON f.device_id = s4.device_id
    AND f.org_id = s4.org_id
    INNER JOIN 30_table s3 ON f.device_id = s3.device_id
    AND f.org_id = s3.org_id
    INNER JOIN 20_table s2 ON f.device_id = s2.device_id
    AND f.org_id = s2.org_id
    INNER JOIN 10_table s1 ON f.device_id = s1.device_id
    AND f.org_id = s1.org_id
    INNER JOIN 50_table s5 ON f.device_id = s5.device_id
    AND f.org_id = s5.org_id
    LEFT JOIN cm_obstructed_on_last_trip AS ob ON f.device_id = ob.device_id
    AND f.org_id = ob.org_id
  )

    SELECT
      org_id,
      device_id,
      cm_device_id,
      cm_product_id,
      upper_camera_serial,
      -- With overwrite mode, we can only modify partitions where start_date <= date < end_date.
      -- Because end_date is exclusive, subtract 1 to make it inclusive.
      date,
      s1_trip_ms,
      s1_connected_ms,
      s1_recording_ms,
      s1_uptime_pct,
      s2_trip_ms,
      s2_connected_ms,
      s2_recording_ms,
      s2_uptime_pct,
      s3_trip_ms,
      s3_connected_ms,
      s3_recording_ms,
      s3_uptime_pct,
      s4_trip_ms,
      s4_connected_ms,
      s4_recording_ms,
      s4_uptime_pct,
      s5_trip_ms,
      s5_connected_ms,
      s5_recording_ms,
      s5_uptime_pct,
      overall_trip_ms,
      overall_connected_ms,
      overall_recording_ms,
      overall_uptime_pct,
      s1_vg_bootcount,
      s1_vg_build_earliest,
      s1_vg_build_latest,
      s1_cm_bootcount,
      s1_cm_build_earliest,
      s1_cm_build_latest,
      s2_vg_bootcount,
      s2_vg_build_earliest,
      s2_vg_build_latest,
      s2_cm_bootcount,
      s2_cm_build_earliest,
      s2_cm_build_latest,
      s3_vg_bootcount,
      s3_vg_build_earliest,
      s3_vg_build_latest,
      s3_cm_bootcount,
      s3_cm_build_earliest,
      s3_cm_build_latest,
      s4_vg_bootcount,
      s4_vg_build_earliest,
      s4_vg_build_latest,
      s4_cm_bootcount,
      s4_cm_build_earliest,
      s4_cm_build_latest,
      s5_vg_bootcount,
      s5_vg_build_earliest,
      s5_vg_build_latest,
      s5_cm_bootcount,
      s5_cm_build_earliest,
      s5_cm_build_latest,
      overall_vg_bootcount,
      overall_vg_build_earliest,
      overall_vg_build_latest,
      overall_cm_bootcount,
      overall_cm_build_earliest,
      overall_cm_build_latest,
      outward_detection_ms,
      inward_detection_ms,
      misaligned_detection_ms,
      s1_start_ms,
      s1_end_ms,
      s2_start_ms,
      s2_end_ms,
      s3_start_ms,
      s3_end_ms,
      s4_start_ms,
      s4_end_ms,
      s5_start_ms,
      s5_end_ms,
      overall_start_ms,
      overall_end_ms
  FROM cm_health_output_staging
  WHERE
    rnk=1
