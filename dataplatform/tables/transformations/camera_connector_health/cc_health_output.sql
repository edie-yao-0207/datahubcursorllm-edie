WITH currently_connected_cm_per_vg AS (
  SELECT
    devices.id AS linked_vg_id,
    gateways.device_id AS linked_cm_id
  FROM
    productsdb.devices devices
    -- For auxcams, the cm_id could be null, so we left join with for potential CM gateway
    LEFT JOIN productsdb.gateways gateways ON upper(replace(gateways.serial, '-', '')) = upper(replace(devices.camera_serial, '-', ''))
    AND devices.org_id = gateways.org_id
  WHERE
    -- For auxcams, the cm_id could be null, so the product_id would also be null
    gateways.product_id IN (
      43,  -- CM32
      44,  -- CM31
      155, -- CM34 BrigidDual-256GB
      167  -- CM33 BrigidSingle
      ) OR gateways.product_id is null
),
current_cm_connected_and_recording_durations AS (
  SELECT
    *
  FROM
    camera_connector_health.combined_connected_and_recording_durations
    INNER JOIN currently_connected_cm_per_vg ON vg_id = linked_vg_id and
    (cm_id=linked_cm_id or (cm_id is null and linked_cm_id is null))
),
connected_and_recording_with_aggregate_trip_time AS (
  SELECT
    *,
    sum(trip_duration_ms) OVER (
      partition BY vg_id,
      cm_id
      ORDER BY
        trip_start_ms DESC
    ) AS total_trip_ms
  FROM
    current_cm_connected_and_recording_durations
  WHERE
    date >= date_sub(${end_date}, 30)
    AND date < ${end_date}
),
-- Filter out the ones that are more than 50 hours of trip time before
connected_and_recording_last_50_hours_trip_time AS (
  SELECT
    *
  FROM
    connected_and_recording_with_aggregate_trip_time
  WHERE
    total_trip_ms <= 50 * 3600000
),
-- Find the aggregate trip, connected, and recording time for the last 50 hours of trip time
total_ignition_on_durations_per_vg AS (
  SELECT
    org_id,
    vg_id,
    cm_id,
    min(trip_start_ms) AS start_ms,
    max(trip_start_ms + trip_duration_ms) AS end_ms,
    sum(trip_duration_ms) AS trip_duration_ms,
    sum(connected_ms) AS connected_ms,
    sum(recording_ms) AS recording_ms
  FROM
    connected_and_recording_last_50_hours_trip_time
  GROUP BY
    org_id,
    vg_id,
    cm_id
),
-- Combine with all the reversing intervals that occured within the last 50 hours of trip time
total_connected_durations_overlapping_reversing AS (
  SELECT
    totals.org_id,
    totals.vg_id,
    totals.cm_id,
    totals.start_ms,
    totals.end_ms,
    totals.trip_duration_ms,
    totals.connected_ms,
    totals.recording_ms,
    reversing.reversing_duration_ms,
    reversing.reversing_start_ms,
    reversing.recording_per_reversing_ms
  FROM
    total_ignition_on_durations_per_vg as totals
    LEFT JOIN camera_connector_health.cc_reversing_and_recording_durations AS reversing ON totals.vg_id = reversing.vg_id
    AND totals.org_id = reversing.org_id
    AND NOT (
      reversing.reversing_start_ms >= totals.end_ms
      OR (
        reversing.reversing_start_ms + reversing.reversing_duration_ms
      ) <= totals.start_ms
    )
),
-- Aggregate all the reversing intervals for the last 50 hours of trip time.
-- If there are reversing intervals, use only the recording per reversing as the uptime pct.
-- Otherwise, use the standard recording per connected time during a trip.
total_combined_reversing_and_connected_durations AS (
  SELECT
    org_id,
    vg_id,
    cm_id,
    start_ms,
    end_ms,
    trip_duration_ms,
    connected_ms,
    recording_ms,
    sum(reversing_duration_ms) AS reversing_ms,
    sum(recording_per_reversing_ms) AS recording_per_reversing_ms,
    CASE
      WHEN sum(reversing_duration_ms) IS NULL THEN recording_ms / connected_ms
      ELSE sum(recording_per_reversing_ms) / sum(reversing_duration_ms)
    END AS uptime_pct
  FROM
    total_connected_durations_overlapping_reversing
  GROUP BY
    org_id,
    vg_id,
    cm_id,
    start_ms,
    end_ms,
    trip_duration_ms,
    connected_ms,
    recording_ms
),
-- Calculate the segment bounds per VG between 40 and 50 hours of trip time ago
s5_ignition_on_durations_per_vg as (
  SELECT
    org_id,
    vg_id,
    cm_id,
    min(trip_start_ms) AS start_ms,
    max(trip_start_ms + trip_duration_ms) AS end_ms,
    sum(trip_duration_ms) AS trip_duration_ms,
    sum(connected_ms) AS connected_ms,
    sum(recording_ms) AS recording_ms
  FROM
    connected_and_recording_last_50_hours_trip_time as trip_totals
  WHERE
    trip_totals.total_trip_ms > 40 * 3600000
    AND trip_totals.total_trip_ms <= 50 * 3600000
  GROUP BY
    org_id,
    vg_id,
    cm_id
),
-- Combine with all the reversing intervals that occured between 40 and 50 hours of trip time ago
s5_connected_durations_overlapping_reversing AS (
  SELECT
    totals.org_id,
    totals.vg_id,
    totals.cm_id,
    totals.start_ms,
    totals.end_ms,
    totals.trip_duration_ms,
    totals.connected_ms,
    totals.recording_ms,
    reversing.reversing_duration_ms,
    reversing.reversing_start_ms,
    reversing.recording_per_reversing_ms
  FROM
    s5_ignition_on_durations_per_vg as totals
    LEFT JOIN camera_connector_health.cc_reversing_and_recording_durations AS reversing ON totals.vg_id = reversing.vg_id
    AND totals.org_id = reversing.org_id
    AND NOT (
      reversing.reversing_start_ms >= totals.end_ms
      OR (
        reversing.reversing_start_ms + reversing.reversing_duration_ms
      ) <= totals.start_ms
    )
),
-- Aggregate all the reversing intervals between 40 and 50 hours of trip time ago.
-- If there are reversing intervals, use only the recording per reversing as the uptime pct.
-- Otherwise, use the standard recording per connected time during a trip.
s5_combined_reversing_and_connected_durations AS (
  SELECT
    org_id,
    vg_id,
    cm_id,
    start_ms,
    end_ms,
    trip_duration_ms,
    connected_ms,
    recording_ms,
    sum(reversing_duration_ms) AS reversing_ms,
    sum(recording_per_reversing_ms) AS recording_per_reversing_ms,
    CASE
      WHEN sum(reversing_duration_ms) IS NULL THEN recording_ms / connected_ms
      ELSE sum(recording_per_reversing_ms) / sum(reversing_duration_ms)
    END AS uptime_pct
  FROM
    s5_connected_durations_overlapping_reversing
  GROUP BY
    org_id,
    vg_id,
    cm_id,
    start_ms,
    end_ms,
    trip_duration_ms,
    connected_ms,
    recording_ms
),
-- Calculate the segment bounds per VG between 30 and 40 hours of trip time ago
s4_ignition_on_durations_per_vg as (
  SELECT
    org_id,
    vg_id,
    cm_id,
    min(trip_start_ms) AS start_ms,
    max(trip_start_ms + trip_duration_ms) AS end_ms,
    sum(trip_duration_ms) AS trip_duration_ms,
    sum(connected_ms) AS connected_ms,
    sum(recording_ms) AS recording_ms
  FROM
    connected_and_recording_last_50_hours_trip_time as trip_totals
  WHERE
    trip_totals.total_trip_ms > 30 * 3600000
    AND trip_totals.total_trip_ms <= 40 * 3600000
  GROUP BY
    org_id,
    vg_id,
    cm_id
),
-- Combine with all the reversing intervals that occured between 30 and 40 hours of trip time ago
s4_connected_durations_overlapping_reversing AS (
  SELECT
    totals.org_id,
    totals.vg_id,
    totals.cm_id,
    totals.start_ms,
    totals.end_ms,
    totals.trip_duration_ms,
    totals.connected_ms,
    totals.recording_ms,
    reversing.reversing_duration_ms,
    reversing.reversing_start_ms,
    reversing.recording_per_reversing_ms
  FROM
    s4_ignition_on_durations_per_vg as totals
    LEFT JOIN camera_connector_health.cc_reversing_and_recording_durations AS reversing ON totals.vg_id = reversing.vg_id
    AND totals.org_id = reversing.org_id
    AND NOT (
      reversing.reversing_start_ms >= totals.end_ms
      OR (
        reversing.reversing_start_ms + reversing.reversing_duration_ms
      ) <= totals.start_ms
    )
),
-- Aggregate all the reversing intervals between 30 and 40 hours of trip time ago.
-- If there are reversing intervals, use only the recording per reversing as the uptime pct.
-- Otherwise, use the standard recording per connected time during a trip.
s4_combined_reversing_and_connected_durations AS (
  SELECT
    org_id,
    vg_id,
    cm_id,
    start_ms,
    end_ms,
    trip_duration_ms,
    connected_ms,
    recording_ms,
    sum(reversing_duration_ms) AS reversing_ms,
    sum(recording_per_reversing_ms) AS recording_per_reversing_ms,
    CASE
      WHEN sum(reversing_duration_ms) IS NULL THEN recording_ms / connected_ms
      ELSE sum(recording_per_reversing_ms) / sum(reversing_duration_ms)
    END AS uptime_pct
  FROM
    s4_connected_durations_overlapping_reversing
  GROUP BY
    org_id,
    vg_id,
    cm_id,
    start_ms,
    end_ms,
    trip_duration_ms,
    connected_ms,
    recording_ms
),
-- Calculate the segment bounds per VG between 20 and 30 hours of trip time ago
s3_ignition_on_durations_per_vg as (
  SELECT
    org_id,
    vg_id,
    cm_id,
    min(trip_start_ms) AS start_ms,
    max(trip_start_ms + trip_duration_ms) AS end_ms,
    sum(trip_duration_ms) AS trip_duration_ms,
    sum(connected_ms) AS connected_ms,
    sum(recording_ms) AS recording_ms
  FROM
    connected_and_recording_last_50_hours_trip_time as trip_totals
  WHERE
    trip_totals.total_trip_ms > 20 * 3600000
    AND trip_totals.total_trip_ms <= 30 * 3600000
  GROUP BY
    org_id,
    vg_id,
    cm_id
),
-- Combine with all the reversing intervals that occured between 20 and 30 hours of trip time ago
s3_connected_durations_overlapping_reversing AS (
  SELECT
    totals.org_id,
    totals.vg_id,
    totals.cm_id,
    totals.start_ms,
    totals.end_ms,
    totals.trip_duration_ms,
    totals.connected_ms,
    totals.recording_ms,
    reversing.reversing_duration_ms,
    reversing.reversing_start_ms,
    reversing.recording_per_reversing_ms
  FROM
    s3_ignition_on_durations_per_vg as totals
    LEFT JOIN camera_connector_health.cc_reversing_and_recording_durations AS reversing ON totals.vg_id = reversing.vg_id
    AND totals.org_id = reversing.org_id
    AND NOT (
      reversing.reversing_start_ms >= totals.end_ms
      OR (
        reversing.reversing_start_ms + reversing.reversing_duration_ms
      ) <= totals.start_ms
    )
),
-- Aggregate all the reversing intervals between 40 and 50 hours of trip time ago.
-- If there are reversing intervals, use only the recording per reversing as the uptime pct.
-- Otherwise, use the standard recording per connected time during a trip.
s3_combined_reversing_and_connected_durations AS (
  SELECT
    org_id,
    vg_id,
    cm_id,
    start_ms,
    end_ms,
    trip_duration_ms,
    connected_ms,
    recording_ms,
    sum(reversing_duration_ms) AS reversing_ms,
    sum(recording_per_reversing_ms) AS recording_per_reversing_ms,
    CASE
      WHEN sum(reversing_duration_ms) IS NULL THEN recording_ms / connected_ms
      ELSE sum(recording_per_reversing_ms) / sum(reversing_duration_ms)
    END AS uptime_pct
  FROM
    s3_connected_durations_overlapping_reversing
  GROUP BY
    org_id,
    vg_id,
    cm_id,
    start_ms,
    end_ms,
    trip_duration_ms,
    connected_ms,
    recording_ms
),
-- Calculate the segment bounds per VG between 10 and 20 hours of trip time ago
s2_ignition_on_durations_per_vg as (
  SELECT
    org_id,
    vg_id,
    cm_id,
    min(trip_start_ms) AS start_ms,
    max(trip_start_ms + trip_duration_ms) AS end_ms,
    sum(trip_duration_ms) AS trip_duration_ms,
    sum(connected_ms) AS connected_ms,
    sum(recording_ms) AS recording_ms
  FROM
    connected_and_recording_last_50_hours_trip_time as trip_totals
  WHERE
    trip_totals.total_trip_ms > 10 * 3600000
    AND trip_totals.total_trip_ms <= 20 * 3600000
  GROUP BY
    org_id,
    vg_id,
    cm_id
),
-- Combine with all the reversing intervals that occured between 10 and 20 hours of trip time ago
s2_connected_durations_overlapping_reversing AS (
  SELECT
    totals.org_id,
    totals.vg_id,
    totals.cm_id,
    totals.start_ms,
    totals.end_ms,
    totals.trip_duration_ms,
    totals.connected_ms,
    totals.recording_ms,
    reversing.reversing_duration_ms,
    reversing.reversing_start_ms,
    reversing.recording_per_reversing_ms
  FROM
    s2_ignition_on_durations_per_vg as totals
    LEFT JOIN camera_connector_health.cc_reversing_and_recording_durations AS reversing ON totals.vg_id = reversing.vg_id
    AND totals.org_id = reversing.org_id
    AND NOT (
      reversing.reversing_start_ms >= totals.end_ms
      OR (
        reversing.reversing_start_ms + reversing.reversing_duration_ms
      ) <= totals.start_ms
    )
),
-- Aggregate all the reversing intervals between 10 and 20 hours of trip time ago.
-- If there are reversing intervals, use only the recording per reversing as the uptime pct.
-- Otherwise, use the standard recording per connected time during a trip.
s2_combined_reversing_and_connected_durations AS (
  SELECT
    org_id,
    vg_id,
    cm_id,
    start_ms,
    end_ms,
    trip_duration_ms,
    connected_ms,
    recording_ms,
    sum(reversing_duration_ms) AS reversing_ms,
    sum(recording_per_reversing_ms) AS recording_per_reversing_ms,
    CASE
      WHEN sum(reversing_duration_ms) IS NULL THEN recording_ms / connected_ms
      ELSE sum(recording_per_reversing_ms) / sum(reversing_duration_ms)
    END AS uptime_pct
  FROM
    s2_connected_durations_overlapping_reversing
  GROUP BY
    org_id,
    vg_id,
    cm_id,
    start_ms,
    end_ms,
    trip_duration_ms,
    connected_ms,
    recording_ms
),
-- Calculate the segment bounds per VG in the most recent 10 hours of trip time.
s1_ignition_on_durations_per_vg as (
  SELECT
    org_id,
    vg_id,
    cm_id,
    min(trip_start_ms) AS start_ms,
    max(trip_start_ms + trip_duration_ms) AS end_ms,
    sum(trip_duration_ms) AS trip_duration_ms,
    sum(connected_ms) AS connected_ms,
    sum(recording_ms) AS recording_ms
  FROM
    connected_and_recording_last_50_hours_trip_time as trip_totals
  WHERE
    trip_totals.total_trip_ms <= 10 * 3600000
  GROUP BY
    org_id,
    vg_id,
    cm_id
),
-- Combine with all the reversing intervals that occured in the most recent 10 hours of trip time.
s1_connected_durations_overlapping_reversing AS (
  SELECT
    totals.org_id,
    totals.vg_id,
    totals.cm_id,
    totals.start_ms,
    totals.end_ms,
    totals.trip_duration_ms,
    totals.connected_ms,
    totals.recording_ms,
    reversing.reversing_duration_ms,
    reversing.reversing_start_ms,
    reversing.recording_per_reversing_ms
  FROM
    s1_ignition_on_durations_per_vg as totals
    LEFT JOIN camera_connector_health.cc_reversing_and_recording_durations AS reversing ON totals.vg_id = reversing.vg_id
    AND totals.org_id = reversing.org_id
    AND NOT (
      reversing.reversing_start_ms >= totals.end_ms
      OR (
        reversing.reversing_start_ms + reversing.reversing_duration_ms
      ) <= totals.start_ms
    )
),
-- Aggregate all the reversing intervals in the most recent 10 hours of trip time.
-- If there are reversing intervals, use only the recording per reversing as the uptime pct.
-- Otherwise, use the standard recording per connected time during a trip.
s1_combined_reversing_and_connected_durations AS (
  SELECT
    org_id,
    vg_id,
    cm_id,
    start_ms,
    end_ms,
    trip_duration_ms,
    connected_ms,
    recording_ms,
    sum(reversing_duration_ms) AS reversing_ms,
    sum(recording_per_reversing_ms) AS recording_per_reversing_ms,
    CASE
      WHEN sum(reversing_duration_ms) IS NULL THEN recording_ms / connected_ms
      ELSE sum(recording_per_reversing_ms) / sum(reversing_duration_ms)
    END AS uptime_pct
  FROM
    s1_connected_durations_overlapping_reversing
  GROUP BY
    org_id,
    vg_id,
    cm_id,
    start_ms,
    end_ms,
    trip_duration_ms,
    connected_ms,
    recording_ms
)
-- The final output combines the totals and segments per VG
SELECT
  totals.org_id,
  totals.vg_id,
  COALESCE(totals.cm_id, 0) as cm_id,
  totals.start_ms AS overall_start_ms,
  totals.end_ms AS overall_end_ms,
  -- With overwrite mode, we can only modify partitions where start_date <= date < end_date.
  -- Because end_date is exclusive, subtract 1 to make it inclusive.
  cast(date_sub(${end_date}, 1) as date) as date,
  COALESCE(totals.trip_duration_ms, 0) AS overall_trip_ms,
  CASE
    WHEN totals.reversing_ms IS NOT NULL THEN 2 -- reversing
    ELSE 1 -- ignition on
  END AS connector_attribute,
  COALESCE(totals.connected_ms, 0) AS overall_connected_ms,
  COALESCE(totals.recording_ms, 0) AS overall_recording_ms,
  COALESCE(totals.reversing_ms, 0) AS overall_reversing_ms,
  COALESCE(totals.recording_per_reversing_ms, 0) AS overall_recording_per_reversing_ms,
  COALESCE(totals.uptime_pct, 0) AS overall_recording_uptime_pct,
  COALESCE((totals.connected_ms / totals.trip_duration_ms), 0) AS overall_connected_uptime_pct,
  COALESCE(s1.start_ms, 0) AS s1_start_ms,
  COALESCE(s1.end_ms, 0) AS s1_end_ms,
  COALESCE(s1.trip_duration_ms, 0) AS s1_trip_ms,
  COALESCE(s1.connected_ms, 0) AS s1_connected_ms,
  COALESCE(s1.recording_ms, 0) AS s1_recording_ms,
  COALESCE(s1.reversing_ms, 0) AS s1_reversing_ms,
  COALESCE(s1.recording_per_reversing_ms, 0) AS s1_recording_per_reversing_ms,
  COALESCE(s1.uptime_pct, 0) AS s1_recording_uptime_pct,
  COALESCE(s2.start_ms, 0) AS s2_start_ms,
  COALESCE(s2.end_ms, 0) AS s2_end_ms,
  COALESCE(s2.trip_duration_ms, 0) AS s2_trip_ms,
  COALESCE(s2.connected_ms, 0) AS s2_connected_ms,
  COALESCE(s2.recording_ms, 0) AS s2_recording_ms,
  COALESCE(s2.reversing_ms, 0) AS s2_reversing_ms,
  COALESCE(s2.recording_per_reversing_ms, 0) AS s2_recording_per_reversing_ms,
  COALESCE(s2.uptime_pct, 0) AS s2_recording_uptime_pct,
  COALESCE(s3.start_ms, 0) AS s3_start_ms,
  COALESCE(s3.end_ms, 0) AS s3_end_ms,
  COALESCE(s3.trip_duration_ms, 0) AS s3_trip_ms,
  COALESCE(s3.connected_ms, 0) AS s3_connected_ms,
  COALESCE(s3.recording_ms, 0) AS s3_recording_ms,
  COALESCE(s3.reversing_ms, 0) AS s3_reversing_ms,
  COALESCE(s3.recording_per_reversing_ms, 0) AS s3_recording_per_reversing_ms,
  COALESCE(s3.uptime_pct, 0) AS s3_recording_uptime_pct,
  COALESCE(s4.start_ms, 0) AS s4_start_ms,
  COALESCE(s4.end_ms, 0) AS s4_end_ms,
  COALESCE(s4.trip_duration_ms, 0) AS s4_trip_ms,
  COALESCE(s4.connected_ms, 0) AS s4_connected_ms,
  COALESCE(s4.recording_ms, 0) AS s4_recording_ms,
  COALESCE(s4.reversing_ms, 0) AS s4_reversing_ms,
  COALESCE(s4.recording_per_reversing_ms, 0) AS s4_recording_per_reversing_ms,
  COALESCE(s4.uptime_pct, 0) AS s4_recording_uptime_pct,
  COALESCE(s5.start_ms, 0) AS s5_start_ms,
  COALESCE(s5.end_ms, 0) AS s5_end_ms,
  COALESCE(s5.trip_duration_ms, 0) AS s5_trip_ms,
  COALESCE(s5.connected_ms, 0) AS s5_connected_ms,
  COALESCE(s5.recording_ms, 0) AS s5_recording_ms,
  COALESCE(s5.reversing_ms, 0) AS s5_reversing_ms,
  COALESCE(s5.recording_per_reversing_ms, 0) AS s5_recording_per_reversing_ms,
  COALESCE(s5.uptime_pct, 0) AS s5_recording_uptime_pct
FROM
  total_combined_reversing_and_connected_durations AS totals
  LEFT JOIN s5_combined_reversing_and_connected_durations s5
  ON totals.vg_id = s5.vg_id
  AND totals.org_id = s5.org_id
  LEFT JOIN s4_combined_reversing_and_connected_durations s4
  ON totals.vg_id = s4.vg_id
  AND totals.org_id = s4.org_id
  LEFT JOIN s3_combined_reversing_and_connected_durations s3
  ON totals.vg_id = s3.vg_id
  AND totals.org_id = s3.org_id
  LEFT JOIN s2_combined_reversing_and_connected_durations s2
  ON totals.vg_id = s2.vg_id
  AND totals.org_id = s2.org_id
  LEFT JOIN s1_combined_reversing_and_connected_durations s1
  ON totals.vg_id = s1.vg_id
  AND totals.org_id = s1.org_id
