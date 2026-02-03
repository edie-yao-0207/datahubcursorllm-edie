-- Purpose: SQL query to alert when the latency of events ingested by Konmari exceeds a given threshold.
--
-- How it works:
-- 1. The query first filters the data to only include events that have passed through both the ingester and persister checkpoints. This allows us to ignore events that have been filtered out and skew the latency results. We use MIN(ingester.processEvent) to get the maximum latency for the event in case of retries.
-- 2. The query then calculates the P50 latency of each event by subtracting the recorded_at_ms of the ingester checkpoint from the recorded_at_ms of the persister checkpoint. This is then divided by 1000 to convert the latency to seconds.
-- 3. Finally, the query compares the latency value against the thresholds.
WITH event_timestamps AS (
    SELECT
        event_id,
        human_readable_event_type,
        date,
        MIN(CASE WHEN checkpoint = 'ingester.processEvent' AND human_readable_status = 'ENTERED' THEN recorded_at_ms END) AS ingester_recorded_at_ms,
        MAX(CASE WHEN checkpoint = 'Persister.SafetyEventV1Persister' AND human_readable_status = 'PASSED' THEN recorded_at_ms END) AS persister_recorded_at_ms
    FROM
        datastreams.event_detection_checkpoint_status
    WHERE
        checkpoint IN ('ingester.processEvent', 'Persister.SafetyEventV1Persister')
        AND date >= DATEADD(hour, -24, CURRENT_TIMESTAMP)
        AND human_readable_event_type IN (
          'DASHCAM_DISTRACTED_DRIVING',
          'DASHCAM_DROWSINESS',
          'DASHCAM_FOLLOWING_DISTANCE',
          'DASHCAM_FORWARD_COLLISION_WARNING',
          'DASHCAM_INWARD_OBSTRUCTION',
          'DASHCAM_LANE_DEPARTURE_WARNING',
          'DASHCAM_MOBILE_USAGE',
          'DASHCAM_NO_SEATBELT',
          'DASHCAM_OUTWARD_OBSTRUCTION',
          'DASHCAM_ROLLING_STOP',
          'VG_BENDIX_ROLLOVER_BRAKE_ACTIVATED',
          'VG_BENDIX_ROLLOVER_ENGINE_ACTIVATED',
          'VG_BENDIX_YAW_BRAKE_ACTIVATED',
          'VG_BENDIX_YAW_ENGINE_ACTIVATED',
          'VG_CRASH_PRIMARY',
          'VG_CRASH_SECONDARY',
          'VG_HARSH_ACCELERATION',
          'VG_HARSH_BRAKING',
          'VG_SHARP_TURN'
          -- add another event type here
        )
    GROUP BY
        event_id,
        human_readable_event_type,
        date
    HAVING
        COUNT(DISTINCT checkpoint) = 2
), latency AS (
  SELECT
        date,
        human_readable_event_type,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (persister_recorded_at_ms - ingester_recorded_at_ms)/1000) AS p50_latency_seconds,
        CASE
           WHEN human_readable_event_type = 'DASHCAM_DISTRACTED_DRIVING' THEN 5 * 60
           WHEN human_readable_event_type = 'DASHCAM_DROWSINESS' THEN 5 * 60
           WHEN human_readable_event_type = 'DASHCAM_FOLLOWING_DISTANCE' THEN 5 * 60
           WHEN human_readable_event_type = 'DASHCAM_FORWARD_COLLISION_WARNING' THEN 7 * 60
           WHEN human_readable_event_type = 'DASHCAM_INWARD_OBSTRUCTION' THEN 2 * 60
           WHEN human_readable_event_type = 'DASHCAM_LANE_DEPARTURE_WARNING' THEN 8 * 60
           WHEN human_readable_event_type = 'DASHCAM_MOBILE_USAGE' THEN 5 * 60
           WHEN human_readable_event_type = 'DASHCAM_NO_SEATBELT' THEN 5 * 60
           WHEN human_readable_event_type = 'DASHCAM_OUTWARD_OBSTRUCTION' THEN 2 * 60
           WHEN human_readable_event_type = 'DASHCAM_ROLLING_STOP' THEN 7 * 60
           WHEN human_readable_event_type = 'VG_BENDIX_ROLLOVER_BRAKE_ACTIVATED' THEN 1
           WHEN human_readable_event_type = 'VG_BENDIX_ROLLOVER_ENGINE_ACTIVATED' THEN 1
           WHEN human_readable_event_type = 'VG_BENDIX_YAW_BRAKE_ACTIVATED' THEN 1
           WHEN human_readable_event_type = 'VG_BENDIX_YAW_ENGINE_ACTIVATED' THEN 1
           WHEN human_readable_event_type = 'VG_CRASH_PRIMARY' THEN 2
           WHEN human_readable_event_type = 'VG_CRASH_SECONDARY' THEN 2
           WHEN human_readable_event_type = 'VG_HARSH_ACCELERATION' THEN 1
           WHEN human_readable_event_type = 'VG_HARSH_BRAKING' THEN 1
           WHEN human_readable_event_type = 'VG_SHARP_TURN' THEN 1
          -- add another event type and its threshold here
           ELSE 0
        END AS latency_threshold_seconds
    FROM
        event_timestamps
    GROUP BY date, human_readable_event_type
)
SELECT
    date,
    human_readable_event_type,
    p50_latency_seconds,
    latency_threshold_seconds
FROM latency
WHERE p50_latency_seconds > latency_threshold_seconds
