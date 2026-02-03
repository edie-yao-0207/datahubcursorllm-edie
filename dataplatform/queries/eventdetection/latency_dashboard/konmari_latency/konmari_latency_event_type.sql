WITH event_latencies AS (
    SELECT
        event_id,
        human_readable_event_type,
        MIN(CASE WHEN checkpoint = 'ingester.processEvent' AND human_readable_status = 'ENTERED' THEN recorded_at_ms END) AS ingester_recorded_at_ms,
        MAX(CASE WHEN checkpoint = 'Persister.SafetyEventV1Persister' AND human_readable_status = 'PASSED' THEN recorded_at_ms END) AS persister_recorded_at_ms
    FROM
        datastreams.event_detection_checkpoint_status
    WHERE
        checkpoint IN ('ingester.processEvent', 'Persister.SafetyEventV1Persister')
        AND date >= :date_range.start  AND date <= :date_range.end
    GROUP BY
        event_id,
        human_readable_event_type
    HAVING
        COUNT(DISTINCT checkpoint) = 2
), latencies AS (
  SELECT
        human_readable_event_type,
        (persister_recorded_at_ms - ingester_recorded_at_ms)/1000 AS latency_seconds
    FROM
        event_latencies
)
SELECT
    human_readable_event_type,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY latency_seconds) AS p50_latency_seconds,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_seconds) AS p95_latency_seconds,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_seconds) AS p99_latency_seconds,
    count(*)
FROM
    latencies
GROUP BY human_readable_event_type
