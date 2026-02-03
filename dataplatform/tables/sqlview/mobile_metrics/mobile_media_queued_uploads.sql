-- tabulate logs with columns like [uuid, bgUpload, contentLength, attempts, durationMs, success]
WITH queued_logs_table AS (
  WITH queued_sucess_logs_table AS (
    WITH json_logs AS (
      SELECT
        from_json(
          json_params,
          'uuid STRING, bgUpload BOOLEAN, contentLength INT, createdAtMS LONG, type INT, uploadAttempts array<struct<completed:BOOLEAN, durationMs:INT, error:STRING, finalState:INT>>'
        ) AS json_log,
        date
      FROM
        datastreams.mobile_logs
      WHERE
        event_type = "DRIVER_MEDIA_UPLOAD_COMPLETE"
        AND date > date_add(current_date(), -11)
        AND date < current_date()
    )
    SELECT
      date,
      json_log.uuid,
      json_log.contentLength,
      size(json_log.uploadAttempts) AS attempts,
      -- get duration from last attempt (which is the one that sent this log)
      json_log.uploadAttempts [size(json_log.uploadAttempts)-1].durationMs AS durationMs,
      true AS success
    FROM
      json_logs
    WHERE
      json_log.bgUpload == false
  ),
  queued_failure_logs_table AS (
    WITH json_logs AS (
      SELECT
        from_json(
          json_params,
          'uuid STRING, bgUpload BOOLEAN, contentLength INT, createdAtMS LONG, type INT, uploadAttempts array<struct<completed:BOOLEAN, durationMs:INT, error:STRING, finalState:INT>>'
        ) AS json_log,
        date
      FROM
        datastreams.mobile_logs
      WHERE
        event_type = "DRIVER_MEDIA_UPLOAD_FAILED"
        AND date > date_add(current_date(), -11)
        AND date < current_date()
    )
    SELECT
      date,
      json_log.uuid,
      json_log.contentLength,
      size(json_log.uploadAttempts) AS attempts,
      -- get duration from last attempt (which is the one that sent this log)
      json_log.uploadAttempts [size(json_log.uploadAttempts)-1].durationMs AS durationMs,
      false AS success
    FROM
      json_logs
    WHERE
      json_log.bgUpload == false
  )
  SELECT
    *
  FROM
    (
      SELECT
        date,
        uuid,
        contentLength,
        attempts,
        durationMs,
        success
      FROM
        queued_sucess_logs_table
      UNION ALL
      SELECT
        date,
        uuid,
        contentLength,
        attempts,
        durationMs,
        success
      FROM
        queued_failure_logs_table
    )
),
queued_nobg_grouped_by_uuid AS (
  SELECT
    uuid,
    date,
    count(*) AS attempts
  FROM
    queued_logs_table t
  GROUP BY
    t.uuid,
    t.date
),
queued_nobg_grouped_by_uuid_successful AS (
  SELECT
    uuid,
    date,
    count(*) AS attempts
  FROM
    queued_logs_table t
  WHERE
    t.success == true
  GROUP BY
    t.uuid,
    t.date
),
-- total number of distict media uploads started
total_uploads AS (
  SELECT
    date,
    count(*) AS total_uploads
  FROM
    queued_nobg_grouped_by_uuid
  GROUP BY
    date
),
-- total number of distict media uploads completed
total_successful_uploads AS (
  SELECT
    date,
    count(*) AS total_successful_uploads
  FROM
    queued_nobg_grouped_by_uuid_successful
  GROUP BY
    date
),
-- average number of retries before success for a given media
average_retries AS (
  SELECT
    date,
    avg(attempts) AS average_retries
  FROM
    queued_nobg_grouped_by_uuid
  GROUP BY
    date
),
duration_p50_ms AS (
  SELECT
    date,
    percentile_approx(durationMs, 0.50) AS duration_p50_ms
  FROM
    queued_logs_table
  WHERE
    success == true
    AND durationMs IS NOT NULL
  GROUP BY
    date
),
duration_p75_ms AS (
  SELECT
    date,
    percentile_approx(durationMs, 0.75) AS duration_p75_ms
  FROM
    queued_logs_table
  WHERE
    success == true
    AND durationMs IS NOT NULL
  GROUP BY
    date
),
duration_p95_ms AS (
  SELECT
    date,
    percentile_approx(durationMs, 0.95) AS duration_p95_ms
  FROM
    queued_logs_table
  WHERE
    success == true
    AND durationMs IS NOT NULL
  GROUP BY
    date
),
duration_p99_ms AS (
  SELECT
    date,
    percentile_approx(durationMs, 0.99) AS duration_p99_ms
  FROM
    queued_logs_table
  WHERE
    success == true
    AND durationMs IS NOT NULL
  GROUP BY
    date
),
-- the mean of the bottom 95 percent durations (ie. thowing out top 5% outliers)
duration_trimmed_mean_95 AS (
  SELECT
    qlt.date AS date,
    avg(qlt.durationMs) AS duration_trimmed_mean_95
  FROM
    queued_logs_table AS qlt
    INNER JOIN duration_p95_ms AS p95 ON p95.date = qlt.date
  WHERE
    success == true
    AND qlt.durationMs < p95.duration_p95_ms
  GROUP BY
    qlt.date
),
-- percent of uploads that eventually complete within the day
success_rate_overall AS (
  SELECT
    tu.date AS date,
    ts.total_successful_uploads / tu.total_uploads AS success_rate_overall
  FROM
    total_uploads AS tu
    INNER JOIN total_successful_uploads AS ts ON ts.date = tu.date
),
-- tabulate metrics by date like [date, total_uploads, average_retries, duration_p50_ms, duration_p75_ms, duration_p95_ms, duration_p99_ms, duration_trimmed_mean_95, success_rate_overall]
queued_upload_nobg_stats AS (
  SELECT
    u.date,
    tu.total_uploads,
    ar.average_retries,
    p50.duration_p50_ms,
    p75.duration_p75_ms,
    p95.duration_p95_ms,
    p99.duration_p99_ms,
    tm.duration_trimmed_mean_95,
    so.success_rate_overall
  FROM
    (
      SELECT
        date
      FROM
        total_uploads
      UNION
      SELECT
        date
      FROM
        average_retries
      UNION
      SELECT
        date
      FROM
        duration_p50_ms
      UNION
      SELECT
        date
      FROM
        duration_p75_ms
      UNION
      SELECT
        date
      FROM
        duration_p95_ms
      UNION
      SELECT
        date
      FROM
        duration_p99_ms
      UNION
      SELECT
        date
      FROM
        duration_trimmed_mean_95
      UNION
      SELECT
        date
      FROM
        success_rate_overall
    ) AS u
    LEFT OUTER JOIN total_uploads AS tu ON tu.date = u.date
    LEFT OUTER JOIN average_retries AS ar ON ar.date = u.date
    LEFT OUTER JOIN duration_p50_ms AS p50 ON p50.date = u.date
    LEFT OUTER JOIN duration_p75_ms AS p75 ON p75.date = u.date
    LEFT OUTER JOIN duration_p95_ms AS p95 ON p95.date = u.date
    LEFT OUTER JOIN duration_p99_ms AS p99 ON p99.date = u.date
    LEFT OUTER JOIN duration_trimmed_mean_95 AS tm ON tm.date = u.date
    LEFT OUTER JOIN success_rate_overall AS so ON so.date = u.date
)
SELECT
  *
FROM
  queued_upload_nobg_stats
ORDER BY
  date ASC
