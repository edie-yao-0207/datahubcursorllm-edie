-- tabulate logs with columns like [action, duration, status, err, uuid]
WITH batched_logs_table AS (
  WITH json_logs AS (
    SELECT
      from_json(
        json_params,
        'action STRING, duration INT, status INT, err STRING, driverMediaUpload struct<uuid:STRING, orgId:INT, driverId:INT, type:INT, state:INT, createdAtMs:LONG>'
      ) AS json_log,
      date
    FROM
      datastreams.mobile_logs
    WHERE
      event_type = "UPLOAD_MEDIA"
      AND date > date_add(current_date(), -11)
      AND date < current_date()
  )
  SELECT
    date,
    json_log.action,
    json_log.duration,
    json_log.status,
    json_log.err,
    json_log.driverMediaUpload.uuid
  FROM
    json_logs
  WHERE
    json_log.action == "upload"
),
batched_grouped_by_uuid AS (
  SELECT
    uuid,
    date,
    count(*) AS attempts
  FROM
    batched_logs_table t
  GROUP BY
    t.uuid,
    t.date
),
batched_grouped_by_uuid_successful AS (
  SELECT
    uuid,
    date,
    count(*) AS attempts
  FROM
    batched_logs_table t
  WHERE
    t.status == 200
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
    batched_grouped_by_uuid
  GROUP BY
    date
),
-- total number of distict media uploads completed
total_successful_uploads AS (
  SELECT
    date,
    count(*) AS total_successful_uploads
  FROM
    batched_grouped_by_uuid_successful
  GROUP BY
    date
),
-- average number of retries before success for a given media
average_retries AS (
  SELECT
    date,
    avg(attempts) AS average_retries
  FROM
    batched_grouped_by_uuid
  GROUP BY
    date
),
duration_p95_ms AS (
  SELECT
    date,
    percentile_approx(duration, 0.95) AS duration_p95_ms
  FROM
    batched_logs_table
  WHERE
    status == 200
    AND duration IS NOT NULL
  GROUP BY
    date
),
duration_p99_ms AS (
  SELECT
    date,
    percentile_approx(duration, 0.99) AS duration_p99_ms
  FROM
    batched_logs_table
  WHERE
    status == 200
    AND duration IS NOT NULL
  GROUP BY
    date
),
-- the mean of the bottom 95 percent durations (ie. thowing out top 5% outliers)
duration_trimmed_mean_95 AS (
  SELECT
    blt.date AS date,
    avg(blt.duration) AS duration_trimmed_mean_95
  FROM
    batched_logs_table AS blt
    INNER JOIN duration_p95_ms AS p95 ON p95.date = blt.date
  WHERE
    blt.status == 200
    AND blt.duration < p95.duration_p95_ms
  GROUP BY
    blt.date
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
-- tabulate metrics by date like [date, total_uploads, average_retries, duration_p95_ms, duration_p99_ms, duration_trimmed_mean_95, success_rate_overall]
batched_upload_metrics AS (
  SELECT
    u.date,
    tu.total_uploads,
    ar.average_retries,
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
    LEFT OUTER JOIN duration_p95_ms AS p95 ON p95.date = u.date
    LEFT OUTER JOIN duration_p99_ms AS p99 ON p99.date = u.date
    LEFT OUTER JOIN duration_trimmed_mean_95 AS tm ON tm.date = u.date
    LEFT OUTER JOIN success_rate_overall AS so ON so.date = u.date
)
SELECT
  *
FROM
  batched_upload_metrics
ORDER BY
  date ASC
