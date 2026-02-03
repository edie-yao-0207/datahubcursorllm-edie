-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Overview
-- MAGIC
-- MAGIC You MUST run this notebook on the developerexperience cluster.
-- MAGIC
-- MAGIC This notebook imports devmetrics from the S3 bucket, creates a view and then performs some analytics via spark-SQL.
-- MAGIC All duration values are in seconds, unless otherwise noted.

-- COMMAND ----------

CREATE OR REPLACE VIEW devexp.devmetrics_raw AS
SELECT * FROM read_files(
  '/Volumes/s3/devmetrics/root/date=*/*.json',
  format => 'json',
  mode   => 'DROPMALFORMED',
  schema => 'task STRING,
    os STRING,
    args STRING,
    repo STRING,
    env STRING,
    team STRING,
    user_hash STRING,
    experimental_features ARRAY<STRING>,
    start_time STRING,
    end_time STRING,
    exit_status STRING,
    ai_analysis_count INT,
    ai_provider STRING,
    ai_model STRING,
    ai_last_success BOOLEAN,
    ai_last_duration DOUBLE,
    ai_last_timestamp STRING'
)
UNION ALL
SELECT * FROM read_files(
  '/Volumes/s3/devmetrics/root/date=*/taskrunner-metrics/*.json',
  format => 'json',
  mode   => 'DROPMALFORMED',
  schema => 'task STRING,
    os STRING,
    args STRING,
    repo STRING,
    env STRING,
    team STRING,
    user_hash STRING,
    experimental_features ARRAY<STRING>,
    start_time STRING,
    end_time STRING,
    exit_status STRING,
    ai_analysis_count INT,
    ai_provider STRING,
    ai_model STRING,
    ai_last_success BOOLEAN,
    ai_last_duration DOUBLE,
    ai_last_timestamp STRING'
);

CREATE OR REPLACE VIEW devexp.devmetrics AS
SELECT
  *,
  CAST(start_time AS TIMESTAMP) AS start_ts,
  CAST(end_time   AS TIMESTAMP) AS end_ts,

  to_unix_timestamp(CAST(end_time AS TIMESTAMP))
    - to_unix_timestamp(CAST(start_time AS TIMESTAMP)) AS duration,

  DATE(date_trunc('MONTH', CAST(start_time AS TIMESTAMP))) AS month,
  DATE(date_trunc('WEEK',  CAST(start_time AS TIMESTAMP))) AS week,

  -- Cast AI timestamp only when present
CASE
    WHEN ai_last_timestamp IS NULL THEN NULL
    WHEN ai_last_timestamp = '0001-01-01T00:00:00Z' THEN NULL
    ELSE CAST(ai_last_timestamp AS TIMESTAMP)
  END AS ai_last_ts

FROM devexp.devmetrics_raw
WHERE CAST(end_time AS TIMESTAMP) > CAST(start_time AS TIMESTAMP);

-- COMMAND ----------
-- Keeps the last 30 days of data for AI metrics
CREATE OR REPLACE VIEW devexp.devmetrics_ai AS
SELECT *
FROM devexp.devmetrics
WHERE
  start_ts >= date_sub(current_timestamp(), 30)
  AND env <> 'buildkite';

-- COMMAND ----------
-- DBTITLE 1,Tasks ordered by duration
select task, avg(duration) as average_duration from devexp.devmetrics group by task order by average_duration desc;

-- COMMAND ----------
-- DBTITLE 1,Tasks ordered by duration ignoring buildkite
select task, avg(duration) as average_duration from devexp.devmetrics where team != 'buildkite' group by task order by average_duration desc;

-- COMMAND ----------
-- DBTITLE 1,Tasks ordered by frequency
select task, count(1) as count from devexp.devmetrics group by task order by count desc;

-- COMMAND ----------
-- DBTITLE 1,Tasks ordered by frequency ignoring buildkite
select task, count(1) as count from devexp.devmetrics where team != 'buildkite' group by task order by count desc;

-- COMMAND ----------
-- DBTITLE 1, db/import data, ordered by average duration
select task, exit_status, avg(duration) / 60.0 as avg_dur_mins, max(duration) / 60.0 as max_dur_mins, count(1) as count from devexp.devmetrics where task like '%db/import%' group by task, exit_status order by avg_dur_mins desc;

-- COMMAND ----------
-- DBTITLE 1, db/import data per month, ordered by task, exit_status, month
select task, exit_status, month, avg(duration) / 60.0 as avg_dur_mins, max(duration) / 60.0 as max_dur_mins, count(1) as count from devexp.devmetrics where task like '%db/import%' group by task, exit_status, month order by task asc, exit_status asc, month asc;

-- COMMAND ----------
-- DBTITLE 1, db/import data per week, ordered by task, exit_status, week
select task, exit_status, week, avg(duration) / 60.0 as avg_dur_mins, max(duration) / 60.0 as max_dur_mins, count(1) as count from devexp.devmetrics where task like '%db/import%' group by task, exit_status, week order by task asc, exit_status asc, week asc;

-- COMMAND ----------
-- Daily unique AI users over the last 30 calendar days (including today)
WITH date_range AS (
  SELECT explode(
    sequence(date_sub(current_date(), 29), current_date(), interval 1 day)
  ) AS date
),
daily_unique_ai_users AS (
  SELECT
    DATE(start_ts) AS date,
    COUNT(DISTINCT user_hash) AS unique_users
  FROM devexp.devmetrics_ai
  WHERE ai_analysis_count > 0
  GROUP BY DATE(start_ts)
)
SELECT
  d.date,
  COALESCE(u.unique_users, 0) AS daily_unique_ai_users
FROM date_range d
LEFT JOIN daily_unique_ai_users u
  ON d.date = u.date
ORDER BY d.date;

-- COMMAND ----------
-- Daily total AI analyses over the last 30 calendar days
WITH date_range AS (
  SELECT explode(
    sequence(date_sub(current_date(), 29), current_date(), interval 1 day)
  ) AS date
),

daily_ai_analyses AS (
  SELECT
    DATE(start_ts) AS date,
    SUM(ai_analysis_count) AS total_ai_analyses
  FROM devexp.devmetrics_ai
  WHERE ai_analysis_count > 0
  GROUP BY DATE(start_ts)
)

SELECT
  d.date,
  COALESCE(a.total_ai_analyses, 0) AS daily_total_ai_analyses
FROM date_range d
LEFT JOIN daily_ai_analyses a
  ON d.date = a.date
ORDER BY d.date;
