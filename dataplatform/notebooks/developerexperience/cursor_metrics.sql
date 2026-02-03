-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Cursor Rule Usage Metrics
-- MAGIC
-- MAGIC You MUST run this notebook on the developerexperience cluster.
-- MAGIC
-- MAGIC This notebook tracks adoption of custom Cursor rules and "Fix with Cursor" buttons.

-- COMMAND ----------

CREATE OR REPLACE VIEW devexp.cursor_metrics_raw AS
SELECT
  timestamp,
  rules,
  user_hash,
  env,
  os,
  repo,
  CAST(timestamp AS TIMESTAMP) AS ts,
  CAST(timestamp AS DATE) AS date,
  DATE(DATE_TRUNC('WEEK', CAST(timestamp AS DATE))) AS week,
  DATE(DATE_TRUNC('MONTH', CAST(timestamp AS DATE))) AS month
FROM read_files(
  '/Volumes/s3/devmetrics/root/date=*/cursor-metrics/*.json',
  format => 'json',
  mode   => 'PERMISSIVE',
  schema => 'timestamp string, rules array<string>, user_hash string, env string, os string, repo string'
)
WHERE timestamp IS NOT NULL;

-- COMMAND ----------

-- Explode rules array to get one row per rule usage
CREATE OR REPLACE VIEW devexp.cursor_metrics AS
SELECT
  timestamp,
  explode(rules) AS rule,
  user_hash,
  env,
  os,
  repo,
  ts,
  date,
  week,
  month
FROM devexp.cursor_metrics_raw;

-- COMMAND ----------
-- DBTITLE 1,Overall Rule Usage

SELECT
  rule,
  COUNT(*) AS total_uses,
  COUNT(DISTINCT user_hash) AS unique_users,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS usage_rate_pct
FROM devexp.cursor_metrics
GROUP BY rule
ORDER BY total_uses DESC;

-- COMMAND ----------
-- DBTITLE 1,Daily Usage Trend

SELECT
  date,
  COUNT(*) AS total_uses,
  COUNT(DISTINCT rule) AS unique_rules_used,
  COUNT(DISTINCT user_hash) AS active_users,
  ROUND(COUNT(*) / COUNT(DISTINCT user_hash), 1) AS avg_uses_per_user
FROM devexp.cursor_metrics
GROUP BY date
ORDER BY date DESC
LIMIT 30;

-- COMMAND ----------
-- DBTITLE 1,Daily Rule Usage

SELECT
  date,
  rule,
  COUNT(*) AS daily_uses,
  COUNT(DISTINCT user_hash) AS unique_users,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY date), 1) AS daily_usage_rate_pct
FROM devexp.cursor_metrics
GROUP BY date, rule
ORDER BY date DESC, daily_uses DESC
LIMIT 100;

-- COMMAND ----------
-- DBTITLE 1,Monthly Trend

SELECT
  month,
  COUNT(*) AS total_uses,
  COUNT(DISTINCT rule) AS unique_rules_used,
  COUNT(DISTINCT user_hash) AS active_users,
  ROUND(COUNT(*) / COUNT(DISTINCT user_hash), 1) AS avg_uses_per_user,
  ROUND(COUNT(*) / COUNT(DISTINCT date), 1) AS avg_uses_per_day
FROM devexp.cursor_metrics
GROUP BY month
ORDER BY month DESC;

-- COMMAND ----------
-- DBTITLE 1,Monthly Rule Usage

SELECT
  month,
  rule,
  COUNT(*) AS monthly_uses,
  COUNT(DISTINCT user_hash) AS unique_users,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY month), 1) AS monthly_usage_rate_pct
FROM devexp.cursor_metrics
GROUP BY month, rule
ORDER BY month DESC, monthly_uses DESC
LIMIT 100;

-- COMMAND ----------
