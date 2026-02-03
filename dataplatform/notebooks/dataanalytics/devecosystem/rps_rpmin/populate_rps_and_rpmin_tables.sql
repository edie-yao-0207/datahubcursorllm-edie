-- Databricks notebook source
INSERT INTO devecosystem_dev.rps
SELECT
  window["start"] as second,
  org_id,
  access_token_name,
  oauth_token_app_uuid,
  http_method,
  path_template,
  count(*) AS call_count
FROM datastreams.api_logs
WHERE date BETWEEN date_sub(date_trunc("DAY", current_timestamp()), 1) AND date(date_trunc("DAY", current_timestamp()))
AND timestamp BETWEEN timestamp(date_sub(date_trunc("DAY", current_timestamp()), 1)) AND date_trunc("DAY", current_timestamp())
AND status_code != 429
GROUP BY 
  window(timestamp, '1 second', '1 second'), second, org_id, 
  http_method, path_template, access_token_name, oauth_token_app_uuid

-- COMMAND ----------

INSERT INTO devecosystem_dev.rpmin
SELECT
  window["start"] as minute,
  org_id,
  access_token_name,
  oauth_token_app_uuid,
  http_method,
  path_template,
  count(*) AS call_count
FROM datastreams.api_logs
WHERE date BETWEEN date_sub(date_trunc("DAY", current_timestamp()), 1) AND date(date_trunc("DAY", current_timestamp()))
AND timestamp BETWEEN timestamp(date_sub(date_trunc("DAY", current_timestamp()), 1)) AND date_trunc("DAY", current_timestamp())
AND status_code != 429
GROUP BY 
  window(timestamp, '1 minute', '1 minute'), minute, org_id, 
  http_method, path_template, access_token_name, oauth_token_app_uuid
