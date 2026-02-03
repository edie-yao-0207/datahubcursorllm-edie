WITH service_account_to_app_uuid AS (
  SELECT
    service_account_user_id,
    MAX(app_uuid) AS app_uuid
  FROM oauth2db_shards.oauth2_access_tokens
  GROUP BY service_account_user_id
),


datastreams_api_logs_1 AS (
  SELECT
    t1.*,
    t1.access_token_id,
    t1.oauth_token_app_uuid,
    CASE
      WHEN (access_token_id IS NULL OR access_token_id = "") THEN COALESCE(oauth_token_app_uuid, t2.app_uuid)
      ELSE oauth_token_app_uuid
    END AS oauth_token_app_uuid_new
  from datastreams.api_logs t1
  LEFT JOIN service_account_to_app_uuid t2
    ON t1.service_account_user_id =t2.service_account_user_id
),

datastreams_api_logs_2 AS (
  SELECT
    t1.*,
    CASE
      WHEN (access_token_id IS NULL OR access_token_id = "") THEN t2.name
      ELSE ""
    END AS app_name
  FROM datastreams_api_logs_1 t1
  LEFT JOIN clouddb.developer_apps t2
    ON REPLACE(LOWER(t1.oauth_token_app_uuid_new), "-", "") = REPLACE(LOWER(t2.uuid), "-", "")
),

api_owners as (
  SELECT
    method,
    route,
    MAX(team) AS team
  FROM definitions.api_owners
  GROUP BY
    method,
    route
)

SELECT
  http_method,
  path_template,
  COALESCE(t2.team , "") AS owning_team,
  t1.org_id,
  COALESCE(MAX(t3.name), "") AS org_name,
  CASE
    WHEN MAX(t3.size_segment) = "SMB" THEN "AE1"
    WHEN MAX(t3.size_segment) = "MM" THEN "AE2/AE3"
    ELSE COALESCE(MAX(t3.size_segment), "")
  END AS org_size_segment,
  COALESCE(MAX(t3.industry), "") AS industry,
  date,
  status_code,
  access_token_id,
  COALESCE(t4.name, "") AS token_name,
  COALESCE(oauth_token_id, "") AS oauth_token_id,
  COALESCE(oauth_token_app_uuid_new, "") AS oauth_token_app_uuid,
  app_name,
  COUNT(CASE WHEN duration_ms <= 5000 THEN 1 ELSE NULL END) AS num_api_requests_less_than_or_equal_to_5_seconds_daily,
  COUNT(1) AS num_api_requests_daily,
  SUM(duration_ms) AS total_duration_ms,
  SUM(duration_ms)/COUNT(1) AS performance,
  (COUNT(CASE WHEN duration_ms <= 5000 THEN 1 ELSE NULL END) / COUNT(1)) * 100 AS percentage_of_requests_less_than_or_equal_to_5_seconds_daily,
  CONCAT(http_method, " ", path_template) AS complete_api_request
FROM datastreams_api_logs_2 t1
LEFT JOIN api_owners t2
  ON t1.http_method = t2.method
  AND t1.path_template = t2.route
LEFT JOIN dataprep.customer_metadata t3
  ON t1.org_id = t3.org_id
LEFT JOIN clouddb.api_tokens t4
  ON t1.access_token_id = t4.id
LEFT JOIN clouddb.organizations t5
  ON t1.org_id = t5.id
WHERE
  t1.date >= ${start_date}
  AND t1.date < ${end_date}
GROUP BY
  http_method,
  path_template,
  COALESCE(t2.team, ""),
  t1.org_id,
  date,
  status_code,
  access_token_id,
  COALESCE(t4.name, ""),
  COALESCE(oauth_token_id, ""),
  COALESCE(oauth_token_app_uuid_new, ""),
  app_name
