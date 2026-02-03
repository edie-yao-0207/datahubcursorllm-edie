WITH top_level_exclude_status_code AS (
  SELECT 
    http_method,
    path_template,
    owning_team, 
    org_id,
    org_name,
    org_size_segment,
    industry,
    date,
    access_token_id, 
    token_name,
    oauth_token_id,
    oauth_token_app_uuid, 
    app_name,
    SUM(num_api_requests_less_than_or_equal_to_5_seconds_daily) AS num_api_requests_less_than_or_equal_to_5_seconds_daily,
    SUM(num_api_requests_daily) AS num_api_requests_daily,
    SUM(total_duration_ms) AS total_duration_ms,
    SUM(total_duration_ms)/SUM(num_api_requests_daily) AS performance,
    (SUM(num_api_requests_less_than_or_equal_to_5_seconds_daily)/SUM(num_api_requests_daily)) * 100 AS percentage_of_requests_less_than_or_equal_to_5_seconds_daily
  FROM dataprep.api_performance
  GROUP BY
    http_method,
    path_template,
    owning_team, 
    org_id,
    org_name,
    org_size_segment,
    industry,
    date,
    access_token_id, 
    token_name,
    oauth_token_id,
    oauth_token_app_uuid, 
    app_name
),

num_fails_by_org_day AS (
  SELECT 
    http_method,
    path_template,
    owning_team, 
    org_id,
    org_name,
    org_size_segment,
    industry,
    date,
    access_token_id, 
    token_name,
    oauth_token_id,
    oauth_token_app_uuid, 
    app_name,
    count(*) AS num_fails_daily
  FROM dataprep.api_performance
  WHERE status_code LIKE "5__"
  GROUP BY
    http_method,
    path_template,
    owning_team, 
    org_id,
    org_name,
    org_size_segment,
    industry,
    date,
    access_token_id, 
    token_name,
    oauth_token_id,
    oauth_token_app_uuid, 
    app_name
)

SELECT 
  t1.http_method,
  t1.path_template,
  t1.owning_team, 
  t1.org_id,
  t1.org_name,
  t1.org_size_segment,
  t1.industry,
  t1.date,
  t1.access_token_id, 
  t1.token_name,
  t1.oauth_token_id,
  t1.oauth_token_app_uuid, 
  t1.app_name,
  t1.num_api_requests_less_than_or_equal_to_5_seconds_daily,
  t1.num_api_requests_daily,
  t1.total_duration_ms,
  t2.num_fails_daily,
  t1.performance,
  (t1.num_api_requests_daily - t2.num_fails_daily)/t1.num_api_requests_daily AS reliability_daily,
  t1.percentage_of_requests_less_than_or_equal_to_5_seconds_daily,
  CONCAT(t1.http_method, " ", t2.path_template) AS complete_api_request
FROM top_level_exclude_status_code t1
LEFT JOIN num_fails_by_org_day t2
  ON t1.http_method = t2.http_method
  AND t1.path_template = t2.path_template
  AND t1.owning_team = t2.owning_team
  AND t1.org_id = t2.org_id
  AND t1.org_name = t2.org_name
  AND t1.org_size_segment = t2.org_size_segment
  AND t1.industry = t2.industry
  AND t1.date = t2.date
  AND t1.access_token_id = t2.access_token_id
  AND t1.token_name = t2.token_name
  AND t1.oauth_token_id = t2.oauth_token_id
  AND t1.oauth_token_app_uuid = t2.oauth_token_app_uuid
  AND t1.app_name = t2.app_name
WHERE 
  t1.date >= ${start_date} 
  AND t1.date < ${end_date}
