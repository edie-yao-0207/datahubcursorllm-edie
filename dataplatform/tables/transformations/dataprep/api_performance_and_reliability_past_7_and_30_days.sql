WITH performance_and_reliability_by_org_past_7_days AS (
  SELECT 
    http_method,
    path_template,
    owning_team,
    org_id,
    org_name,
    org_size_segment,
    industry,
    access_token_id,
    token_name,
    oauth_token_id,
    oauth_token_app_uuid, 
    app_name,
    SUM(num_api_requests_less_than_or_equal_to_5_seconds_daily) as num_requests_less_than_or_equal_to_5_seconds_daily_past_7_days,
    SUM(num_api_requests_daily) AS num_requests_daily_past_7_days,
    SUM(total_duration_ms) AS total_duration_ms_past_7_days,
    SUM(num_fails_daily) AS num_fails_daily_past_7_days,
    SUM(total_duration_ms)/SUM(num_api_requests_daily) AS performance_past_7_days,
    (SUM(num_api_requests_daily) - SUM(num_fails_daily))/SUM(num_api_requests_daily) AS reliability_past_7_days,
    (SUM(num_api_requests_less_than_or_equal_to_5_seconds_daily)/SUM(num_api_requests_daily)) * 100 as percentage_of_requests_daily_less_than_or_equal_to_5_seconds_past_7_days
  FROM dataprep.api_reliability
  WHERE date > date_add(CURRENT_TIMESTAMP(), -7)
  group by 
    http_method,
    path_template,
    owning_team, 
    org_id,
    org_name,
    org_size_segment,
    industry,
    access_token_id, 
    token_name,
    oauth_token_id,
    oauth_token_app_uuid, 
    app_name
),

performance_and_reliability_by_org_past_30_days as (
  SELECT 
    http_method,
    path_template,
    owning_team,
    org_id,
    org_name,
    org_size_segment,
    industry,
    access_token_id,
    token_name,
    oauth_token_id,
    oauth_token_app_uuid,
    app_name,
    SUM(num_api_requests_less_than_or_equal_to_5_seconds_daily) as num_requests_less_than_or_equal_to_5_seconds_daily_past_30_days,
    SUM(num_api_requests_daily) AS num_requests_daily_past_30_days,
    SUM(total_duration_ms) AS total_duration_ms_past_30_days,
    SUM(num_fails_daily) AS num_fails_daily_past_30_days,
    SUM(total_duration_ms)/SUM(num_api_requests_daily) AS performance_past_30_days,
    (SUM(num_api_requests_daily) - SUM(num_fails_daily))/SUM(num_api_requests_daily) AS reliability_past_30_days,
    (SUM(num_api_requests_less_than_or_equal_to_5_seconds_daily)/SUM(num_api_requests_daily)) * 100 as percentage_of_requests_daily_less_than_or_equal_to_5_seconds_past_30_days
  FROM dataprep.api_reliability
  WHERE date > date_add(CURRENT_TIMESTAMP(), -30)
  GROUP BY 
    http_method,
    path_template,
    owning_team, 
    org_id,
    org_name,
    org_size_segment,
    industry,
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
  t1.access_token_id,
  t1.token_name,
  t1.oauth_token_id,
  t1.oauth_token_app_uuid,
  t1.app_name,
  num_requests_less_than_or_equal_to_5_seconds_daily_past_7_days,
  num_requests_daily_past_7_days,
  total_duration_ms_past_7_days,
  num_fails_daily_past_7_days,
  num_requests_less_than_or_equal_to_5_seconds_daily_past_30_days,
  num_requests_daily_past_30_days,
  total_duration_ms_past_30_days,
  num_fails_daily_past_30_days,
  performance_past_7_days,
  reliability_past_7_days,
  percentage_of_requests_daily_less_than_or_equal_to_5_seconds_past_7_days,
  performance_past_30_days,
  reliability_past_30_days,
  percentage_of_requests_daily_less_than_or_equal_to_5_seconds_past_30_days,
  CONCAT(t1.http_method, " ", t2.path_template) AS complete_api_request
FROM performance_and_reliability_by_org_past_7_days t1
LEFT JOIN performance_and_reliability_by_org_past_30_days t2
  ON t1.http_method = t2.http_method
  AND t1.path_template = t2.path_template
  AND t1.owning_team = t2.owning_team
  AND t1.org_id = t2.org_id
  AND t1.org_name = t2.org_name
  AND t1.org_size_segment = t2.org_size_segment
  AND t1.industry = t2.industry
  AND t1.access_token_id = t2.access_token_id
  AND t1.token_name = t2.token_name
  AND t1.oauth_token_id = t2.oauth_token_id
  AND t1.oauth_token_app_uuid = t2.oauth_token_app_uuid
  AND t1.app_name = t2.app_name