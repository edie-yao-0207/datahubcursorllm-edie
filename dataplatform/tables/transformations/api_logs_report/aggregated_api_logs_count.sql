SELECT
    date,
    org_id,
    DATE_TRUNC('HOUR', timestamp) AS interval_start,
    path_template,
    status_code,
    http_method,
    access_token_id,
    oauth_token_id,
    access_token_name,
    oauth_token_app_uuid,
    COUNT(*) as count
FROM datastreams.api_logs WHERE date >= ${start_date} and date < ${end_date}
GROUP BY date, org_id, status_code, http_method, path_template, access_token_id, oauth_token_id, access_token_name, interval_start, oauth_token_app_uuid
