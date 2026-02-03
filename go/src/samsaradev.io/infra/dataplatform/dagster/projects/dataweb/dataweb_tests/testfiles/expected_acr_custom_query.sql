events_us
AS (
SELECT
CAST(rr.org_id AS BIGINT) AS org_id,
DATE_TRUNC('day', rr.created_at) AS date,
rr.user_id AS user_id,
CONCAT(rr.org_id, '_', rr.uuid) AS usage_id
FROM reportconfigdb_shards.report_runs rr
JOIN clouddb.organizations o
ON rr.org_id = o.id
WHERE
rr.report_run_metadata.report_config IS NOT NULL
AND rr.report_run_metadata.report_config.is_superuser_only = FALSE -- Exclude superuser-only reports
AND rr.org_id IS NOT NULL
AND rr.created_at IS NOT NULL
),
events_us_filtered
AS (
SELECT e.*
FROM events_us e
LEFT OUTER
JOIN users_us u
ON e.user_id = u.user_id
WHERE
TO_DATE(date) BETWEEN DATE_SUB('2025-09-15', 55)
AND '2025-09-15'
AND u.user_id IS NULL -- filter out internal usage
),
events_eu
AS (
SELECT
CAST(rr.org_id AS BIGINT) AS org_id,
DATE_TRUNC('day', rr.created_at) AS date,
rr.user_id AS user_id,
CONCAT(rr.org_id, '_', rr.uuid) AS usage_id
FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-reportconfigdb/reportconfigdb/report_runs_v0` rr
JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-clouddb/prod_db/organizations_v3` o
ON rr.org_id = o.id
WHERE
rr.report_run_metadata.report_config IS NOT NULL
AND rr.report_run_metadata.report_config.is_superuser_only = FALSE -- Exclude superuser-only reports
AND rr.org_id IS NOT NULL
AND rr.created_at IS NOT NULL
-- Note: We are not filtering on rr.status (e.g., status=2 for 'Complete')
-- to count all attempts that meet the above criteria, similar to the engineer's query.
-- If only 'Complete' runs should be counted, add '
AND rr.status = 2' here.
UNION ALL
SELECT
CAST(rr.org_id AS BIGINT) AS org_id,
DATE_TRUNC('day', rr.created_at) AS date,
rr.user_id AS user_id,
CONCAT(rr.org_id, '_', rr.uuid) AS usage_id
FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-reportconfig-shard-1db/reportconfigdb/report_runs_v0` rr
JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-clouddb/prod_db/organizations_v3` o
ON rr.org_id = o.id
WHERE
rr.report_run_metadata.report_config IS NOT NULL
AND rr.report_run_metadata.report_config.is_superuser_only = FALSE -- Exclude superuser-only reports
AND rr.org_id IS NOT NULL
AND rr.created_at IS NOT NULL
-- Note: We are not filtering on rr.status (e.g., status=2 for 'Complete')
-- to count all attempts that meet the above criteria, similar to the engineer's query.
-- If only 'Complete' runs should be counted, add '
AND rr.status = 2' here.
),
events_eu_filtered
AS (
SELECT e.*
FROM events_eu e
LEFT OUTER
JOIN users_eu u
ON e.user_id = u.user_id
WHERE
TO_DATE(date) BETWEEN DATE_SUB('2025-09-15', 55)
AND '2025-09-15'
AND u.user_id IS NULL -- filter out internal usage
),
custom_query_events
AS (
SELECT *
FROM events_us_filtered
UNION ALL
SELECT *
FROM events_eu_filtered
),
custom_query_events_aggregated
AS (
SELECT
org_id,
COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('2025-09-15', 6)
AND '2025-09-15' THEN usage_id END) AS usage_weekly,
COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('2025-09-15', 27)
AND '2025-09-15' THEN usage_id END) AS usage_monthly,
COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('2025-09-15', 55)
AND DATE_SUB('2025-09-15', 28) THEN usage_id END) AS usage_prior_month,
COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('2025-09-15', 34)
AND DATE_SUB('2025-09-15', 28) THEN usage_id END) AS usage_weekly_prior_month,
COUNT(DISTINCT CASE WHEN date = '2025-09-15' THEN user_id END) AS daily_active_user,
COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('2025-09-15', 6)
AND '2025-09-15' THEN user_id END) AS weekly_active_users,
COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('2025-09-15', 27)
AND '2025-09-15' THEN user_id END) AS monthly_active_users,
MAX(CASE WHEN date = '2025-09-15' THEN 1 ELSE 0 END) AS org_active_day
FROM custom_query_events
GROUP BY org_id
),
all_user_events
AS (
SELECT org_id, date, user_id
FROM custom_query_events
),
user_metrics
AS (
SELECT
org_id,
COUNT(DISTINCT CASE WHEN date = '2025-09-15' THEN CAST(user_id AS STRING) END) AS daily_active_user,
COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('2025-09-15', 6)
AND '2025-09-15' THEN CAST(user_id AS STRING) END) AS weekly_active_users,
COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('2025-09-15', 27)
AND '2025-09-15' THEN CAST(user_id AS STRING) END) AS monthly_active_users
FROM all_user_events
WHERE CAST(user_id AS STRING) != '-1' -- adding to filter out custom query events that don't actually have a user id
GROUP BY org_id
),
final_usage_metrics
AS (
SELECT
m.org_id,
COALESCE(SUM(m.usage_weekly), 0) AS usage_weekly,
COALESCE(SUM(m.usage_monthly), 0) AS usage_monthly,
COALESCE(SUM(m.usage_prior_month), 0) AS usage_prior_month,
COALESCE(SUM(m.usage_weekly_prior_month), 0) AS usage_weekly_prior_month,
COALESCE(um.daily_active_user, 0) AS daily_active_user,
COALESCE(um.weekly_active_users, 0) AS weekly_active_users,
COALESCE(um.monthly_active_users, 0) AS monthly_active_users,
COALESCE(MAX(m.org_active_day), 0) AS org_active_day
FROM (
SELECT *
FROM custom_query_events_aggregated
) m
FULL OUTER
JOIN user_metrics um ON m.org_id = um.org_id
GROUP BY m.org_id, um.daily_active_user, um.weekly_active_users, um.monthly_active_users
)
