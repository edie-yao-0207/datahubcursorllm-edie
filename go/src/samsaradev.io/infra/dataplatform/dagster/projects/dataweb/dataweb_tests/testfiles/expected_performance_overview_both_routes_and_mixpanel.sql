cloud_route_events
AS (
SELECT
CAST(cr.orgid AS BIGINT) AS org_id,
TO_DATE(cr.mp_date) AS date,
COALESCE(e_user.user_id, cr.mp_user_id, cr.mp_device_id) AS user_id
FROM mixpanel_samsara.cloud_route AS cr
LEFT OUTER
JOIN enabled_users e_user
ON COALESCE(cr.mp_user_id, cr.mp_device_id) = e_user.email
AND CAST(cr.orgid AS BIGINT) = e_user.org_id
WHERE
cr.routename IN (
'fleet_supervisor_insights'
) -- Use the list of routenames
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.com'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.canary%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.forms.canary%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsaracanarydevcontractor%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsaratest%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%@samsara%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%@samsara-service-account.com'
AND cr.orgid IS NOT NULL
AND TO_DATE(cr.mp_date) BETWEEN DATE_SUB('2025-09-15', 55)
AND '2025-09-15'
),
mixpanel_events
AS (
SELECT
CAST(cr.orgid AS BIGINT) AS org_id,
TO_DATE(cr.mp_date) AS date,
COALESCE(e_user.user_id, cr.mp_user_id, cr.mp_device_id) AS user_id
FROM mixpanel_samsara.supervisor_view_safety_score_navigation_click AS cr
LEFT OUTER
JOIN enabled_users e_user
ON COALESCE(cr.mp_user_id, cr.mp_device_id) = e_user.email
AND CAST(cr.orgid AS BIGINT) = e_user.org_id
WHERE
COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.com'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.canary%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.forms.canary%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsaracanarydevcontractor%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsaratest%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%@samsara%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%@samsara-service-account.com'
AND cr.orgid IS NOT NULL
AND cr.routename IN ('fleet_supervisor_insights')
AND TO_DATE(cr.mp_date) BETWEEN DATE_SUB('2025-09-15', 55)
AND '2025-09-15'
UNION ALL
SELECT
CAST(cr.orgid AS BIGINT) AS org_id,
TO_DATE(cr.mp_date) AS date,
COALESCE(e_user.user_id, cr.mp_user_id, cr.mp_device_id) AS user_id
FROM mixpanel_samsara.supervisor_view_ecodriving_score_navigation_click AS cr
LEFT OUTER
JOIN enabled_users e_user
ON COALESCE(cr.mp_user_id, cr.mp_device_id) = e_user.email
AND CAST(cr.orgid AS BIGINT) = e_user.org_id
WHERE
COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.com'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.canary%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.forms.canary%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsaracanarydevcontractor%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsaratest%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%@samsara%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%@samsara-service-account.com'
AND cr.orgid IS NOT NULL
AND cr.routename IN ('fleet_supervisor_insights')
AND TO_DATE(cr.mp_date) BETWEEN DATE_SUB('2025-09-15', 55)
AND '2025-09-15'
UNION ALL
SELECT
CAST(cr.orgid AS BIGINT) AS org_id,
TO_DATE(cr.mp_date) AS date,
COALESCE(e_user.user_id, cr.mp_user_id, cr.mp_device_id) AS user_id
FROM mixpanel_samsara.supervisor_insights_drivers_to_recognize_click_driver_name AS cr
LEFT OUTER
JOIN enabled_users e_user
ON COALESCE(cr.mp_user_id, cr.mp_device_id) = e_user.email
AND CAST(cr.orgid AS BIGINT) = e_user.org_id
WHERE
COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.com'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.canary%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.forms.canary%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsaracanarydevcontractor%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsaratest%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%@samsara%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%@samsara-service-account.com'
AND cr.orgid IS NOT NULL
AND cr.routename IN ('fleet_supervisor_insights')
AND TO_DATE(cr.mp_date) BETWEEN DATE_SUB('2025-09-15', 55)
AND '2025-09-15'
UNION ALL
SELECT
CAST(cr.orgid AS BIGINT) AS org_id,
TO_DATE(cr.mp_date) AS date,
COALESCE(e_user.user_id, cr.mp_user_id, cr.mp_device_id) AS user_id
FROM mixpanel_samsara.supervisor_insights_drivers_requiring_attention_click_send_to_coaching AS cr
LEFT OUTER
JOIN enabled_users e_user
ON COALESCE(cr.mp_user_id, cr.mp_device_id) = e_user.email
AND CAST(cr.orgid AS BIGINT) = e_user.org_id
WHERE
COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.com'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.canary%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.forms.canary%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsaracanarydevcontractor%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsaratest%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%@samsara%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%@samsara-service-account.com'
AND cr.orgid IS NOT NULL
AND cr.routename IN ('fleet_supervisor_insights')
AND TO_DATE(cr.mp_date) BETWEEN DATE_SUB('2025-09-15', 55)
AND '2025-09-15'
UNION ALL
SELECT
CAST(cr.orgid AS BIGINT) AS org_id,
TO_DATE(cr.mp_date) AS date,
COALESCE(e_user.user_id, cr.mp_user_id, cr.mp_device_id) AS user_id
FROM mixpanel_samsara.supervisor_insights_drivers_requiring_attention_click_send_message AS cr
LEFT OUTER
JOIN enabled_users e_user
ON COALESCE(cr.mp_user_id, cr.mp_device_id) = e_user.email
AND CAST(cr.orgid AS BIGINT) = e_user.org_id
WHERE
COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.com'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.canary%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.forms.canary%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsaracanarydevcontractor%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsaratest%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%@samsara%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%@samsara-service-account.com'
AND cr.orgid IS NOT NULL
AND cr.routename IN ('fleet_supervisor_insights')
AND TO_DATE(cr.mp_date) BETWEEN DATE_SUB('2025-09-15', 55)
AND '2025-09-15'
UNION ALL
SELECT
CAST(cr.orgid AS BIGINT) AS org_id,
TO_DATE(cr.mp_date) AS date,
COALESCE(e_user.user_id, cr.mp_user_id, cr.mp_device_id) AS user_id
FROM mixpanel_samsara.supervisor_insights_drivers_requiring_attention_click_driver_name AS cr
LEFT OUTER
JOIN enabled_users e_user
ON COALESCE(cr.mp_user_id, cr.mp_device_id) = e_user.email
AND CAST(cr.orgid AS BIGINT) = e_user.org_id
WHERE
COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.com'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.canary%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.forms.canary%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsaracanarydevcontractor%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsaratest%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%@samsara%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%@samsara-service-account.com'
AND cr.orgid IS NOT NULL
AND cr.routename IN ('fleet_supervisor_insights')
AND TO_DATE(cr.mp_date) BETWEEN DATE_SUB('2025-09-15', 55)
AND '2025-09-15'
UNION ALL
SELECT
CAST(cr.orgid AS BIGINT) AS org_id,
TO_DATE(cr.mp_date) AS date,
COALESCE(e_user.user_id, cr.mp_user_id, cr.mp_device_id) AS user_id
FROM mixpanel_samsara.supervisor_insights_coaching_timeliness_click_send_notification AS cr
LEFT OUTER
JOIN enabled_users e_user
ON COALESCE(cr.mp_user_id, cr.mp_device_id) = e_user.email
AND CAST(cr.orgid AS BIGINT) = e_user.org_id
WHERE
COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.com'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.canary%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.forms.canary%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsaracanarydevcontractor%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsaratest%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%@samsara%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%@samsara-service-account.com'
AND cr.orgid IS NOT NULL
AND cr.routename IN ('fleet_supervisor_insights')
AND TO_DATE(cr.mp_date) BETWEEN DATE_SUB('2025-09-15', 55)
AND '2025-09-15'
UNION ALL
SELECT
CAST(cr.orgid AS BIGINT) AS org_id,
TO_DATE(cr.mp_date) AS date,
COALESCE(e_user.user_id, cr.mp_user_id, cr.mp_device_id) AS user_id
FROM mixpanel_samsara.supervisor_insights_coaching_effectiveness_click_view_drivers AS cr
LEFT OUTER
JOIN enabled_users e_user
ON COALESCE(cr.mp_user_id, cr.mp_device_id) = e_user.email
AND CAST(cr.orgid AS BIGINT) = e_user.org_id
WHERE
COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.com'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.canary%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.forms.canary%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsaracanarydevcontractor%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsaratest%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%@samsara%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%@samsara-service-account.com'
AND cr.orgid IS NOT NULL
AND cr.routename IN ('fleet_supervisor_insights')
AND TO_DATE(cr.mp_date) BETWEEN DATE_SUB('2025-09-15', 55)
AND '2025-09-15'
UNION ALL
SELECT
CAST(cr.orgid AS BIGINT) AS org_id,
TO_DATE(cr.mp_date) AS date,
COALESCE(e_user.user_id, cr.mp_user_id, cr.mp_device_id) AS user_id
FROM mixpanel_samsara.supervisor_insights_training_timeliness_click_send_message AS cr
LEFT OUTER
JOIN enabled_users e_user
ON COALESCE(cr.mp_user_id, cr.mp_device_id) = e_user.email
AND CAST(cr.orgid AS BIGINT) = e_user.org_id
WHERE
COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.com'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.canary%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.forms.canary%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsaracanarydevcontractor%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsaratest%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%@samsara%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%@samsara-service-account.com'
AND cr.orgid IS NOT NULL
AND cr.routename IN ('fleet_supervisor_insights')
AND TO_DATE(cr.mp_date) BETWEEN DATE_SUB('2025-09-15', 55)
AND '2025-09-15'
UNION ALL
SELECT
CAST(cr.orgid AS BIGINT) AS org_id,
TO_DATE(cr.mp_date) AS date,
COALESCE(e_user.user_id, cr.mp_user_id, cr.mp_device_id) AS user_id
FROM mixpanel_samsara.positive_recognition_kudos_send_kudos AS cr
LEFT OUTER
JOIN enabled_users e_user
ON COALESCE(cr.mp_user_id, cr.mp_device_id) = e_user.email
AND CAST(cr.orgid AS BIGINT) = e_user.org_id
WHERE
COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.com'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.canary%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.forms.canary%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsaracanarydevcontractor%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsaratest%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%@samsara%'
AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%@samsara-service-account.com'
AND cr.orgid IS NOT NULL
AND cr.routename IN ('fleet_supervisor_insights')
AND TO_DATE(cr.mp_date) BETWEEN DATE_SUB('2025-09-15', 55)
AND '2025-09-15'
),
total_events
AS (
SELECT *
FROM cloud_route_events
UNION ALL
SELECT *
FROM mixpanel_events
),
total_events_aggregated
AS (
SELECT
org_id,
COUNT(CASE WHEN date BETWEEN DATE_SUB('2025-09-15', 6)
AND '2025-09-15' THEN 1 END) AS usage_weekly,
COUNT(CASE WHEN date BETWEEN DATE_SUB('2025-09-15', 27)
AND '2025-09-15' THEN 1 END) AS usage_monthly,
COUNT(CASE WHEN date BETWEEN DATE_SUB('2025-09-15', 55)
AND DATE_SUB('2025-09-15', 28) THEN 1 END) AS usage_prior_month,
COUNT(CASE WHEN date BETWEEN DATE_SUB('2025-09-15', 34)
AND DATE_SUB('2025-09-15', 28) THEN 1 END) AS usage_weekly_prior_month,
COUNT(DISTINCT CASE WHEN date = '2025-09-15' THEN user_id END) AS daily_active_user,
COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('2025-09-15', 6)
AND '2025-09-15' THEN user_id END) AS weekly_active_users,
COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('2025-09-15', 27)
AND '2025-09-15' THEN user_id END) AS monthly_active_users,
MAX(CASE WHEN date = '2025-09-15' THEN 1 ELSE 0 END) AS org_active_day
FROM total_events
GROUP BY org_id
),
all_user_events
AS (
SELECT org_id, date, user_id
FROM total_events
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
FROM total_events_aggregated
) m
FULL OUTER
JOIN user_metrics um ON m.org_id = um.org_id
GROUP BY m.org_id, um.daily_active_user, um.weekly_active_users, um.monthly_active_users
)

