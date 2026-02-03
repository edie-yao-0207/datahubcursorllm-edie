WITH base_dates AS (
    SELECT MAX(date) AS max_date
    FROM product_analytics_staging.agg_executive_scorecard_acr_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_advanced_idling_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_ai_assistant_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_ai_assistant_main_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_ai_assistant_open_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_ai_fault_codes_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_ai_fault_codes_entrypoint_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_ai_report_builder_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_ai_report_builder_entrypoint_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_api_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_asset_qualifications_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_automations_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_driver_efficiency_report_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_dvir_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_dvir2_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_driver_app_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_fleet_benchmarks_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_functions_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_fuel_card_integrations_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_positive_recognition_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_route_planning_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_routing_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_safety_inbox_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_safety_incab_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_safety_settings_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_coaching_automation_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_performance_overview_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_navigation_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_connected_training_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_connected_workflows_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_connected_asset_maintenance_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_fuel_card_transactions_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_fuel_theft_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_hos_coaching_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_hos_logs_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_idling_coaching_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_ifta_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_worker_safety_mobile_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_worker_safety_wearable_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_worker_safety_wearable_incidents_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_worker_qualifications_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_utilization_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_weather_intelligence_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_safety_training_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_spotlight_search_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_route_forms_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_virtual_coaching_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_manager_coaching_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_marketplace_integrations_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_group_coaching_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_roadside_parking_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_detention_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_custom_dashboards_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_maintenance_status_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_device_install_workflow_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_driver_detail_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_pedestrian_collision_warning_multicam_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_pedestrian_collision_warning_cm_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_pedestrian_collision_warning_cm_incab_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_multicam_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_pedestrian_collision_warning_multicam_incab_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS
)
SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Advanced Custom Reports' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_acr_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Advanced Idling Report' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_advanced_idling_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'AI Assistant' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_ai_assistant_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'AI Assistant (Main Flow)' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_ai_assistant_main_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'AI Assistant (Open Chat)' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_ai_assistant_open_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'AI Fault Code Descriptions' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_ai_fault_codes_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'AI Fault Codes (Ask Samsara)' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_ai_fault_codes_entrypoint_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'AI Report Builder (Create Report)' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_ai_report_builder_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'AI Report Builder (Build With AI)' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_ai_report_builder_entrypoint_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'API Usage' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_api_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Asset Qualifications' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_asset_qualifications_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Automations' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_automations_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Driver Efficiency Report' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_driver_efficiency_report_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
CONCAT(feature, ' Incab Alerts') AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
CAST(0 AS BIGINT) AS daily_active_user,
CAST(0 AS BIGINT) AS daily_enabled_users,
CAST(0 AS BIGINT) AS weekly_active_users,
CAST(0 AS BIGINT) AS weekly_enabled_users,
CAST(0 AS BIGINT) AS monthly_active_users,
CAST(0 AS BIGINT) AS monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_safety_incab_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'DVIR' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_dvir_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)


UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'DVIR 2.0' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_dvir2_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)
UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Driver App' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_driver_app_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
CAST(0 AS BIGINT) AS daily_active_user,
CAST(0 AS BIGINT) AS daily_enabled_users,
CAST(0 AS BIGINT) AS weekly_active_users,
CAST(0 AS BIGINT) AS weekly_enabled_users,
CAST(0 AS BIGINT) AS monthly_active_users,
CAST(0 AS BIGINT) AS monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_safety_settings_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Fleet Benchmarks' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_fleet_benchmarks_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Functions' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_functions_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Fuel Card Integrations' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_fuel_card_integrations_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Positive Recognition' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_positive_recognition_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Route Planning' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_route_planning_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Routing and Dispatch' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_routing_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Safety Inbox v2' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_safety_inbox_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Coaching Automation' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_coaching_automation_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Performance Overview' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_performance_overview_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Connected Workflows' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_connected_workflows_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Connected Training' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_connected_training_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Turn-by-Turn Navigation' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_navigation_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Connected Asset Maintenance' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_connected_asset_maintenance_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Fuel Card Transactions Investigation' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_fuel_card_transactions_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Fuel Theft' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_fuel_theft_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'HoS Coaching' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_hos_coaching_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'HoS Logs' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_hos_logs_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Idling Coaching' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_idling_coaching_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'IFTA Report' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_ifta_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Worker Safety Mobile' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_worker_safety_mobile_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Worker Safety Wearable' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_worker_safety_wearable_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Worker Safety Wearable Incidents' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_worker_safety_wearable_incidents_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Worker Qualifications' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_worker_qualifications_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Asset Utilization v2' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_utilization_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Weather Intelligence' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_weather_intelligence_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Safety Training' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_safety_training_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Spotlight Search' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_spotlight_search_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Route Forms' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_route_forms_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Driver Event Review' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_virtual_coaching_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Manager-Led Coaching' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_manager_coaching_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Marketplace Integrations' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_marketplace_integrations_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Group Coaching' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_group_coaching_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Roadside Parking' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_roadside_parking_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Detention 2.0' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_detention_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Custom Dashboards' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_custom_dashboards_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Maintenance Status' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_maintenance_status_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Device Install Workflow' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_device_install_workflow_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Driver Detail v2' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_driver_detail_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Pedestrian Collision Warning (Multicam)' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_pedestrian_collision_warning_multicam_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Pedestrian Collision Warning (Brigid)' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_pedestrian_collision_warning_cm_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Pedestrian Collision Warning (Brigid) Incab Alerts' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
CAST(0 AS BIGINT) AS daily_active_user,
CAST(0 AS BIGINT) AS daily_enabled_users,
CAST(0 AS BIGINT) AS weekly_active_users,
CAST(0 AS BIGINT) AS weekly_enabled_users,
CAST(0 AS BIGINT) AS monthly_active_users,
CAST(0 AS BIGINT) AS monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_pedestrian_collision_warning_cm_incab_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'AI Multicam' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
daily_active_user,
daily_enabled_users,
weekly_active_users,
weekly_enabled_users,
monthly_active_users,
monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_multicam_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)

UNION ALL

SELECT date,
org_id,
org_name,
org_category,
account_arr_segment,
account_size_segment_name,
account_billing_country,
region,
sam_number,
account_id,
account_name,
'Pedestrian Collision Warning (Multicam) Incab Alerts' AS feature,
enabled,
usage_weekly,
usage_monthly,
usage_prior_month,
usage_weekly_prior_month,
CAST(0 AS BIGINT) AS daily_active_user,
CAST(0 AS BIGINT) AS daily_enabled_users,
CAST(0 AS BIGINT) AS weekly_active_users,
CAST(0 AS BIGINT) AS weekly_enabled_users,
CAST(0 AS BIGINT) AS monthly_active_users,
CAST(0 AS BIGINT) AS monthly_enabled_users,
org_active_day,
org_active_week,
org_active_week_prior_month,
org_active_month,
org_active_prior_month
FROM product_analytics_staging.agg_executive_scorecard_pedestrian_collision_warning_multicam_incab_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)
