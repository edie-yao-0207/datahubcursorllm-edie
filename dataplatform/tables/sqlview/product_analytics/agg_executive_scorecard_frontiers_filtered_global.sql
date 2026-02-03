WITH base_dates AS (
    SELECT MAX(date) AS max_date
    FROM product_analytics_staging.agg_executive_scorecard_advanced_ecodriving_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_alberta_hos_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_canada_hos_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_charge_insights_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_csl_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_low_bridge_strike_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_lsd_mapping_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_off_duty_deferral_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_oil_well_exemption_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_satellite_connectivity_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_trip_purpose_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_tachograph_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_smart_compliance_global
    WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS

    UNION ALL

    SELECT MAX(date)
    FROM product_analytics_staging.agg_executive_scorecard_ebpms_global
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
'Advanced Ecodriving' AS feature,
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
FROM product_analytics_staging.agg_executive_scorecard_advanced_ecodriving_global
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
'Alberta HoS' AS feature,
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
FROM product_analytics_staging.agg_executive_scorecard_alberta_hos_global
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
'Oil Well Exemption' AS feature,
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
FROM product_analytics_staging.agg_executive_scorecard_oil_well_exemption_global
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
'Canada HoS' AS feature,
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
FROM product_analytics_staging.agg_executive_scorecard_canada_hos_global
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
'Charge Insights' AS feature,
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
FROM product_analytics_staging.agg_executive_scorecard_charge_insights_global
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
'Commercial Speed Limits' AS feature,
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
FROM product_analytics_staging.agg_executive_scorecard_csl_global
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
'Satellite Connectivity' AS feature,
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
FROM product_analytics_staging.agg_executive_scorecard_satellite_connectivity_global
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
'Off Duty Deferral' AS feature,
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
FROM product_analytics_staging.agg_executive_scorecard_off_duty_deferral_global
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
'LSD Mapping' AS feature,
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
FROM product_analytics_staging.agg_executive_scorecard_lsd_mapping_global
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
'Trip Purpose' AS feature,
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
FROM product_analytics_staging.agg_executive_scorecard_trip_purpose_global
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
'Low Bridge Strike' AS feature,
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
FROM product_analytics_staging.agg_executive_scorecard_low_bridge_strike_global
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
'Tachograph' AS feature,
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
FROM product_analytics_staging.agg_executive_scorecard_tachograph_global
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
'EU Smart Compliance' AS feature,
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
FROM product_analytics_staging.agg_executive_scorecard_smart_compliance_global
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
'EBPMS' AS feature,
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
FROM product_analytics_staging.agg_executive_scorecard_ebpms_global
WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND date = (SELECT MIN(max_date) FROM base_dates)
