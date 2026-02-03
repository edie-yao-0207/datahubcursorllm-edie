SELECT a.date,
a.org_id,
a.org_name,
a.org_category,
a.account_arr_segment,
a.account_size_segment_name,
a.account_billing_country,
a.region,
a.sam_number,
a.account_id,
a.account_name,
a.feature,
a.enabled,
a.usage_weekly,
a.usage_monthly,
a.usage_prior_month,
a.usage_weekly_prior_month,
a.daily_active_user,
a.daily_enabled_users,
a.weekly_active_users,
a.weekly_enabled_users,
a.monthly_active_users,
a.monthly_enabled_users,
a.org_active_day
FROM product_analytics.agg_product_usage_global a
JOIN datamodel_core.dim_organizations o
  ON a.org_id = o.org_id
WHERE a.org_category NOT IN ('Failed Trial', 'Internal Orgs')
AND o.date = (SELECT MAX(date) FROM datamodel_core.dim_organizations)
