SELECT
  duo.date,
  duo.org_id,
  duo.user_id,
  last_web_login_date,
  first_web_login_date,
  sam_map.sam_number,
  duo.is_samsara_email,
  o.internal_type,
  o.account_size_segment,
  o.account_industry,
  o.account_arr_segment,
  o.is_paid_customer,
  o.is_paid_safety_customer,
  o.is_paid_telematics_customer,
  o.is_paid_stce_customer,
  c.region AS subregion,
  c.avg_mileage,
  c.fleet_size,
  c.industry_vertical,
  c.fuel_category,
  c.primary_driving_environment,
  c.fleet_composition
FROM datamodel_platform.dim_users_organizations duo
JOIN datamodel_platform_bronze.raw_clouddb_users_organizations rcuo
  ON duo.date = rcuo.date
  AND duo.org_id = rcuo.organization_id
  AND duo.user_id = rcuo.user_id
JOIN datamodel_core.dim_organizations o
  ON o.date = duo.date
  AND o.org_id = duo.org_id
LEFT JOIN product_analytics.map_org_sam_number_latest sam_map
  ON sam_map.org_id = o.org_id
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories c
  ON o.org_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories)
WHERE COALESCE(DATE(rcuo.expire_at), DATE_ADD(duo.date, 1)) >= duo.date
