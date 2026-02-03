SELECT DISTINCT
  org_id,
  date
FROM product_analytics.dim_organizations_safety_settings
WHERE csl_enabled = TRUE
