SELECT DISTINCT
  org_id,
  date
FROM data_tools_delta_share.product_analytics.dim_organizations_safety_settings
WHERE csl_enabled = TRUE
