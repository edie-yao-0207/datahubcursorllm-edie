relevant_segments
AS (
-- Get any segments for the FF
SELECT *
FROM datamodel_launchdarkly_bronze.segment_flag
WHERE
key  IN ('expose-workflow-action-engine-immobilization')
AND date = (
SELECT MAX(date)
FROM datamodel_launchdarkly_bronze.segment_flag
WHERE date <= GREATEST('2025-09-15', '2025-10-01')
)
),
variations
AS (
-- Get all variations for the FF
SELECT
ffv.value,
ffv.feature_flag_key,
vim.variation_index
FROM datamodel_launchdarkly_bronze.feature_flag_variation ffv
JOIN release_management.ld_flag_variation_index_map vim
ON ffv.feature_flag_key = vim.key
AND UPPER(ffv.value) = UPPER(vim.variation_value)
WHERE
ffv.feature_flag_key  IN ('expose-workflow-action-engine-immobilization')
AND ffv.date = (
SELECT MAX(date)
FROM datamodel_launchdarkly_bronze.feature_flag_variation
WHERE date <= GREATEST('2025-09-15', '2025-10-01')
)
),
fallthrough_variation
AS (
SELECT *
FROM datamodel_launchdarkly_bronze.feature_flag_environment
WHERE
feature_flag_key  IN ('expose-workflow-action-engine-immobilization')
AND date = (
SELECT MAX(date)
FROM datamodel_launchdarkly_bronze.feature_flag_environment
WHERE date <= GREATEST('2025-09-15', '2025-10-01')
)
),
segment_parsing
AS (
-- Get various rules from segments for org enablement
SELECT
rs.key AS feature_flag_key,
rc.segment_rule_id AS rule_id,
rc.attribute,
rc.op,
rc.negate,
FROM_JSON(rc.values, 'array<string>') AS value_array
FROM datamodel_launchdarkly_bronze.rule_clause rc
JOIN relevant_segments rs
ON rc.segment_key = rs.segment_key
WHERE
rc.attribute IN ('orgId', 'orgCreatedAt', 'orgLocale', 'orgReleaseType', 'samNumber', 'orgIsInternal')
AND rc.date = (
SELECT MAX(date)
FROM datamodel_launchdarkly_bronze.rule_clause rc
WHERE rc.date <= GREATEST('2025-09-15', '2025-10-01')
)
),
relevant_rules
AS (
-- Get all relevant rules for the FF
SELECT *
FROM datamodel_launchdarkly_bronze.feature_flag_environment_rule
WHERE
feature_flag_key  IN ('expose-workflow-action-engine-immobilization')
AND date = (
SELECT MAX(date)
FROM datamodel_launchdarkly_bronze.feature_flag_environment_rule
WHERE date <= GREATEST('2025-09-15', '2025-10-01')
)
),
rule_clauses_parsed
AS (
-- Parse to find all orgs that are also enabled from FF rules
SELECT
rc.feature_flag_key,
rc.feature_flag_environment_rule_id AS rule_id,
rc.attribute,
rc.op,
rc.negate,
LOWER(v.value) AS variation,
FROM_JSON(rc.values, 'array<string>') AS value_array
FROM datamodel_launchdarkly_bronze.feature_flag_environment_rule_clause rc
JOIN relevant_rules ffr
ON ffr.id = rc.feature_flag_environment_rule_id
JOIN variations v
ON ffr.variation = v.variation_index
AND rc.feature_flag_key = v.feature_flag_key
WHERE
rc.attribute IN ('orgId', 'orgCreatedAt', 'orgLocale', 'orgReleaseType', 'samNumber', 'orgIsInternal')
AND rc.date = (
SELECT MAX(date)
FROM datamodel_launchdarkly_bronze.feature_flag_environment_rule_clause
WHERE date <= GREATEST('2025-09-15', '2025-10-01')
)
),
launchdarkly_orgs
AS (
SELECT DISTINCT
feature_flag_key,
rule_id,
attribute,
'true' AS variation,
op,
negate,
value
FROM segment_parsing
LATERAL VIEW EXPLODE(value_array) AS value
UNION
SELECT DISTINCT
feature_flag_key,
rule_id,
attribute,
variation,
op,
negate,
value
FROM rule_clauses_parsed
LATERAL VIEW EXPLODE(value_array) AS value
),
pivoted_conditions
AS (
-- Pivot to apply multiple conditions in the same rule if exists
SELECT
feature_flag_key,
rule_id,
variation,
op,
negate,
COLLECT_SET(CASE WHEN attribute = 'orgId' THEN value ELSE NULL END) AS org_ids,
COLLECT_SET(CASE WHEN attribute = 'orgLocale' THEN value ELSE NULL END) AS org_locales,
COLLECT_SET(CASE WHEN attribute = 'orgReleaseType' THEN value ELSE NULL END) AS org_release_types,
COLLECT_SET(CASE WHEN attribute = 'orgCreatedAt' THEN value ELSE NULL END) AS org_created_at,
COLLECT_SET(CASE WHEN attribute = 'samNumber' THEN value ELSE NULL END) AS sam_numbers,
COLLECT_SET(CASE WHEN attribute = 'orgIsInternal' THEN value ELSE NULL END) AS is_internal
FROM launchdarkly_orgs
GROUP BY feature_flag_key, rule_id, variation, op, negate
),
pivoted_conditions_exploded
AS (
SELECT
feature_flag_key,
rule_id,
variation,
op,
negate,
org_ids,
org_locales,
org_release_types,
sam_numbers,
is_internal,
exploded_ts AS org_created_at_ts
FROM (
SELECT
*,
EXPLODE(
CASE
WHEN SIZE(org_created_at) = 0
OR org_created_at IS NULL THEN ARRAY(NULL)
ELSE org_created_at
END
) AS exploded_ts
FROM pivoted_conditions
) pc
),
pubsec_us
AS (
-- This identifies whether an organization is PubSec or not
-- Note that we only care about this for US accounts
SELECT
sam_number,
sled_account
FROM finopsdb.customer_info
),
pubsec_eu
AS (
-- This identifies whether an organization is PubSec or not
-- Note that we only care about this for US accounts
SELECT
sam_number,
sled_account
FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-finopsdb/finopsdb/customer_info_v0`
),
account_arr
AS (
-- Handles edge case where multiple accounts with different ARRs exist for a given SAM
SELECT
account_id,
customer_arr,
customer_arr_segment
FROM product_analytics_staging.stg_customer_enriched
WHERE date = (
SELECT MAX(date)
FROM product_analytics_staging.stg_customer_enriched)
),
account_arr_sam
AS (
-- Handles cases where there's no account_id match (take max for SAM instead)
SELECT sam_number_undecorated AS sam_number,
MAX_BY(customer_arr, customer_arr) AS customer_arr,
MAX_BY(customer_arr_segment, customer_arr) AS customer_arr_segment
FROM product_analytics_staging.stg_customer_enriched
WHERE date = (
SELECT MAX(date)
FROM product_analytics_staging.stg_customer_enriched)
GROUP BY sam_number_undecorated
),
relevant_orgs
AS (
SELECT
o.org_id,
org_name,
CASE
WHEN COALESCE(a.customer_arr, a_sam.customer_arr) IS NULL THEN 'Unknown'
WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 0
AND 1000 THEN '0 - 1k'
WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 1000.01
AND 10000 THEN '1k - 10k'
WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 10000.01
AND 100000 THEN '10k - 100k'
ELSE COALESCE(a.customer_arr_segment, a_sam.customer_arr_segment)
END AS account_arr_segment,
COALESCE(o.account_size_segment_name, 'Unknown') AS account_size_segment_name,
COALESCE(CASE WHEN o.account_billing_country IN ('United Kingdom', 'Ireland', 'Isle of Man') THEN 'UK&I'
WHEN o.account_billing_country IN ('Germany', 'Austria', 'Switzerland') THEN 'DACH'
WHEN o.account_billing_country IN ('Belgium', 'Netherlands', 'Luxembourg') THEN 'BNL'
WHEN o.account_billing_country IN ('Bulgaria', 'Czech Republic', 'Denmark', 'France', 'Hungary', 'Italy', 'Poland', 'Romania', 'Slovakia', 'Spain', 'Sweden') THEN 'Mainland Europe: Others'
WHEN o.account_billing_country = 'United States'
AND COALESCE(p.sled_account, 0) = 1 THEN 'United States PubSec'
WHEN o.account_billing_country = 'United States'
AND COALESCE(p.sled_account, 0) = 0 THEN 'United States Private'
WHEN o.account_billing_country IN ('Canada', 'Mexico') THEN o.account_billing_country
WHEN o.account_billing_country IS NULL THEN NULL
ELSE 'Other' END, 'Unknown') AS account_billing_country,
COALESCE(o.sam_number, 'No SAM Number') AS sam_number,
COALESCE(o.account_id, 'No Account ID') AS account_id,
COALESCE(o.account_name, 'No Account Name') AS account_name,
c.org_category,
o.created_at,
o.locale,
o.release_type,
'us-west-2' AS region
FROM datamodel_core.dim_organizations o
JOIN product_analytics_staging.dim_organizations_classification c
ON o.date = c.date
AND o.org_id = c.org_id
LEFT OUTER
JOIN account_arr a
ON o.account_id = a.account_id
LEFT OUTER
JOIN account_arr_sam a_sam
ON o.sam_number = a_sam.sam_number
LEFT OUTER
JOIN pubsec_us p
ON o.sam_number = p.sam_number
WHERE
o.date = '2025-09-15'
UNION ALL
SELECT
o.org_id,
org_name,
CASE
WHEN COALESCE(a.customer_arr, a_sam.customer_arr) IS NULL THEN 'Unknown'
WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 0
AND 1000 THEN '0 - 1k'
WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 1000.01
AND 10000 THEN '1k - 10k'
WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 10000.01
AND 100000 THEN '10k - 100k'
ELSE COALESCE(a.customer_arr_segment, a_sam.customer_arr_segment)
END AS account_arr_segment,
COALESCE(o.account_size_segment_name, 'Unknown') AS account_size_segment_name,
COALESCE(CASE WHEN o.account_billing_country IN ('United Kingdom', 'Ireland', 'Isle of Man') THEN 'UK&I'
WHEN o.account_billing_country IN ('Germany', 'Austria', 'Switzerland') THEN 'DACH'
WHEN o.account_billing_country IN ('Belgium', 'Netherlands', 'Luxembourg') THEN 'BNL'
WHEN o.account_billing_country IN ('Bulgaria', 'Czech Republic', 'Denmark', 'France', 'Hungary', 'Italy', 'Poland', 'Romania', 'Slovakia', 'Spain', 'Sweden') THEN 'Mainland Europe: Others'
WHEN o.account_billing_country = 'United States'
AND COALESCE(p.sled_account, 0) = 1 THEN 'United States PubSec'
WHEN o.account_billing_country = 'United States'
AND COALESCE(p.sled_account, 0) = 0 THEN 'United States Private'
WHEN o.account_billing_country IN ('Canada', 'Mexico') THEN o.account_billing_country
WHEN o.account_billing_country IS NULL THEN NULL
ELSE 'Other' END, 'Unknown') AS account_billing_country,
COALESCE(o.sam_number, 'No SAM Number') AS sam_number,
COALESCE(o.account_id, 'No Account ID') AS account_id,
COALESCE(o.account_name, 'No Account Name') AS account_name,
c.org_category,
o.created_at,
o.locale,
o.release_type,
'eu-west-1' AS region
FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` o
JOIN data_tools_delta_share.product_analytics_staging.dim_organizations_classification c
ON o.date = c.date
AND o.org_id = c.org_id
LEFT OUTER
JOIN account_arr a
ON o.account_id = a.account_id
LEFT OUTER
JOIN account_arr_sam a_sam
ON o.sam_number = a_sam.sam_number
LEFT OUTER
JOIN pubsec_eu p
ON o.sam_number = p.sam_number
WHERE
o.date = '2025-09-15'
),
relevant_licenses
AS (
SELECT DISTINCT o.org_id, fo.product_sku AS sku, xref.is_legacy_license
FROM edw.silver.fct_license_orders_daily_snapshot fo
JOIN datamodel_core.dim_organizations o
ON fo.sam_number = COALESCE(o.salesforce_sam_number, o.sam_number)
JOIN edw.silver.license_hw_sku_xref xref
ON fo.product_sku = xref.license_sku
WHERE
o.date = '2025-09-15'
AND DATE(fo._run_dt) BETWEEN DATE_SUB('2025-09-15', 6)
AND '2025-09-15'
AND (
(fo.net_quantity > 0)
)
AND (
fo.product_sku IN ('LIC-EI-SEC')
)
UNION ALL
SELECT DISTINCT o.org_id, fo.product_sku AS sku, xref.is_legacy_license
FROM edw.silver.fct_license_orders_daily_snapshot fo
JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` o
ON fo.sam_number = COALESCE(o.salesforce_sam_number, o.sam_number)
JOIN edw.silver.license_hw_sku_xref xref
ON fo.product_sku = xref.license_sku
WHERE
o.date = '2025-09-15'
AND DATE(fo._run_dt) BETWEEN DATE_SUB('2025-09-15', 6)
AND '2025-09-15'
AND (
(fo.net_quantity > 0)
)
AND (
fo.product_sku IN ('LIC-EI-SEC')
)
),
relevant_licenses_deduped
AS (
SELECT DISTINCT org_id
FROM relevant_licenses
),
relevant_licenses_final
AS (
SELECT DISTINCT org_id
FROM relevant_licenses_deduped
),
opt_in_orgs_us
AS (
-- Orgs who have opted in via the Feature Flag
SELECT
o.id AS org_id,
fp.ld_feature_key,
MAX_BY(fpss.enabled, fpss.updated_at) as is_feature_enabled,
MAX_BY(DATE(fpss.updated_at), fpss.updated_at) AS enabled_date
FROM releasemanagementdb_shards.feature_packages AS fp
JOIN releasemanagementdb_shards.feature_package_self_serve AS fpss
ON fpss.feature_package_uuid = fp.uuid
JOIN clouddb.organizations AS o
ON o.id = fpss.org_id
WHERE
fp.ld_feature_key  IN ('expose-workflow-action-engine-immobilization')
AND fp.deleted = 0 -- still active
AND fpss.scope = 1 -- org-scoped
GROUP BY
ALL
HAVING
is_feature_enabled = 1
),
feature_packages_eu
AS (
SELECT
ld_feature_key,
uuid
FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-releasemanagement-shard-1db/releasemanagementdb/feature_packages_v0`
WHERE deleted = 0
UNION ALL
SELECT
ld_feature_key,
uuid
FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-releasemanagementdb/releasemanagementdb/feature_packages_v0`
WHERE deleted = 0
),
feature_package_self_serve_eu
AS (
SELECT
feature_package_uuid,
enabled,
updated_at,
org_id,
scope
FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-releasemanagement-shard-1db/releasemanagementdb/feature_package_self_serve_v2`
UNION ALL
SELECT
feature_package_uuid,
enabled,
updated_at,
org_id,
scope
FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-releasemanagementdb/releasemanagementdb/feature_package_self_serve_v2`
),
opt_in_orgs_eu
AS (
-- Orgs who have opted in via the Feature Flag
SELECT
o.id AS org_id,
fp.ld_feature_key,
MAX_BY(fpss.enabled, fpss.updated_at) as is_feature_enabled,
MAX_BY(DATE(fpss.updated_at), fpss.updated_at) AS enabled_date
FROM feature_packages_eu AS fp
JOIN feature_package_self_serve_eu AS fpss
ON fpss.feature_package_uuid = fp.uuid
JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-clouddb/prod_db/organizations_v3` AS o
ON o.id = fpss.org_id
WHERE
fp.ld_feature_key  IN ('expose-workflow-action-engine-immobilization')
AND fpss.scope = 1 --org-scoped
GROUP BY
ALL
HAVING
is_feature_enabled = 1
),
final_relevant_orgs
AS (
-- Final set of relevant orgs for enablement
-- 1. Orgs with a license from self-serve that are not explicitly excluded from LD
-- 2. Orgs with a license that are explicitly granted access by LD
SELECT
o.org_id,
o.org_name,
o.account_arr_segment,
o.account_size_segment_name,
o.account_billing_country,
o.sam_number,
o.account_id,
o.account_name,
o.org_category,
o.region
FROM relevant_orgs o
JOIN opt_in_orgs_us oo
ON o.org_id = oo.org_id
JOIN relevant_licenses_final rld
ON o.org_id = rld.org_id
LEFT OUTER
JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
ON
(
LOWER(ldo.variation) = 'false'
AND (
SIZE(ldo.org_ids) = 0
OR (
ldo.negate = FALSE
AND (
CASE WHEN ldo.op = 'in' THEN ARRAY_CONTAINS(ldo.org_ids, CAST(o.org_id AS STRING))
WHEN ldo.op = 'lessThan' THEN o.org_id < CAST(ldo.org_ids[0] AS BIGINT)
WHEN ldo.op = 'greaterThan' THEN o.org_id > CAST(ldo.org_ids[0] AS BIGINT) END
)
)
OR (
ldo.negate = TRUE
AND NOT (
CASE WHEN ldo.op = 'in' THEN ARRAY_CONTAINS(ldo.org_ids, CAST(o.org_id AS STRING))
WHEN ldo.op = 'lessThan' THEN o.org_id < CAST(ldo.org_ids[0] AS BIGINT)
WHEN ldo.op = 'greaterThan' THEN o.org_id > CAST(ldo.org_ids[0] AS BIGINT) END
)
)
)
AND (
ldo.org_created_at_ts IS NULL
OR (
ldo.negate = FALSE
AND (
CASE WHEN ldo.op IN ('greaterThanOrEqual') THEN DATE(o.created_at) >= DATE(
FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000))
WHEN ldo.op IN ('after', 'greaterThan') THEN DATE(o.created_at) > DATE(
FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000))
WHEN ldo.op IN ('before', 'lessThan') THEN DATE(o.created_at) < DATE(
FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000)) END
)
)
OR (
ldo.negate = TRUE
AND NOT (
CASE WHEN ldo.op IN ('greaterThanOrEqual') THEN DATE(o.created_at) >= DATE(
FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000))
WHEN ldo.op IN ('after', 'greaterThan') THEN DATE(o.created_at) > DATE(
FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000))
WHEN ldo.op IN ('before', 'lessThan') THEN DATE(o.created_at) < DATE(
FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000)) END
)
)
)
AND (
SIZE(ldo.org_locales) = 0
OR (
ldo.negate = FALSE
AND (
ARRAY_CONTAINS(ldo.org_locales, o.locale)
)
)
OR (
ldo.negate = TRUE
AND NOT (
ARRAY_CONTAINS(ldo.org_locales, o.locale)
)
)
)
AND (
SIZE(ldo.sam_numbers) = 0
OR (
ldo.negate = FALSE
AND (
ARRAY_CONTAINS(ldo.sam_numbers, o.sam_number)
)
)
OR (
ldo.negate = TRUE
AND NOT (
ARRAY_CONTAINS(ldo.sam_numbers, o.sam_number)
)
)
)
AND (
SIZE(ldo.org_release_types) = 0
OR (
ldo.negate = FALSE
AND (
(ARRAY_CONTAINS(ldo.org_release_types, 'EARLY_ADOPTER')
AND o.release_type = 'Early Adopter')
OR
(ARRAY_CONTAINS(ldo.org_release_types, 'PHASE_1')
AND o.release_type = 'Phase 1')
OR
(ARRAY_CONTAINS(ldo.org_release_types, 'PHASE_2')
AND o.release_type = 'Phase 2')
)
)
OR (
ldo.negate = TRUE
AND NOT (
(ARRAY_CONTAINS(ldo.org_release_types, 'EARLY_ADOPTER')
AND o.release_type = 'Early Adopter')
OR
(ARRAY_CONTAINS(ldo.org_release_types, 'PHASE_1')
AND o.release_type = 'Phase 1')
OR
(ARRAY_CONTAINS(ldo.org_release_types, 'PHASE_2')
AND o.release_type = 'Phase 2')
)
)
)
AND (
SIZE(ldo.is_internal) = 0
OR (
ldo.negate = FALSE
AND (
ARRAY_CONTAINS(ldo.is_internal, 'true')
AND o.org_category = 'Internal Orgs'
)
)
OR (
ldo.negate = TRUE
AND NOT (
ARRAY_CONTAINS(ldo.is_internal, 'true')
AND o.org_category = 'Internal Orgs'
)
)
)
)
WHERE ldo.variation IS NULL
AND o.release_type IN ('Early Adopter', 'Phase 1')
AND enabled_date <= '2025-09-15'
UNION
SELECT
o.org_id,
o.org_name,
o.account_arr_segment,
o.account_size_segment_name,
o.account_billing_country,
o.sam_number,
o.account_id,
o.account_name,
o.org_category,
o.region
FROM relevant_orgs o
JOIN opt_in_orgs_eu oo
ON o.org_id = oo.org_id
JOIN relevant_licenses_final rld
ON o.org_id = rld.org_id
LEFT OUTER
JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
ON
(
LOWER(ldo.variation) = 'false'
AND (
SIZE(ldo.org_ids) = 0
OR (
ldo.negate = FALSE
AND (
CASE WHEN ldo.op = 'in' THEN ARRAY_CONTAINS(ldo.org_ids, CAST(o.org_id AS STRING))
WHEN ldo.op = 'lessThan' THEN o.org_id < CAST(ldo.org_ids[0] AS BIGINT)
WHEN ldo.op = 'greaterThan' THEN o.org_id > CAST(ldo.org_ids[0] AS BIGINT) END
)
)
OR (
ldo.negate = TRUE
AND NOT (
CASE WHEN ldo.op = 'in' THEN ARRAY_CONTAINS(ldo.org_ids, CAST(o.org_id AS STRING))
WHEN ldo.op = 'lessThan' THEN o.org_id < CAST(ldo.org_ids[0] AS BIGINT)
WHEN ldo.op = 'greaterThan' THEN o.org_id > CAST(ldo.org_ids[0] AS BIGINT) END
)
)
)
AND (
ldo.org_created_at_ts IS NULL
OR (
ldo.negate = FALSE
AND (
CASE WHEN ldo.op IN ('greaterThanOrEqual') THEN DATE(o.created_at) >= DATE(
FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000))
WHEN ldo.op IN ('after', 'greaterThan') THEN DATE(o.created_at) > DATE(
FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000))
WHEN ldo.op IN ('before', 'lessThan') THEN DATE(o.created_at) < DATE(
FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000)) END
)
)
OR (
ldo.negate = TRUE
AND NOT (
CASE WHEN ldo.op IN ('greaterThanOrEqual') THEN DATE(o.created_at) >= DATE(
FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000))
WHEN ldo.op IN ('after', 'greaterThan') THEN DATE(o.created_at) > DATE(
FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000))
WHEN ldo.op IN ('before', 'lessThan') THEN DATE(o.created_at) < DATE(
FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000)) END
)
)
)
AND (
SIZE(ldo.org_locales) = 0
OR (
ldo.negate = FALSE
AND (
ARRAY_CONTAINS(ldo.org_locales, o.locale)
)
)
OR (
ldo.negate = TRUE
AND NOT (
ARRAY_CONTAINS(ldo.org_locales, o.locale)
)
)
)
AND (
SIZE(ldo.sam_numbers) = 0
OR (
ldo.negate = FALSE
AND (
ARRAY_CONTAINS(ldo.sam_numbers, o.sam_number)
)
)
OR (
ldo.negate = TRUE
AND NOT (
ARRAY_CONTAINS(ldo.sam_numbers, o.sam_number)
)
)
)
AND (
SIZE(ldo.org_release_types) = 0
OR (
ldo.negate = FALSE
AND (
(ARRAY_CONTAINS(ldo.org_release_types, 'EARLY_ADOPTER')
AND o.release_type = 'Early Adopter')
OR
(ARRAY_CONTAINS(ldo.org_release_types, 'PHASE_1')
AND o.release_type = 'Phase 1')
OR
(ARRAY_CONTAINS(ldo.org_release_types, 'PHASE_2')
AND o.release_type = 'Phase 2')
)
)
OR (
ldo.negate = TRUE
AND NOT (
(ARRAY_CONTAINS(ldo.org_release_types, 'EARLY_ADOPTER')
AND o.release_type = 'Early Adopter')
OR
(ARRAY_CONTAINS(ldo.org_release_types, 'PHASE_1')
AND o.release_type = 'Phase 1')
OR
(ARRAY_CONTAINS(ldo.org_release_types, 'PHASE_2')
AND o.release_type = 'Phase 2')
)
)
)
AND (
SIZE(ldo.is_internal) = 0
OR (
ldo.negate = FALSE
AND (
ARRAY_CONTAINS(ldo.is_internal, 'true')
AND o.org_category = 'Internal Orgs'
)
)
OR (
ldo.negate = TRUE
AND NOT (
ARRAY_CONTAINS(ldo.is_internal, 'true')
AND o.org_category = 'Internal Orgs'
)
)
)
)
WHERE ldo.variation IS NULL
AND o.release_type IN ('Early Adopter', 'Phase 1')
AND enabled_date <= '2025-09-15'
UNION
SELECT
o.org_id,
o.org_name,
o.account_arr_segment,
o.account_size_segment_name,
o.account_billing_country,
o.sam_number,
o.account_id,
o.account_name,
o.org_category,
o.region
FROM relevant_orgs o
JOIN relevant_licenses_final rld
ON o.org_id = rld.org_id
LEFT OUTER
JOIN fallthrough_variation fv
ON fv.feature_flag_key  IN ('expose-workflow-action-engine-immobilization')
LEFT OUTER
JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
ON
(
LOWER(ldo.variation) = 'false'
AND (
SIZE(ldo.org_ids) = 0
OR (
ldo.negate = FALSE
AND (
CASE WHEN ldo.op = 'in' THEN ARRAY_CONTAINS(ldo.org_ids, CAST(o.org_id AS STRING))
WHEN ldo.op = 'lessThan' THEN o.org_id < CAST(ldo.org_ids[0] AS BIGINT)
WHEN ldo.op = 'greaterThan' THEN o.org_id > CAST(ldo.org_ids[0] AS BIGINT) END
)
)
OR (
ldo.negate = TRUE
AND NOT (
CASE WHEN ldo.op = 'in' THEN ARRAY_CONTAINS(ldo.org_ids, CAST(o.org_id AS STRING))
WHEN ldo.op = 'lessThan' THEN o.org_id < CAST(ldo.org_ids[0] AS BIGINT)
WHEN ldo.op = 'greaterThan' THEN o.org_id > CAST(ldo.org_ids[0] AS BIGINT) END
)
)
)
AND (
ldo.org_created_at_ts IS NULL
OR (
ldo.negate = FALSE
AND (
CASE WHEN ldo.op IN ('greaterThanOrEqual') THEN DATE(o.created_at) >= DATE(
FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000))
WHEN ldo.op IN ('after', 'greaterThan') THEN DATE(o.created_at) > DATE(
FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000))
WHEN ldo.op IN ('before', 'lessThan') THEN DATE(o.created_at) < DATE(
FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000)) END
)
)
OR (
ldo.negate = TRUE
AND NOT (
CASE WHEN ldo.op IN ('greaterThanOrEqual') THEN DATE(o.created_at) >= DATE(
FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000))
WHEN ldo.op IN ('after', 'greaterThan') THEN DATE(o.created_at) > DATE(
FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000))
WHEN ldo.op IN ('before', 'lessThan') THEN DATE(o.created_at) < DATE(
FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000)) END
)
)
)
AND (
SIZE(ldo.org_locales) = 0
OR (
ldo.negate = FALSE
AND (
ARRAY_CONTAINS(ldo.org_locales, o.locale)
)
)
OR (
ldo.negate = TRUE
AND NOT (
ARRAY_CONTAINS(ldo.org_locales, o.locale)
)
)
)
AND (
SIZE(ldo.sam_numbers) = 0
OR (
ldo.negate = FALSE
AND (
ARRAY_CONTAINS(ldo.sam_numbers, o.sam_number)
)
)
OR (
ldo.negate = TRUE
AND NOT (
ARRAY_CONTAINS(ldo.sam_numbers, o.sam_number)
)
)
)
AND (
SIZE(ldo.org_release_types) = 0
OR (
ldo.negate = FALSE
AND (
(ARRAY_CONTAINS(ldo.org_release_types, 'EARLY_ADOPTER')
AND o.release_type = 'Early Adopter')
OR
(ARRAY_CONTAINS(ldo.org_release_types, 'PHASE_1')
AND o.release_type = 'Phase 1')
OR
(ARRAY_CONTAINS(ldo.org_release_types, 'PHASE_2')
AND o.release_type = 'Phase 2')
)
)
OR (
ldo.negate = TRUE
AND NOT (
(ARRAY_CONTAINS(ldo.org_release_types, 'EARLY_ADOPTER')
AND o.release_type = 'Early Adopter')
OR
(ARRAY_CONTAINS(ldo.org_release_types, 'PHASE_1')
AND o.release_type = 'Phase 1')
OR
(ARRAY_CONTAINS(ldo.org_release_types, 'PHASE_2')
AND o.release_type = 'Phase 2')
)
)
)
AND (
SIZE(ldo.is_internal) = 0
OR (
ldo.negate = FALSE
AND (
ARRAY_CONTAINS(ldo.is_internal, 'true')
AND o.org_category = 'Internal Orgs'
)
)
OR (
ldo.negate = TRUE
AND NOT (
ARRAY_CONTAINS(ldo.is_internal, 'true')
AND o.org_category = 'Internal Orgs'
)
)
)
)
AND ldo.feature_flag_key = fv.feature_flag_key
WHERE (
ldo.variation IS NULL
AND fv.fallthrough_variation = 1 -- true by default
)
AND o.release_type IN ('Early Adopter', 'Phase 1')
UNION
SELECT
o.org_id,
o.org_name,
o.account_arr_segment,
o.account_size_segment_name,
o.account_billing_country,
o.sam_number,
o.account_id,
o.account_name,
o.org_category,
o.region
FROM relevant_orgs o
JOIN relevant_licenses_final rld
ON o.org_id = rld.org_id
JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
ON
(
LOWER(ldo.variation) = 'true'
AND (
SIZE(ldo.org_ids) = 0
OR (
ldo.negate = FALSE
AND (
CASE WHEN ldo.op = 'in' THEN ARRAY_CONTAINS(ldo.org_ids, CAST(o.org_id AS STRING))
WHEN ldo.op = 'lessThan' THEN o.org_id < CAST(ldo.org_ids[0] AS BIGINT)
WHEN ldo.op = 'greaterThan' THEN o.org_id > CAST(ldo.org_ids[0] AS BIGINT) END
)
)
OR (
ldo.negate = TRUE
AND NOT (
CASE WHEN ldo.op = 'in' THEN ARRAY_CONTAINS(ldo.org_ids, CAST(o.org_id AS STRING))
WHEN ldo.op = 'lessThan' THEN o.org_id < CAST(ldo.org_ids[0] AS BIGINT)
WHEN ldo.op = 'greaterThan' THEN o.org_id > CAST(ldo.org_ids[0] AS BIGINT) END
)
)
)
AND (
ldo.org_created_at_ts IS NULL
OR (
ldo.negate = FALSE
AND (
CASE WHEN ldo.op IN ('greaterThanOrEqual') THEN DATE(o.created_at) >= DATE(
FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000))
WHEN ldo.op IN ('after', 'greaterThan') THEN DATE(o.created_at) > DATE(
FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000))
WHEN ldo.op IN ('before', 'lessThan') THEN DATE(o.created_at) < DATE(
FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000)) END
)
)
OR (
ldo.negate = TRUE
AND NOT (
CASE WHEN ldo.op IN ('greaterThanOrEqual') THEN DATE(o.created_at) >= DATE(
FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000))
WHEN ldo.op IN ('after', 'greaterThan') THEN DATE(o.created_at) > DATE(
FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000))
WHEN ldo.op IN ('before', 'lessThan') THEN DATE(o.created_at) < DATE(
FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000)) END
)
)
)
AND (
SIZE(ldo.org_locales) = 0
OR (
ldo.negate = FALSE
AND (
ARRAY_CONTAINS(ldo.org_locales, o.locale)
)
)
OR (
ldo.negate = TRUE
AND NOT (
ARRAY_CONTAINS(ldo.org_locales, o.locale)
)
)
)
AND (
SIZE(ldo.sam_numbers) = 0
OR (
ldo.negate = FALSE
AND (
ARRAY_CONTAINS(ldo.sam_numbers, o.sam_number)
)
)
OR (
ldo.negate = TRUE
AND NOT (
ARRAY_CONTAINS(ldo.sam_numbers, o.sam_number)
)
)
)
AND (
SIZE(ldo.org_release_types) = 0
OR (
ldo.negate = FALSE
AND (
(ARRAY_CONTAINS(ldo.org_release_types, 'EARLY_ADOPTER')
AND o.release_type = 'Early Adopter')
OR
(ARRAY_CONTAINS(ldo.org_release_types, 'PHASE_1')
AND o.release_type = 'Phase 1')
OR
(ARRAY_CONTAINS(ldo.org_release_types, 'PHASE_2')
AND o.release_type = 'Phase 2')
)
)
OR (
ldo.negate = TRUE
AND NOT (
(ARRAY_CONTAINS(ldo.org_release_types, 'EARLY_ADOPTER')
AND o.release_type = 'Early Adopter')
OR
(ARRAY_CONTAINS(ldo.org_release_types, 'PHASE_1')
AND o.release_type = 'Phase 1')
OR
(ARRAY_CONTAINS(ldo.org_release_types, 'PHASE_2')
AND o.release_type = 'Phase 2')
)
)
)
AND (
SIZE(ldo.is_internal) = 0
OR (
ldo.negate = FALSE
AND (
ARRAY_CONTAINS(ldo.is_internal, 'true')
AND o.org_category = 'Internal Orgs'
)
)
OR (
ldo.negate = TRUE
AND NOT (
ARRAY_CONTAINS(ldo.is_internal, 'true')
AND o.org_category = 'Internal Orgs'
)
)
)
)
WHERE 1=1
),
