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
license_filter
AS (
SELECT DISTINCT o.org_id
FROM edw.silver.fct_license_orders_daily_snapshot fo
JOIN datamodel_core.dim_organizations o
ON fo.sam_number = COALESCE(o.salesforce_sam_number, o.sam_number)
JOIN edw.silver.license_hw_sku_xref xref
ON fo.product_sku = xref.license_sku
WHERE
o.date = '2025-09-15'
AND DATE(fo._run_dt) BETWEEN DATE_SUB('2025-09-15', 6)
AND '2025-09-15'
AND (fo.net_quantity > 0)
AND (
fo.product_sku LIKE 'LIC-AG%'
OR fo.product_sku LIKE 'LIC-VG%'
OR fo.product_sku LIKE 'LIC-CM%'
OR fo.product_sku LIKE 'LIC-AT%'
OR fo.product_sku LIKE 'LIC-TRLR%'
)
AND xref.is_core_license = TRUE
AND fo.product_sku != 'LIC-VG-ASAT'
UNION ALL
SELECT DISTINCT o.org_id
FROM edw.silver.fct_license_orders_daily_snapshot fo
JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` o
ON fo.sam_number = COALESCE(o.salesforce_sam_number, o.sam_number)
JOIN edw.silver.license_hw_sku_xref xref
ON fo.product_sku = xref.license_sku
WHERE
o.date = '2025-09-15'
AND DATE(fo._run_dt) BETWEEN DATE_SUB('2025-09-15', 6)
AND '2025-09-15'
AND (fo.net_quantity > 0)
AND (
fo.product_sku LIKE 'LIC-AG%'
OR fo.product_sku LIKE 'LIC-VG%'
OR fo.product_sku LIKE 'LIC-CM%'
OR fo.product_sku LIKE 'LIC-AT%'
OR fo.product_sku LIKE 'LIC-TRLR%'
)
AND xref.is_core_license = TRUE
AND fo.product_sku != 'LIC-VG-ASAT'
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
final_relevant_orgs
AS (
-- Final relevant orgs have an applicable license as well
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
JOIN license_filter lf
ON o.org_id = lf.org_id
WHERE 1=1
),
