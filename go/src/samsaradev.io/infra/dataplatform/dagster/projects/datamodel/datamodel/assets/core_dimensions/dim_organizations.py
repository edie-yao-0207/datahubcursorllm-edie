from dagster import AssetKey, DailyPartitionsDefinition

from ...common.constants import (
    SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    org_id_default_description,
    sam_number_default_description,
)
from ...common.utils import (
    AWSRegions,
    Database,
    DQGroup,
    NonEmptyDQCheck,
    Operator,
    PrimaryKeyDQCheck,
    SQLDQCheck,
    TableType,
    TrendDQCheck,
    WarehouseWriteMode,
    apply_db_overrides,
    build_assets_from_sql,
    build_table_description,
    get_all_regions,
)

key_prefix = "datamodel_core"

databases = {
    "database_bronze": Database.DATAMODEL_CORE_BRONZE,
    "database_silver": Database.DATAMODEL_CORE_SILVER,
    "database_gold": Database.DATAMODEL_CORE,
    "database_platform_gold": Database.DATAMODEL_PLATFORM,
}

database_dev_overrides = {
    "database_bronze_dev": Database.DATAMODEL_DEV,
    "database_silver_dev": Database.DATAMODEL_DEV,
    "database_gold_dev": Database.DATAMODEL_DEV,
    "database_platform_gold_dev": Database.DATAMODEL_DEV,
}

databases = apply_db_overrides(databases, database_dev_overrides)

daily_partition_def = DailyPartitionsDefinition(start_date="2023-09-05")
pipeline_group_name = "dim_organizations"

dqs = DQGroup(
    group_name=pipeline_group_name,
    partition_def=daily_partition_def,
    slack_alerts_channel=SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    regions=get_all_regions(),
)

raw_clouddb_organizations_query = """

SELECT
    organizations.*,
    '{DATEID}' AS date

FROM clouddb.organizations{TIMETRAVEL_DATE} organizations
"""

raw_clouddb_groups_query = """

SELECT
    groups.*,
    '{DATEID}' AS date

FROM clouddb.groups{TIMETRAVEL_DATE} groups
"""

raw_clouddb_sfdc_accounts_query = """

SELECT
    sfdc_accounts.*,
    '{DATEID}' AS date

FROM clouddb.sfdc_accounts{TIMETRAVEL_DATE} sfdc_accounts
"""

raw_clouddb_org_sfdc_accounts_query = """

SELECT
    org_sfdc_accounts.*,
    '{DATEID}' AS date

FROM clouddb.org_sfdc_accounts{TIMETRAVEL_DATE} org_sfdc_accounts
"""

raw_dim_sfdc_account_schema = [
    {
        "name": "account_id",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Unique ID of the account"},
    },
    {
        "name": "parent_account_id",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Unique ID of the account's parent. If there is no parent, this takes the value of the original account ID."
        },
    },
    {
        "name": "account_name",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Name of the account"},
    },
    {
        "name": "sam_number_undecorated",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": sam_number_default_description},
    },
    {
        "name": "created_date",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Date the account was created"
        },
    },
    {
        "name": "last_activity_date",
        "type": "date",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Last activity of the account"
        },
    },
    {
        "name": "actual_deactivate_date",
        "type": "date",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Date if the account was deactivated"
        },
    },
    {
        "name": "last_modified_date",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Date the account was last modified"
        },
    },
    {
        "name": "industry",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Industry classification of the account."
        },
    },
    {
        "name": "sub_industry",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Sub industry classification of the account."
        },
    },
    {
        "name": "cs_tier",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Account tier - Premier, Plus, Starter, Elite"
        },
    },
    {
        "name": "account_csm_email",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Email for the CSM of the org"},
    },
    {
        "name": "account_size_segment",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Classifies accounts by size - Small Business, Mid Market, Enterprise"
        },
    },
    {
        "name": "delinquent",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Boolean, signaling if the account is delinquent"
        },
    },
    {
        "name": "delinquency_status",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Provides additional information on delinquent accounts"
        },
    },
    {
        "name": "delinquent_as_of",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Data the account became delinquent"
        },
    },
    {
        "name": "first_purchase_date",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Date of initial account purchase"
        },
    },
    {
        "name": "billing_country",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Geographic information about the account"
        },
    },
    {
        "name": "billing_state",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Geographic information about the account"
        },
    },
    {
        "name": "auto_renewal",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Autorenewal status of SAM: TRUE if one contract associated with account has auto renewal, else FALSE."
        },
    },
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The field which partitions the table, in 'YYYY-mm-dd' format."
        },
    },
    {
        "name": "customer_arr_segment",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": """ARR from the previously publicly reporting quarter in the buckets:
            <0
            0 - 100k
            100k - 500K,
            500K - 1M
            1M+

            Note - ARR information is calculated at the account level, not the SAM Number level
            An account and have multiple SAMs. Additionally a SAM can have multiple accounts, resulting in a single SAM having multiple account_arr_segments
            """
        },
    },
    {
        "name": "ai_model_industry_archetype",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "AI model-predicted industry archetype classification for the account"
        },
    },
    {
        "name": "ai_model_primary_industry",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "AI model-predicted primary industry classification for the account"
        },
    },
    {
        "name": "ai_model_secondary_industry",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "AI model-predicted secondary industry classification for the account"
        },
    },
]

raw_dim_sfdc_account_us_query = """
-- US REGION QUERY
-- Note - Due to the fact that dim_sfdc_account data recides only in the US cluster,
-- we have slightly divergent queries between the EU and the US

WITH contract_renewal AS (
SELECT DISTINCT account_id, TRUE AS auto_renewal
FROM edw.salesforce_sterling.sfdc_contract
WHERE auto_renewal = TRUE)
SELECT
  account.account_id,
  coalesce(account.ultimate_parent_id, account.account_id) as parent_account_id,
  account.account_name,
  account.sam_number AS sam_number_undecorated,
  account.created_date,
  CAST(account.last_activity_date AS DATE) AS last_activity_date,
  CAST(account.actual_deactivate_date AS DATE) AS actual_deactivate_date,
  account.last_modified_date,
  account.industry,
  account.sub_industry,
  account.cs_tier,
  account.csm_email as account_csm_email,
  enriched.account_size_segment,
  account.is_customer_delinquent AS delinquent,
  account.delinquency_status,
  account.delinquent_as_of,
  DATE_FORMAT(account.first_purchase_date, 'yyyy-MM-dd') AS first_purchase_date,
  account.billing_country,
  account.billing_state,
  COALESCE(contract_renewal.auto_renewal, FALSE) AS auto_renewal,
  '{DATEID}' AS date,
  CASE enriched.customer_arr_segment
    WHEN '<=0'
    THEN '<0'
    ELSE COALESCE(enriched.customer_arr_segment, 'unknown')
  END AS customer_arr_segment,
  enriched.ai_model_industry_archetype,
  enriched.ai_model_primary_industry,
  enriched.ai_model_secondary_industry
FROM edw.silver.dim_customer{TIMETRAVEL_DATE} account
LEFT OUTER JOIN contract_renewal
    ON contract_renewal.account_id = account.account_id
LEFT OUTER JOIN edw.silver.dim_account_enriched enriched
    ON account.sam_number = enriched.sam_number_undecorated
    AND account.account_id = enriched.account_id
"""


raw_dim_sfdc_account_eu_query = """
-- EU REGION QUERY
-- Note - Due to the fact that dim_sfdc_account data recides only in the US cluster,
-- we have slightly divergent queries between the EU and the US

WITH contract_renewal AS (
SELECT DISTINCT account_id, TRUE AS auto_renewal
FROM edw_delta_share.salesforce_sterling.sfdc_contract
WHERE auto_renewal = TRUE)
SELECT
  account.account_id,
  coalesce(account.ultimate_parent_id, account.account_id) as parent_account_id,
  account.account_name,
  account.sam_number AS sam_number_undecorated,
  account.created_date,
  CAST(account.last_activity_date AS DATE) AS last_activity_date,
  CAST(account.actual_deactivate_date AS DATE) AS actual_deactivate_date,
  account.last_modified_date,
  account.industry,
  account.sub_industry,
  account.cs_tier,
  account.csm_email as account_csm_email,
  enriched.account_size_segment,
  account.is_customer_delinquent AS delinquent,
  account.delinquency_status,
  account.delinquent_as_of,
  DATE_FORMAT(account.first_purchase_date, 'yyyy-MM-dd') AS first_purchase_date,
  account.billing_country,
  account.billing_state,
  COALESCE(contract_renewal.auto_renewal, FALSE) AS auto_renewal,
  '{DATEID}' AS date,
  CASE enriched.customer_arr_segment
    WHEN '<=0'
    THEN '<0'
    ELSE COALESCE(enriched.customer_arr_segment, 'unknown')
  END AS customer_arr_segment,
  enriched.ai_model_industry_archetype,
  enriched.ai_model_primary_industry,
  enriched.ai_model_secondary_industry
FROM edw_delta_share.silver.dim_customer{TIMETRAVEL_DATE} account
LEFT OUTER JOIN contract_renewal
    ON contract_renewal.account_id = account.account_id
LEFT OUTER JOIN edw_delta_share.silver.dim_account_enriched enriched
    ON account.sam_number = enriched.sam_number_undecorated
    AND account.account_id = enriched.account_id
"""

raw_dim_sfdc_account_ca_query = """
-- CA REGION QUERY
-- Note - Due to the fact that dim_sfdc_account data recides only in the US cluster,
-- we have slightly divergent queries between the CA and the US

WITH contract_renewal AS (
SELECT DISTINCT account_id, TRUE AS auto_renewal
FROM edw_delta_share.salesforce_sterling.sfdc_contract
WHERE auto_renewal = TRUE)
SELECT
  account.account_id,
  coalesce(account.ultimate_parent_id, account.account_id) as parent_account_id,
  account.account_name,
  account.sam_number AS sam_number_undecorated,
  account.created_date,
  CAST(account.last_activity_date AS DATE) AS last_activity_date,
  CAST(account.actual_deactivate_date AS DATE) AS actual_deactivate_date,
  account.last_modified_date,
  account.industry,
  account.sub_industry,
  account.cs_tier,
  account.csm_email as account_csm_email,
  enriched.account_size_segment,
  account.is_customer_delinquent AS delinquent,
  account.delinquency_status,
  account.delinquent_as_of,
  DATE_FORMAT(account.first_purchase_date, 'yyyy-MM-dd') AS first_purchase_date,
  account.billing_country,
  account.billing_state,
  COALESCE(contract_renewal.auto_renewal, FALSE) AS auto_renewal,
  '{DATEID}' AS date,
  CASE enriched.customer_arr_segment
    WHEN '<=0'
    THEN '<0'
    ELSE COALESCE(enriched.customer_arr_segment, 'unknown')
  END AS customer_arr_segment,
  enriched.ai_model_industry_archetype,
  enriched.ai_model_primary_industry,
  enriched.ai_model_secondary_industry
FROM edw_delta_share.silver.dim_customer{TIMETRAVEL_DATE} account
LEFT OUTER JOIN contract_renewal
    ON contract_renewal.account_id = account.account_id
LEFT OUTER JOIN edw_delta_share.silver.dim_account_enriched enriched
    ON account.sam_number = enriched.sam_number_undecorated
    AND account.account_id = enriched.account_id
"""

stg_sfdc_accounts_schema = [
    {
        "name": "org_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": org_id_default_description},
    },
    {
        "name": "account_id",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Unique ID of the account"},
    },
    {
        "name": "parent_account_id",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Unique ID of the account's parent. If there is no parent, this takes the value of the original account ID."
        },
    },
    {
        "name": "account_name",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Name of the account"},
    },
    {
        "name": "sam_number",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": sam_number_default_description},
    },
    {
        "name": "salesforce_sam_number",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "SAM number as represented in Salesforce for the given account"
        },
    },
    {
        "name": "parent_sam_number",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "SAM number for the associated parent_account_id (will have the same value as sam_number when there is no parent)"
        },
    },
    {
        "name": "account_created_date",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Date the account was created"
        },
    },
    {
        "name": "account_last_activity_date",
        "type": "date",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Last activity of the account"
        },
    },
    {
        "name": "account_actual_deactivate_date",
        "type": "date",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Date if the account was deactivated"
        },
    },
    {
        "name": "account_last_modified_date",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Date the account was last modified"
        },
    },
    {
        "name": "account_industry",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Industry classification of the account."
        },
    },
    {
        "name": "account_sub_industry",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Sub industry classification of the account."
        },
    },
    {
        "name": "account_cs_tier",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Account tier - Premier, Plus, Starter, Elite"
        },
    },
    {
        "name": "account_csm_email",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Email for the CSM of the org"},
    },
    {
        "name": "account_size_segment",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Classifies accounts by size - Small Business, Mid Market, Enterprise"
        },
    },
    {
        "name": "account_size_segment_name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Classifies accounts by size - Small Business, Mid Market, Enterprise"
        },
    },
    {
        "name": "account_customer_in_trial_mode",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.fct_orders. Boolean, classifies if customer is in an active free trial contract (uses Salesforce data)"
        },
    },
    {
        "name": "is_account_delinquent",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Boolean, signaling if the account is delinquent"
        },
    },
    {
        "name": "account_delinquency_status",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Provides additional information on delinquent accounts"
        },
    },
    {
        "name": "account_delinquent_as_of",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Data the account became delinquent"
        },
    },
    {
        "name": "account_first_purchase_date",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Date of initial account purchase"
        },
    },
    {
        "name": "account_billing_country",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Geographic information about the account"
        },
    },
    {
        "name": "account_billing_state",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Geographic information about the account"
        },
    },
    {
        "name": "account_auto_renewal",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Autorenewal status of SAM: TRUE if one contract associated with account has auto renewal, else FALSE."
        },
    },
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The field which partitions the table, in 'YYYY-mm-dd' format."
        },
    },
    {
        "name": "account_arr_segment",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": """ARR from the previously publicly reporting quarter in the buckets:
            <0
            0 - 100k
            100k - 500K,
            500K - 1M
            1M+

            Note - ARR information is calculated at the account level, not the SAM Number level
            An account and have multiple SAMs. Additionally a SAM can have multiple accounts, resulting in a single SAM having multiple account_arr_segments
            """
        },
    },
    {
        "name": "ai_model_industry_archetype",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "AI model-predicted industry archetype classification for the account"
        },
    },
    {
        "name": "ai_model_primary_industry",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "AI model-predicted primary industry classification for the account"
        },
    },
    {
        "name": "ai_model_secondary_industry",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "AI model-predicted secondary industry classification for the account"
        },
    },
]


stg_sfdc_accounts_us_query = """
--sql
WITH trials AS (
    SELECT DISTINCT
    sfdc_account_id,
    org_id
  FROM edw.silver.fct_orders
  LATERAL VIEW EXPLODE(org_id_list) AS org_id
  WHERE order_type = 'Free Trial Opportunity'
  AND DATE('{DATEID}') BETWEEN trial_start_date AND trial_end_date
)
SELECT
  raw_clouddb_org_sfdc_accounts.org_id,
  raw_clouddb_sfdc_accounts.sfdc_account_id AS account_id,
  parent.account_id as parent_account_id,
  account.account_name,
  raw_clouddb_sfdc_accounts.sam_number AS sam_number,
  account.sam_number_undecorated AS salesforce_sam_number,
  parent.sam_number_undecorated AS parent_sam_number,
  account.created_date AS account_created_date,
  account.last_activity_date AS account_last_activity_date,
  account.actual_deactivate_date AS account_actual_deactivate_date,
  account.last_modified_date AS account_last_modified_date,
  account.industry AS account_industry,
  account.sub_industry AS account_sub_industry,
  account.cs_tier AS account_cs_tier,
  account.account_csm_email,
  account.account_size_segment AS account_size_segment,
  CASE
    WHEN account.account_size_segment = 'ENT' THEN 'Enterprise'
    WHEN account.account_size_segment = 'AE1' THEN 'Small Business'
    WHEN account.account_size_segment = 'AE2/AE3' THEN 'Mid Market'
    ELSE account.account_size_segment
  END AS account_size_segment_name,
  CASE WHEN trials.org_id IS NOT NULL THEN TRUE ELSE FALSE END AS account_customer_in_trial_mode,
  account.delinquent AS is_account_delinquent,
  account.delinquency_status AS account_delinquency_status,
  account.delinquent_as_of AS account_delinquent_as_of,
  account.first_purchase_date AS account_first_purchase_date,
  account.billing_country AS account_billing_country,
  account.billing_state AS account_billing_state,
  account.auto_renewal AS account_auto_renewal,
  '{DATEID}' AS date,
  account.customer_arr_segment AS account_arr_segment,
  account.ai_model_industry_archetype,
  account.ai_model_primary_industry,
  account.ai_model_secondary_industry
FROM {database_bronze_dev}.raw_clouddb_sfdc_accounts
INNER JOIN {database_bronze_dev}.raw_clouddb_org_sfdc_accounts
ON raw_clouddb_org_sfdc_accounts.sfdc_account_id = raw_clouddb_sfdc_accounts.id
LEFT OUTER JOIN {database_bronze_dev}.raw_dim_sfdc_account account
ON (SUBSTRING(account.account_id, 1, 15) = SUBSTRING(raw_clouddb_sfdc_accounts.sfdc_account_id, 1, 15)
AND account.date = '{DATEID}')
LEFT OUTER JOIN {database_bronze_dev}.raw_dim_sfdc_account parent
ON parent.account_id = account.parent_account_id
AND parent.date = '{DATEID}'
LEFT OUTER JOIN trials
ON trials.org_id = raw_clouddb_org_sfdc_accounts.org_id
AND trials.sfdc_account_id = raw_clouddb_sfdc_accounts.sfdc_account_id
WHERE raw_clouddb_sfdc_accounts.date = '{DATEID}'
AND raw_clouddb_org_sfdc_accounts.date = '{DATEID}'
--endsql
"""

stg_sfdc_accounts_eu_query = """
--sql
WITH trials AS (
    SELECT DISTINCT
    sfdc_account_id,
    org_id
  FROM edw_delta_share.silver.fct_orders
  LATERAL VIEW EXPLODE(org_id_list) AS org_id
  WHERE order_type = 'Free Trial Opportunity'
  AND DATE('{DATEID}') BETWEEN trial_start_date AND trial_end_date
)
SELECT
  raw_clouddb_org_sfdc_accounts.org_id,
  raw_clouddb_sfdc_accounts.sfdc_account_id AS account_id,
  parent.account_id as parent_account_id,
  account.account_name,
  raw_clouddb_sfdc_accounts.sam_number AS sam_number,
  account.sam_number_undecorated AS salesforce_sam_number,
  parent.sam_number_undecorated AS parent_sam_number,
  account.created_date AS account_created_date,
  account.last_activity_date AS account_last_activity_date,
  account.actual_deactivate_date AS account_actual_deactivate_date,
  account.last_modified_date AS account_last_modified_date,
  account.industry AS account_industry,
  account.sub_industry AS account_sub_industry,
  account.cs_tier AS account_cs_tier,
  account.account_csm_email,
  account.account_size_segment AS account_size_segment,
  CASE
    WHEN account.account_size_segment = 'ENT' THEN 'Enterprise'
    WHEN account.account_size_segment = 'AE1' THEN 'Small Business'
    WHEN account.account_size_segment = 'AE2/AE3' THEN 'Mid Market'
    ELSE account.account_size_segment
  END AS account_size_segment_name,
  CASE WHEN trials.org_id IS NOT NULL THEN TRUE ELSE FALSE END AS account_customer_in_trial_mode,
  account.delinquent AS is_account_delinquent,
  account.delinquency_status AS account_delinquency_status,
  account.delinquent_as_of AS account_delinquent_as_of,
  account.first_purchase_date AS account_first_purchase_date,
  account.billing_country AS account_billing_country,
  account.billing_state AS account_billing_state,
  account.auto_renewal AS account_auto_renewal,
  '{DATEID}' AS date,
  account.customer_arr_segment AS account_arr_segment,
  account.ai_model_industry_archetype,
  account.ai_model_primary_industry,
  account.ai_model_secondary_industry
FROM {database_bronze_dev}.raw_clouddb_sfdc_accounts
INNER JOIN {database_bronze_dev}.raw_clouddb_org_sfdc_accounts
ON raw_clouddb_org_sfdc_accounts.sfdc_account_id = raw_clouddb_sfdc_accounts.id
LEFT OUTER JOIN {database_bronze_dev}.raw_dim_sfdc_account account
ON (SUBSTRING(account.account_id, 1, 15) = SUBSTRING(raw_clouddb_sfdc_accounts.sfdc_account_id, 1, 15)
AND account.date = '{DATEID}')
LEFT OUTER JOIN {database_bronze_dev}.raw_dim_sfdc_account parent
ON parent.account_id = account.parent_account_id
AND parent.date = '{DATEID}'
LEFT OUTER JOIN trials
ON trials.org_id = raw_clouddb_org_sfdc_accounts.org_id
AND trials.sfdc_account_id = raw_clouddb_sfdc_accounts.sfdc_account_id
WHERE raw_clouddb_sfdc_accounts.date = '{DATEID}'
AND raw_clouddb_org_sfdc_accounts.date = '{DATEID}'
--endsql
"""

stg_sfdc_accounts_ca_query = """
--sql
WITH trials AS (
    SELECT DISTINCT
    sfdc_account_id,
    org_id
  FROM edw_delta_share.silver.fct_orders
  LATERAL VIEW EXPLODE(org_id_list) AS org_id
  WHERE order_type = 'Free Trial Opportunity'
  AND DATE('{DATEID}') BETWEEN trial_start_date AND trial_end_date
)
SELECT
  raw_clouddb_org_sfdc_accounts.org_id,
  raw_clouddb_sfdc_accounts.sfdc_account_id AS account_id,
  parent.account_id as parent_account_id,
  account.account_name,
  raw_clouddb_sfdc_accounts.sam_number AS sam_number,
  account.sam_number_undecorated AS salesforce_sam_number,
  parent.sam_number_undecorated AS parent_sam_number,
  account.created_date AS account_created_date,
  account.last_activity_date AS account_last_activity_date,
  account.actual_deactivate_date AS account_actual_deactivate_date,
  account.last_modified_date AS account_last_modified_date,
  account.industry AS account_industry,
  account.sub_industry AS account_sub_industry,
  account.cs_tier AS account_cs_tier,
  account.account_csm_email,
  account.account_size_segment AS account_size_segment,
  CASE
    WHEN account.account_size_segment = 'ENT' THEN 'Enterprise'
    WHEN account.account_size_segment = 'AE1' THEN 'Small Business'
    WHEN account.account_size_segment = 'AE2/AE3' THEN 'Mid Market'
    ELSE account.account_size_segment
  END AS account_size_segment_name,
  CASE WHEN trials.org_id IS NOT NULL THEN TRUE ELSE FALSE END AS account_customer_in_trial_mode,
  account.delinquent AS is_account_delinquent,
  account.delinquency_status AS account_delinquency_status,
  account.delinquent_as_of AS account_delinquent_as_of,
  account.first_purchase_date AS account_first_purchase_date,
  account.billing_country AS account_billing_country,
  account.billing_state AS account_billing_state,
  account.auto_renewal AS account_auto_renewal,
  '{DATEID}' AS date,
  account.customer_arr_segment AS account_arr_segment,
  account.ai_model_industry_archetype,
  account.ai_model_primary_industry,
  account.ai_model_secondary_industry
FROM {database_bronze_dev}.raw_clouddb_sfdc_accounts
INNER JOIN {database_bronze_dev}.raw_clouddb_org_sfdc_accounts
ON raw_clouddb_org_sfdc_accounts.sfdc_account_id = raw_clouddb_sfdc_accounts.id
LEFT OUTER JOIN {database_bronze_dev}.raw_dim_sfdc_account account
ON (SUBSTRING(account.account_id, 1, 15) = SUBSTRING(raw_clouddb_sfdc_accounts.sfdc_account_id, 1, 15)
AND account.date = '{DATEID}')
LEFT OUTER JOIN {database_bronze_dev}.raw_dim_sfdc_account parent
ON parent.account_id = account.parent_account_id
AND parent.date = '{DATEID}'
LEFT OUTER JOIN trials
ON trials.org_id = raw_clouddb_org_sfdc_accounts.org_id
AND trials.sfdc_account_id = raw_clouddb_sfdc_accounts.sfdc_account_id
WHERE raw_clouddb_sfdc_accounts.date = '{DATEID}'
AND raw_clouddb_org_sfdc_accounts.date = '{DATEID}'
--endsql
"""

stg_organizations_schema = [
    {
        "name": "org_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": org_id_default_description},
    },
    {
        "name": "org_name",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Name of the organization"},
    },
    {
        "name": "created_at",
        "type": "timestamp",
        "nullable": True,
        "metadata": {"comment": "Date the organization was created"},
    },
    {
        "name": "locale",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Organization locale"},
    },
    {
        "name": "internal_type",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Represents if the organization is external or internal (Customer org, Internal Org, Lab Org, ...)"
        },
    },
    {
        "name": "internal_type_name",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Translation of the internal type"},
    },
    {
        "name": "quarantine_enabled",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "Indicates whether the account is in quarantine state or not."
        },
    },
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The field which partitions the table, in 'YYYY-mm-dd' format."
        },
    },
    {
        "name": "group_config_override",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Override for group info"},
    },
    {
        "name": "is_simulated_org",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Flag that represents is an org is a simulated org (e.g. Big Orgs). List of orgs is sourced from internaldb.simulated_orgs"
        },
    },
    {
        "name": "release_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Calculated from clouddb.organizations.release_type_enum. 0 = Phase 1, 1 = Phase 2, 2 = Early Adopter"
        },
    },
]


stg_organizations_query = """
--sql

WITH simulated_orgs AS (
SELECT DISTINCT org_id
 FROM internaldb.simulated_orgs
 WHERE org_id IS NOT NULL)
SELECT
  raw_clouddb_organizations.id AS org_id,
  raw_clouddb_organizations.name AS org_name,
  raw_clouddb_organizations.created_at,
  raw_clouddb_organizations.locale,
  CASE
    WHEN raw_clouddb_organizations.internal_type = 0 AND simulated_orgs.org_id IS NOT NULL
    THEN 1
    ELSE raw_clouddb_organizations.internal_type
  END AS internal_type,
  CASE
    WHEN raw_clouddb_organizations.internal_type = 0 AND simulated_orgs.org_id IS NULL THEN 'Customer Org'
    WHEN raw_clouddb_organizations.internal_type = 0 AND simulated_orgs.org_id IS NOT NULL THEN 'Internal Org'
    WHEN raw_clouddb_organizations.internal_type = 1 THEN 'Internal Org'
    WHEN raw_clouddb_organizations.internal_type = 2 THEN 'Lab Org'
    WHEN raw_clouddb_organizations.internal_type = 3 THEN 'Developer Portal Test Org'
    WHEN raw_clouddb_organizations.internal_type = 4 THEN 'Developer Portal Dev Org'
  END AS internal_type_name,
  CAST(raw_clouddb_organizations.quarantine_enabled AS INT) AS quarantine_enabled,
  '{DATEID}' AS date,
  raw_clouddb_groups.group_config_override,
  CASE
    WHEN simulated_orgs.org_id IS NOT NULL
    THEN True
    ELSE False
  END AS is_simulated_org,
  CASE
    WHEN release_type_enum = 0 THEN 'Phase 1'
    WHEN release_type_enum = 1 THEN 'Phase 2'
    WHEN release_type_enum = 2 THEN 'Early Adopter'
  END AS release_type
FROM
  {database_bronze_dev}.raw_clouddb_organizations
LEFT OUTER JOIN {database_bronze_dev}.raw_clouddb_groups
ON (raw_clouddb_organizations.id = raw_clouddb_groups.organization_id
AND raw_clouddb_groups.date = '{DATEID}')
LEFT OUTER JOIN simulated_orgs
ON (raw_clouddb_organizations.id = simulated_orgs.org_id)
WHERE raw_clouddb_organizations.date = '{DATEID}'
--endsql
"""

dim_organizations_schema = [
    {
        "name": "org_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": org_id_default_description},
    },
    {
        "name": "org_name",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Name of the organization"},
    },
    {
        "name": "created_at",
        "type": "timestamp",
        "nullable": True,
        "metadata": {"comment": "Date the organization was created"},
    },
    {
        "name": "locale",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Organization locale"},
    },
    {
        "name": "internal_type",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Represents if the organization is external or internal (Customer org, Internal Org, Lab Org, ...)"
        },
    },
    {
        "name": "internal_type_name",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Translation of the internal type"},
    },
    {
        "name": "quarantine_enabled",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "Indicates whether the account is in quarantine state or not."
        },
    },
    {
        "name": "account_id",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Unique ID of the account"},
    },
    {
        "name": "parent_account_id",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Unique ID of the account's parent. If there is no parent, this takes the value of the original account ID."
        },
    },
    {
        "name": "account_name",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Name of the account"},
    },
    {
        "name": "sam_number",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": sam_number_default_description},
    },
    {
        "name": "salesforce_sam_number",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "SAM number as represented in Salesforce for the given account"
        },
    },
    {
        "name": "parent_sam_number",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "SAM number for the associated parent_account_id (will have the same value as sam_number when there is no parent)"
        },
    },
    {
        "name": "account_created_date",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Date the account was created"
        },
    },
    {
        "name": "account_last_activity_date",
        "type": "date",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Last activity of the account"
        },
    },
    {
        "name": "account_actual_deactivate_date",
        "type": "date",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Date if the account was deactivated"
        },
    },
    {
        "name": "account_last_modified_date",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Date the account was last modified"
        },
    },
    {
        "name": "account_industry",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Industry classification of the account."
        },
    },
    {
        "name": "account_sub_industry",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Sub industry classification of the account."
        },
    },
    {
        "name": "account_cs_tier",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Account tier - Premier, Plus, Starter, Elite. This will be null if there's no match between the org/account tables."
        },
    },
    {
        "name": "account_csm_email",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Email for the CSM of the org"},
    },
    {
        "name": "account_size_segment",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Classifies accounts by size - Small Business, Mid Market, Enterprise"
        },
    },
    {
        "name": "account_size_segment_name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Classifies accounts by size - Small Business, Mid Market, Enterprise"
        },
    },
    {
        "name": "account_customer_in_trial_mode",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.fct_orders. Boolean, classifies if customer is in an active free trial contract (uses Salesforce data)"
        },
    },
    {
        "name": "is_account_delinquent",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Boolean, signaling if the account is delinquent"
        },
    },
    {
        "name": "account_delinquency_status",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Provides additional information on delinquent accounts"
        },
    },
    {
        "name": "account_delinquent_as_of",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Data the account became delinquent"
        },
    },
    {
        "name": "account_first_purchase_date",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Date of initial account purchase"
        },
    },
    {
        "name": "account_billing_country",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Geographic information about the account"
        },
    },
    {
        "name": "account_billing_state",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Geographic information about the account"
        },
    },
    {
        "name": "account_auto_renewal",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Autorenewal status of SAM: TRUE if at least one contract associated with account has auto renewal."
        },
    },
    {
        "name": "is_paid_customer",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": """TRUE if the following criteria are met:
                        (1) org_id has a non-zero count of non-expired and non-trial licenses in edw.silver.fct_license_orders for corresponding date
                        (2) org_id not in internaldb.simulated_orgs (queried in stg_organizations)
                        (3) internal_type of org_id is equal to 0
                        (4) org_id has a non null sam_number
                        (5) sam_number is not one of internally allocated accounts: 'C2N65AXKY7','CJEU6A8TFD','CR5MBC7RNY','C2NUBA3HFM','CNU9J2BYRW'

                        otherwise FALSE.
                        """
        },
    },
    {
        "name": "is_paid_telematics_customer",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": """TRUE if the following criteria are met:
                        (1) org_id has a non-zero count of non-expired and non-trial licenses in edw.silver.fct_license_orders for corresponding date
                        (2) at least one active license within telematics product family
                        (3) org_id not in internaldb.simulated_orgs (queried in stg_organizations)
                        (4) internal_type of org_id is equal to 0
                        (5) org_id has a non null sam_number
                        (6) sam_number is not one of internally allocated accounts: 'C2N65AXKY7','CJEU6A8TFD','CR5MBC7RNY','C2NUBA3HFM','CNU9J2BYRW'

                        otherwise FALSE.
                        """
        },
    },
    {
        "name": "is_paid_safety_customer",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": """TRUE if the following criteria are met:
                        (1) org_id has a non-zero count of non-expired and non-trial licenses in edw.silver.fct_license_orders for corresponding date
                        (2) at least one active license within safety product family
                        (3) org_id not in internaldb.simulated_orgs (queried in stg_organizations)
                        (4) internal_type of org_id is equal to 0
                        (5) org_id has a non null sam_number
                        (6) sam_number is not one of internally allocated accounts: 'C2N65AXKY7','CJEU6A8TFD','CR5MBC7RNY','C2NUBA3HFM','CNU9J2BYRW'

                        otherwise FALSE.
                        """
        },
    },
    {
        "name": "is_paid_stce_customer",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": """TRUE if the following criteria are met:
                        (1) org_id has a non-zero count of non-expired and non-trial licenses in edw.silver.fct_license_orders for corresponding date
                        (2) at least one active license within stce product family
                        (3) org_id not in internaldb.simulated_orgs (queried in stg_organizations)
                        (4) internal_type of org_id is equal to 0
                        (5) org_id has a non null sam_number
                        (6) sam_number is not one of internally allocated accounts: 'C2N65AXKY7','CJEU6A8TFD','CR5MBC7RNY','C2NUBA3HFM','CNU9J2BYRW'

                        otherwise FALSE.
                        """
        },
    },
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The field which partitions the table, in 'YYYY-mm-dd' format."
        },
    },
    {
        "name": "is_simulated_org",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Flag that represents is an org is a simulated org (e.g. Big Orgs). List of orgs is sourced from internaldb.simulated_orgs"
        },
    },
    {
        "name": "release_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Calculated from clouddb.organizations.release_type_enum. 0 = Phase 1, 1 = Phase 2, 2 = Early Adopter"
        },
    },
    {
        "name": "is_excluded_from_ml_model_training",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Flag that represents whether an org is excluded from ML model training or not"
        },
    },
    {
        "name": "account_arr_segment",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": """ARR from the previously publicly reporting quarter in the buckets:
            <0
            0 - 100k
            100k - 500K,
            500K - 1M
            1M+

            Note - ARR information is calculated at the account level, not the SAM Number level
            An account and have multiple SAMs. Additionally a SAM can have multiple accounts, resulting in a single SAM having multiple account_arr_segments
            """
        },
    },
    {
        "name": "account_ai_model_industry_archetype",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "AI model-predicted industry archetype classification for the account"
        },
    },
    {
        "name": "account_ai_model_primary_industry",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "AI model-predicted primary industry classification for the account"
        },
    },
    {
        "name": "account_ai_model_secondary_industry",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "AI model-predicted secondary industry classification for the account"
        },
    },
]


dim_organizations_query = """
--sql
WITH licenses AS (
 SELECT org_id,
        SUM(lo.product_net_qty) AS total_active_licenses,
        COALESCE(SUM(CASE WHEN LOWER(xref.product_family) = 'telematics' THEN lo.product_net_qty ELSE NULL END), 0) AS total_active_telematics_licenses,
        COALESCE(SUM(CASE WHEN LOWER(xref.product_family) = 'safety' THEN lo.product_net_qty ELSE NULL END), 0) AS total_active_safety_licenses,
        COALESCE(SUM(CASE WHEN LOWER(xref.product_family) ilike 'smart trailers%' THEN lo.product_net_qty ELSE NULL END), 0) AS total_active_stce_licenses
 FROM edw.silver.fct_license_orders lo
 JOIN edw.silver.license_hw_sku_xref xref
  ON lo.product_sku = xref.license_sku
 JOIN {database_silver_dev}.stg_sfdc_accounts org
  ON lo.sam_number = COALESCE(org.salesforce_sam_number, org.sam_number) -- Joining to salesforce_sam_number as it doesn't perfectly align with the SAM in our table
 WHERE COALESCE(product_net_qty, 0) > 0
 AND ((DATE(contract_end_date) >= DATE('{DATEID}') OR contract_end_date IS NULL) AND DATE(contract_start_date) <= DATE('{DATEID}'))
 AND is_trial = FALSE
 AND org.date = '{DATEID}'
 GROUP BY 1),
simulated_orgs AS (
 SELECT *
 FROM internaldb.simulated_orgs
 WHERE org_id IS NOT NULL)

SELECT
    stg_organizations.org_id,
    stg_organizations.org_name,
    stg_organizations.created_at,
    stg_organizations.locale,
    stg_organizations.internal_type,
    stg_organizations.internal_type_name,
    stg_organizations.quarantine_enabled,
    stg_sfdc_accounts.account_id,
    stg_sfdc_accounts.parent_account_id,
    stg_sfdc_accounts.account_name,
    stg_sfdc_accounts.sam_number,
    stg_sfdc_accounts.salesforce_sam_number,
    stg_sfdc_accounts.parent_sam_number,
    stg_sfdc_accounts.account_created_date,
    stg_sfdc_accounts.account_last_activity_date,
    stg_sfdc_accounts.account_actual_deactivate_date,
    stg_sfdc_accounts.account_last_modified_date,
    stg_sfdc_accounts.account_industry,
    stg_sfdc_accounts.account_sub_industry,
    stg_sfdc_accounts.account_cs_tier,
    stg_sfdc_accounts.account_csm_email,
    stg_sfdc_accounts.account_size_segment,
    stg_sfdc_accounts.account_size_segment_name,
    stg_sfdc_accounts.account_customer_in_trial_mode,
    stg_sfdc_accounts.is_account_delinquent,
    stg_sfdc_accounts.account_delinquency_status,
    stg_sfdc_accounts.account_delinquent_as_of,
    stg_sfdc_accounts.account_first_purchase_date,
    stg_sfdc_accounts.account_billing_country,
    stg_sfdc_accounts.account_billing_state,
    stg_sfdc_accounts.account_auto_renewal,
    CASE WHEN licenses.total_active_licenses > 0
         AND stg_organizations.internal_type = 0
         AND stg_sfdc_accounts.sam_number IS NOT NULL
         AND stg_sfdc_accounts.sam_number NOT IN ('C2N65AXKY7','CJEU6A8TFD','CR5MBC7RNY','C2NUBA3HFM','CNU9J2BYRW')
         AND NOT stg_organizations.is_simulated_org
         THEN TRUE ELSE FALSE END AS is_paid_customer,
    CASE WHEN licenses.total_active_safety_licenses > 0
         AND stg_organizations.internal_type = 0
         AND stg_sfdc_accounts.sam_number IS NOT NULL
         AND stg_sfdc_accounts.sam_number NOT IN ('C2N65AXKY7','CJEU6A8TFD','CR5MBC7RNY','C2NUBA3HFM','CNU9J2BYRW')
         AND NOT stg_organizations.is_simulated_org
         THEN TRUE ELSE FALSE END AS is_paid_safety_customer,
    CASE WHEN licenses.total_active_stce_licenses> 0
         AND stg_organizations.internal_type = 0
         AND stg_sfdc_accounts.sam_number IS NOT NULL
         AND stg_sfdc_accounts.sam_number NOT IN ('C2N65AXKY7','CJEU6A8TFD','CR5MBC7RNY','C2NUBA3HFM','CNU9J2BYRW')
         AND NOT stg_organizations.is_simulated_org
         THEN TRUE ELSE FALSE END AS is_paid_stce_customer,
    CASE WHEN licenses.total_active_telematics_licenses > 0
         AND stg_organizations.internal_type = 0
         AND stg_sfdc_accounts.sam_number IS NOT NULL
         AND stg_sfdc_accounts.sam_number NOT IN ('C2N65AXKY7','CJEU6A8TFD','CR5MBC7RNY','C2NUBA3HFM','CNU9J2BYRW')
         AND NOT stg_organizations.is_simulated_org
         THEN TRUE ELSE FALSE END AS is_paid_telematics_customer,
    '{DATEID}' AS date,
    stg_organizations.is_simulated_org,
    stg_organizations.release_type,
    CASE WHEN bl.org_id IS NOT NULL AND COALESCE(bl.date_added, '2000-01-01') <= '{DATEID}' THEN TRUE ELSE FALSE END AS is_excluded_from_ml_model_training,
    COALESCE(stg_sfdc_accounts.account_arr_segment, 'unknown') account_arr_segment,
    stg_sfdc_accounts.ai_model_industry_archetype as account_ai_model_industry_archetype,
    stg_sfdc_accounts.ai_model_primary_industry as account_ai_model_primary_industry,
    stg_sfdc_accounts.ai_model_secondary_industry as account_ai_model_secondary_industry
FROM {database_silver_dev}.stg_organizations
LEFT OUTER JOIN {database_silver_dev}.stg_sfdc_accounts
ON (stg_organizations.org_id = stg_sfdc_accounts.org_id
AND stg_sfdc_accounts.date = '{DATEID}')
LEFT OUTER JOIN licenses
ON licenses.org_id = stg_organizations.org_id
LEFT OUTER JOIN (SELECT DISTINCT org_id, date_added FROM dojo.ml_blocklisted_org_ids) bl
ON bl.org_id = stg_organizations.org_id
WHERE stg_organizations.date = '{DATEID}'
--endsql
"""

dim_organizations_eu_query = """
--sql
WITH licenses AS (
 SELECT org_id,
        SUM(lo.product_net_qty) AS total_active_licenses,
        COALESCE(SUM(CASE WHEN LOWER(xref.product_family) = 'telematics' THEN lo.product_net_qty ELSE NULL END), 0) AS total_active_telematics_licenses,
        COALESCE(SUM(CASE WHEN LOWER(xref.product_family) = 'safety' THEN lo.product_net_qty ELSE NULL END), 0) AS total_active_safety_licenses,
        COALESCE(SUM(CASE WHEN LOWER(xref.product_family) ilike 'smart trailers%' THEN lo.product_net_qty ELSE NULL END), 0) AS total_active_stce_licenses
 FROM edw_delta_share.silver.fct_license_orders lo
 JOIN edw_delta_share.silver.license_hw_sku_xref xref
  ON lo.product_sku = xref.license_sku
 JOIN {database_silver_dev}.stg_sfdc_accounts org -- Use Salesforce SAM number to join as it doesn't perfectly align with the SAM in our table
  ON lo.sam_number = COALESCE(org.salesforce_sam_number, org.sam_number)
 WHERE COALESCE(product_net_qty, 0) > 0
 AND ((DATE(contract_end_date) >= DATE('{DATEID}') OR contract_end_date IS NULL) AND DATE(contract_start_date) <= DATE('{DATEID}'))
 AND is_trial = FALSE
 AND org.date = '{DATEID}'
 GROUP BY 1),
simulated_orgs AS (
 SELECT *
 FROM internaldb.simulated_orgs
 WHERE org_id IS NOT NULL)

SELECT
    stg_organizations.org_id,
    stg_organizations.org_name,
    stg_organizations.created_at,
    stg_organizations.locale,
    stg_organizations.internal_type,
    stg_organizations.internal_type_name,
    stg_organizations.quarantine_enabled,
    stg_sfdc_accounts.account_id,
    stg_sfdc_accounts.parent_account_id,
    stg_sfdc_accounts.account_name,
    stg_sfdc_accounts.sam_number,
    stg_sfdc_accounts.salesforce_sam_number,
    stg_sfdc_accounts.parent_sam_number,
    stg_sfdc_accounts.account_created_date,
    stg_sfdc_accounts.account_last_activity_date,
    stg_sfdc_accounts.account_actual_deactivate_date,
    stg_sfdc_accounts.account_last_modified_date,
    stg_sfdc_accounts.account_industry,
    stg_sfdc_accounts.account_sub_industry,
    stg_sfdc_accounts.account_cs_tier,
    stg_sfdc_accounts.account_csm_email,
    stg_sfdc_accounts.account_size_segment,
    stg_sfdc_accounts.account_size_segment_name,
    stg_sfdc_accounts.account_customer_in_trial_mode,
    stg_sfdc_accounts.is_account_delinquent,
    stg_sfdc_accounts.account_delinquency_status,
    stg_sfdc_accounts.account_delinquent_as_of,
    stg_sfdc_accounts.account_first_purchase_date,
    stg_sfdc_accounts.account_billing_country,
    stg_sfdc_accounts.account_billing_state,
    stg_sfdc_accounts.account_auto_renewal,
    CASE WHEN licenses.total_active_licenses > 0
         AND stg_organizations.internal_type = 0
         AND stg_sfdc_accounts.sam_number IS NOT NULL
         AND stg_sfdc_accounts.sam_number NOT IN ('C2N65AXKY7','CJEU6A8TFD','CR5MBC7RNY','C2NUBA3HFM','CNU9J2BYRW')
         AND NOT stg_organizations.is_simulated_org
         THEN TRUE ELSE FALSE END AS is_paid_customer,
    CASE WHEN licenses.total_active_safety_licenses > 0
         AND stg_organizations.internal_type = 0
         AND stg_sfdc_accounts.sam_number IS NOT NULL
         AND stg_sfdc_accounts.sam_number NOT IN ('C2N65AXKY7','CJEU6A8TFD','CR5MBC7RNY','C2NUBA3HFM','CNU9J2BYRW')
         AND NOT stg_organizations.is_simulated_org
         THEN TRUE ELSE FALSE END AS is_paid_safety_customer,
    CASE WHEN licenses.total_active_stce_licenses> 0
         AND stg_organizations.internal_type = 0
         AND stg_sfdc_accounts.sam_number IS NOT NULL
         AND stg_sfdc_accounts.sam_number NOT IN ('C2N65AXKY7','CJEU6A8TFD','CR5MBC7RNY','C2NUBA3HFM','CNU9J2BYRW')
         AND NOT stg_organizations.is_simulated_org
         THEN TRUE ELSE FALSE END AS is_paid_stce_customer,
    CASE WHEN licenses.total_active_telematics_licenses > 0
         AND stg_organizations.internal_type = 0
         AND stg_sfdc_accounts.sam_number IS NOT NULL
         AND stg_sfdc_accounts.sam_number NOT IN ('C2N65AXKY7','CJEU6A8TFD','CR5MBC7RNY','C2NUBA3HFM','CNU9J2BYRW')
         AND NOT stg_organizations.is_simulated_org
         THEN TRUE ELSE FALSE END AS is_paid_telematics_customer,
    '{DATEID}' AS date,
    stg_organizations.is_simulated_org,
    stg_organizations.release_type,
    CASE WHEN bl.org_id IS NOT NULL AND COALESCE(bl.date_added, '2000-01-01') <= '{DATEID}' THEN TRUE ELSE FALSE END AS is_excluded_from_ml_model_training,
    COALESCE(stg_sfdc_accounts.account_arr_segment, 'unknown') account_arr_segment,
    stg_sfdc_accounts.ai_model_industry_archetype as account_ai_model_industry_archetype,
    stg_sfdc_accounts.ai_model_primary_industry as account_ai_model_primary_industry,
    stg_sfdc_accounts.ai_model_secondary_industry as account_ai_model_secondary_industry
FROM {database_silver_dev}.stg_organizations
LEFT OUTER JOIN {database_silver_dev}.stg_sfdc_accounts
ON (stg_organizations.org_id = stg_sfdc_accounts.org_id
AND stg_sfdc_accounts.date = '{DATEID}')
LEFT OUTER JOIN licenses
ON licenses.org_id = stg_organizations.org_id
LEFT OUTER JOIN (SELECT DISTINCT org_id, date_added FROM dojo.ml_blocklisted_org_ids) bl
ON bl.org_id = stg_organizations.org_id
WHERE stg_organizations.date = '{DATEID}'
--endsql
"""

dim_organizations_ca_query = """
--sql
WITH licenses AS (
 SELECT org_id,
        SUM(lo.product_net_qty) AS total_active_licenses,
        COALESCE(SUM(CASE WHEN LOWER(xref.product_family) = 'telematics' THEN lo.product_net_qty ELSE NULL END), 0) AS total_active_telematics_licenses,
        COALESCE(SUM(CASE WHEN LOWER(xref.product_family) = 'safety' THEN lo.product_net_qty ELSE NULL END), 0) AS total_active_safety_licenses,
        COALESCE(SUM(CASE WHEN LOWER(xref.product_family) ilike 'smart trailers%' THEN lo.product_net_qty ELSE NULL END), 0) AS total_active_stce_licenses
 FROM edw_delta_share.silver.fct_license_orders lo
 JOIN edw_delta_share.silver.license_hw_sku_xref xref
  ON lo.product_sku = xref.license_sku
 JOIN {database_silver_dev}.stg_sfdc_accounts org -- Use Salesforce SAM number to join as it doesn't perfectly align with the SAM in our table
  ON lo.sam_number = COALESCE(org.salesforce_sam_number, org.sam_number)
 WHERE COALESCE(product_net_qty, 0) > 0
 AND ((DATE(contract_end_date) >= DATE('{DATEID}') OR contract_end_date IS NULL) AND DATE(contract_start_date) <= DATE('{DATEID}'))
 AND is_trial = FALSE
 AND org.date = '{DATEID}'
 GROUP BY 1),
simulated_orgs AS (
 SELECT *
 FROM internaldb.simulated_orgs
 WHERE org_id IS NOT NULL)

SELECT
    stg_organizations.org_id,
    stg_organizations.org_name,
    stg_organizations.created_at,
    stg_organizations.locale,
    stg_organizations.internal_type,
    stg_organizations.internal_type_name,
    stg_organizations.quarantine_enabled,
    stg_sfdc_accounts.account_id,
    stg_sfdc_accounts.parent_account_id,
    stg_sfdc_accounts.account_name,
    stg_sfdc_accounts.sam_number,
    stg_sfdc_accounts.salesforce_sam_number,
    stg_sfdc_accounts.parent_sam_number,
    stg_sfdc_accounts.account_created_date,
    stg_sfdc_accounts.account_last_activity_date,
    stg_sfdc_accounts.account_actual_deactivate_date,
    stg_sfdc_accounts.account_last_modified_date,
    stg_sfdc_accounts.account_industry,
    stg_sfdc_accounts.account_sub_industry,
    stg_sfdc_accounts.account_cs_tier,
    stg_sfdc_accounts.account_csm_email,
    stg_sfdc_accounts.account_size_segment,
    stg_sfdc_accounts.account_size_segment_name,
    stg_sfdc_accounts.account_customer_in_trial_mode,
    stg_sfdc_accounts.is_account_delinquent,
    stg_sfdc_accounts.account_delinquency_status,
    stg_sfdc_accounts.account_delinquent_as_of,
    stg_sfdc_accounts.account_first_purchase_date,
    stg_sfdc_accounts.account_billing_country,
    stg_sfdc_accounts.account_billing_state,
    stg_sfdc_accounts.account_auto_renewal,
    CASE WHEN licenses.total_active_licenses > 0
         AND stg_organizations.internal_type = 0
         AND stg_sfdc_accounts.sam_number IS NOT NULL
         AND stg_sfdc_accounts.sam_number NOT IN ('C2N65AXKY7','CJEU6A8TFD','CR5MBC7RNY','C2NUBA3HFM','CNU9J2BYRW')
         AND NOT stg_organizations.is_simulated_org
         THEN TRUE ELSE FALSE END AS is_paid_customer,
    CASE WHEN licenses.total_active_safety_licenses > 0
         AND stg_organizations.internal_type = 0
         AND stg_sfdc_accounts.sam_number IS NOT NULL
         AND stg_sfdc_accounts.sam_number NOT IN ('C2N65AXKY7','CJEU6A8TFD','CR5MBC7RNY','C2NUBA3HFM','CNU9J2BYRW')
         AND NOT stg_organizations.is_simulated_org
         THEN TRUE ELSE FALSE END AS is_paid_safety_customer,
    CASE WHEN licenses.total_active_stce_licenses> 0
         AND stg_organizations.internal_type = 0
         AND stg_sfdc_accounts.sam_number IS NOT NULL
         AND stg_sfdc_accounts.sam_number NOT IN ('C2N65AXKY7','CJEU6A8TFD','CR5MBC7RNY','C2NUBA3HFM','CNU9J2BYRW')
         AND NOT stg_organizations.is_simulated_org
         THEN TRUE ELSE FALSE END AS is_paid_stce_customer,
    CASE WHEN licenses.total_active_telematics_licenses > 0
         AND stg_organizations.internal_type = 0
         AND stg_sfdc_accounts.sam_number IS NOT NULL
         AND stg_sfdc_accounts.sam_number NOT IN ('C2N65AXKY7','CJEU6A8TFD','CR5MBC7RNY','C2NUBA3HFM','CNU9J2BYRW')
         AND NOT stg_organizations.is_simulated_org
         THEN TRUE ELSE FALSE END AS is_paid_telematics_customer,
    '{DATEID}' AS date,
    stg_organizations.is_simulated_org,
    stg_organizations.release_type,
    -- CASE WHEN bl.org_id IS NOT NULL AND COALESCE(bl.date_added, '2000-01-01') <= '{DATEID}' THEN TRUE ELSE FALSE END AS is_excluded_from_ml_model_training,
    FALSE AS is_excluded_from_ml_model_training,
    COALESCE(stg_sfdc_accounts.account_arr_segment, 'unknown') account_arr_segment,
    stg_sfdc_accounts.ai_model_industry_archetype as account_ai_model_industry_archetype,
    stg_sfdc_accounts.ai_model_primary_industry as account_ai_model_primary_industry,
    stg_sfdc_accounts.ai_model_secondary_industry as account_ai_model_secondary_industry
FROM {database_silver_dev}.stg_organizations
LEFT OUTER JOIN {database_silver_dev}.stg_sfdc_accounts
ON (stg_organizations.org_id = stg_sfdc_accounts.org_id
AND stg_sfdc_accounts.date = '{DATEID}')
LEFT OUTER JOIN licenses
ON licenses.org_id = stg_organizations.org_id
-- TODO (add back in after this table is added in CA)
-- LEFT OUTER JOIN (SELECT DISTINCT org_id, date_added FROM dojo.ml_blocklisted_org_ids) bl
-- ON bl.org_id = stg_organizations.org_id
WHERE stg_organizations.date = '{DATEID}'
--endsql
"""


assets = build_assets_from_sql(
    name="raw_clouddb_organizations",
    schema=None,
    sql_query=raw_clouddb_organizations_query,
    description="""The most recent org data""",
    primary_keys=[],
    upstreams=[],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_bronze_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
raw_clouddb_organizations_1 = assets[AWSRegions.US_WEST_2.value]
raw_clouddb_organizations_2 = assets[AWSRegions.EU_WEST_1.value]
raw_clouddb_organizations_3 = assets[AWSRegions.CA_CENTRAL_1.value]


assets = build_assets_from_sql(
    name="raw_clouddb_groups",
    schema=None,
    sql_query=raw_clouddb_groups_query,
    description="""The most recent groups data""",
    primary_keys=[],
    upstreams=[],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_bronze_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
raw_clouddb_groups_1 = assets[AWSRegions.US_WEST_2.value]
raw_clouddb_groups_2 = assets[AWSRegions.EU_WEST_1.value]
raw_clouddb_groups_3 = assets[AWSRegions.CA_CENTRAL_1.value]


assets = build_assets_from_sql(
    name="raw_clouddb_sfdc_accounts",
    schema=None,
    sql_query=raw_clouddb_sfdc_accounts_query,
    description="""The most recent accounts data""",
    primary_keys=[],
    upstreams=[],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_bronze_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
raw_clouddb_sfdc_accounts_1 = assets[AWSRegions.US_WEST_2.value]
raw_clouddb_sfdc_accounts_2 = assets[AWSRegions.EU_WEST_1.value]
raw_clouddb_sfdc_accounts_3 = assets[AWSRegions.CA_CENTRAL_1.value]


assets = build_assets_from_sql(
    name="raw_clouddb_org_sfdc_accounts",
    schema=None,
    sql_query=raw_clouddb_org_sfdc_accounts_query,
    description="""The most recent org SFDC accounts data""",
    primary_keys=[],
    upstreams=[],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_bronze_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
raw_clouddb_org_sfdc_accounts_1 = assets[AWSRegions.US_WEST_2.value]
raw_clouddb_org_sfdc_accounts_2 = assets[AWSRegions.EU_WEST_1.value]
raw_clouddb_org_sfdc_accounts_3 = assets[AWSRegions.CA_CENTRAL_1.value]


assets = build_assets_from_sql(
    name="raw_dim_sfdc_account",
    schema=raw_dim_sfdc_account_schema,
    sql_query=raw_dim_sfdc_account_us_query,
    description="""Grabs the latest SFDC account data""",
    primary_keys=["date", "account_id"],
    upstreams=[],
    regions=[AWSRegions.US_WEST_2.value],
    group_name=pipeline_group_name,
    database=databases["database_bronze_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
raw_dim_sfdc_account_1 = assets[AWSRegions.US_WEST_2.value]

assets = build_assets_from_sql(
    name="raw_dim_sfdc_account",
    schema=raw_dim_sfdc_account_schema,
    sql_query=raw_dim_sfdc_account_eu_query,
    description="""Grabs the latest SFDC account data""",
    primary_keys=["date", "account_id"],
    upstreams=[],
    regions=[AWSRegions.EU_WEST_1.value],
    group_name=pipeline_group_name,
    database=databases["database_bronze_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
raw_dim_sfdc_account_2 = assets[AWSRegions.EU_WEST_1.value]

assets = build_assets_from_sql(
    name="raw_dim_sfdc_account",
    schema=raw_dim_sfdc_account_schema,
    sql_query=raw_dim_sfdc_account_ca_query,
    description="""Grabs the latest SFDC account data""",
    primary_keys=["date", "account_id"],
    upstreams=[],
    regions=[AWSRegions.CA_CENTRAL_1.value],
    group_name=pipeline_group_name,
    database=databases["database_bronze_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
raw_dim_sfdc_account_3 = assets[AWSRegions.CA_CENTRAL_1.value]


assets = build_assets_from_sql(
    name="stg_sfdc_accounts",
    schema=stg_sfdc_accounts_schema,
    sql_query=stg_sfdc_accounts_us_query,
    primary_keys=["date", "org_id"],
    upstreams=[
        AssetKey([databases["database_bronze_dev"], "raw_dim_sfdc_account"]),
        AssetKey([databases["database_bronze_dev"], "raw_clouddb_sfdc_accounts"]),
        AssetKey([databases["database_bronze_dev"], "raw_clouddb_org_sfdc_accounts"]),
    ],
    regions=[AWSRegions.US_WEST_2.value],
    group_name=pipeline_group_name,
    database=databases["database_silver_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
stg_sfdc_accounts_1 = assets[AWSRegions.US_WEST_2.value]

assets = build_assets_from_sql(
    name="stg_sfdc_accounts",
    schema=stg_sfdc_accounts_schema,
    sql_query=stg_sfdc_accounts_eu_query,
    primary_keys=["date", "org_id"],
    upstreams=[
        AssetKey([databases["database_bronze_dev"], "raw_dim_sfdc_account"]),
        AssetKey([databases["database_bronze_dev"], "raw_clouddb_sfdc_accounts"]),
        AssetKey([databases["database_bronze_dev"], "raw_clouddb_org_sfdc_accounts"]),
    ],
    regions=[AWSRegions.EU_WEST_1.value],
    group_name=pipeline_group_name,
    database=databases["database_silver_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
stg_sfdc_accounts_2 = assets[AWSRegions.EU_WEST_1.value]

assets = build_assets_from_sql(
    name="stg_sfdc_accounts",
    schema=stg_sfdc_accounts_schema,
    sql_query=stg_sfdc_accounts_ca_query,
    primary_keys=["date", "org_id"],
    upstreams=[
        AssetKey([databases["database_bronze_dev"], "raw_dim_sfdc_account"]),
        AssetKey([databases["database_bronze_dev"], "raw_clouddb_sfdc_accounts"]),
        AssetKey([databases["database_bronze_dev"], "raw_clouddb_org_sfdc_accounts"]),
    ],
    regions=[AWSRegions.CA_CENTRAL_1.value],
    group_name=pipeline_group_name,
    database=databases["database_silver_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
stg_sfdc_accounts_3 = assets[AWSRegions.CA_CENTRAL_1.value]


dqs["stg_sfdc_accounts"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_stg_sfdc_accounts",
        table="stg_sfdc_accounts",
        primary_keys=["date", "org_id"],
        blocking=True,
        database=databases["database_silver_dev"],
    )
)

dqs["stg_sfdc_accounts"].append(
    NonEmptyDQCheck(
        name="dq_empty_stg_sfdc_accounts",
        table="stg_sfdc_accounts",
        blocking=True,
        database=databases["database_silver_dev"],
    )
)


assets = build_assets_from_sql(
    name="stg_organizations",
    schema=stg_organizations_schema,
    sql_query=stg_organizations_query,
    primary_keys=["date", "org_id"],
    upstreams=[
        AssetKey([databases["database_bronze_dev"], "raw_clouddb_organizations"]),
        AssetKey([databases["database_bronze_dev"], "raw_clouddb_groups"]),
    ],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_silver_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
stg_organizations_1 = assets[AWSRegions.US_WEST_2.value]
stg_organizations_2 = assets[AWSRegions.EU_WEST_1.value]
stg_organizations_3 = assets[AWSRegions.CA_CENTRAL_1.value]


dqs["stg_organizations"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_stg_organizations",
        table="stg_organizations",
        primary_keys=["date", "org_id"],
        blocking=True,
        database=databases["database_silver_dev"],
    )
)

dqs["stg_organizations"].append(
    NonEmptyDQCheck(
        name="dq_empty_stg_organizations",
        table="stg_organizations",
        blocking=True,
        database=databases["database_silver_dev"],
    )
)


assets = build_assets_from_sql(
    name="dim_organizations",
    description=build_table_description(
        table_desc="""This table provides a daily snapshot of organizations.
In Samsara, a customer, represented by a SAM number, can consist of one or numerous orgs.
The vast majority of activities and actions taken by customers are done at the organizational level (e.g., a trip or safety event is associated with an organization rather than a SAM number).
Additionally, a device is mapped to an organization, not a SAM number.

This table enriches the organization data with additional metadata about the customer (SAM Number). The customer data points have an "account" prefix and are sourced from Biztech Salesforce tables.""",
        row_meaning="""A unique organization as recorded on a specific date.
This allows you to track and analyze organizations over time.
If you only care about a single date,
filter for the org on that date (or the latest date for the most up to date information)""",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by="9am PST",
    ),
    schema=dim_organizations_schema,
    sql_query=dim_organizations_query,
    primary_keys=["date", "org_id"],
    upstreams=[
        AssetKey([databases["database_silver_dev"], "dq_stg_sfdc_accounts"]),
        AssetKey([databases["database_silver_dev"], "dq_stg_organizations"]),
        AssetKey(["dojo", "ml_blocklisted_org_ids"]),
    ],
    regions=[AWSRegions.US_WEST_2.value],
    group_name=pipeline_group_name,
    database=databases["database_gold_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
dim_organizations_1 = assets[AWSRegions.US_WEST_2.value]

assets = build_assets_from_sql(
    name="dim_organizations",
    description=build_table_description(
        table_desc="""This table provides a daily snapshot of organizations.
In Samsara, a customer, represented by a SAM number, can consist of one or numerous orgs.
The vast majority of activities and actions taken by customers are done at the organizational level (e.g., a trip or safety event is associated with an organization rather than a SAM number).
Additionally, a device is mapped to an organization, not a SAM number.

This table enriches the organization data with additional metadata about the customer (SAM Number). The customer data points have an "account" prefix and are sourced from Biztech Salesforce tables.""",
        row_meaning="""A unique organization as recorded on a specific date.
This allows you to track and analyze organizations over time.
If you only care about a single date,
filter for the org on that date (or the latest date for the most up to date information)""",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by="9am PST",
    ),
    schema=dim_organizations_schema,
    sql_query=dim_organizations_eu_query,
    primary_keys=["date", "org_id"],
    upstreams=[
        AssetKey([databases["database_silver_dev"], "dq_stg_sfdc_accounts"]),
        AssetKey([databases["database_silver_dev"], "dq_stg_organizations"]),
    ],
    regions=[AWSRegions.EU_WEST_1.value],
    group_name=pipeline_group_name,
    database=databases["database_gold_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
dim_organizations_2 = assets[AWSRegions.EU_WEST_1.value]

assets = build_assets_from_sql(
    name="dim_organizations",
    description=build_table_description(
        table_desc="""This table provides a daily snapshot of organizations.
In Samsara, a customer, represented by a SAM number, can consist of one or numerous orgs.
The vast majority of activities and actions taken by customers are done at the organizational level (e.g., a trip or safety event is associated with an organization rather than a SAM number).
Additionally, a device is mapped to an organization, not a SAM number.

This table enriches the organization data with additional metadata about the customer (SAM Number). The customer data points have an "account" prefix and are sourced from Biztech Salesforce tables.""",
        row_meaning="""A unique organization as recorded on a specific date.
This allows you to track and analyze organizations over time.
If you only care about a single date,
filter for the org on that date (or the latest date for the most up to date information)""",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by="9am PST",
    ),
    schema=dim_organizations_schema,
    sql_query=dim_organizations_ca_query,
    primary_keys=["date", "org_id"],
    upstreams=[
        AssetKey([databases["database_silver_dev"], "dq_stg_sfdc_accounts"]),
        AssetKey([databases["database_silver_dev"], "dq_stg_organizations"]),
    ],
    regions=[AWSRegions.CA_CENTRAL_1.value],
    group_name=pipeline_group_name,
    database=databases["database_gold_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
dim_organizations_3 = assets[AWSRegions.CA_CENTRAL_1.value]

dqs["dim_organizations"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_dim_organizations",
        table="dim_organizations",
        primary_keys=["date", "org_id"],
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["dim_organizations"].append(
    NonEmptyDQCheck(
        name="dq_empty_dim_organizations",
        table="dim_organizations",
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["dim_organizations"].append(
    TrendDQCheck(
        name="dim_organizations_check_day_over_day_row_count",
        database=databases["database_gold_dev"],
        table="dim_organizations",
        tolerance=0.04,
        blocking=True,
    )
)

dqs["dim_organizations"].append(
    SQLDQCheck(
        name="check_is_paid_customer_trend",
        database=databases["database_gold_dev"],
        sql_query=f'SELECT ROUND(COUNT(DISTINCT CASE WHEN is_paid_customer = TRUE THEN sam_number END) * 1.0 / COUNT(DISTINCT sam_number), 2) AS observed_value FROM {databases["database_gold_dev"]}.dim_organizations WHERE internal_type = 0',
        expected_value=0.40,
        operator=Operator.gte,
        blocking=False,
    )
)

dqs["dim_organizations"].append(
    SQLDQCheck(
        name="check_is_paid_customer",
        database=databases["database_gold_dev"],
        sql_query=f'select count(distinct is_paid_customer) as observed_value FROM {databases["database_gold_dev"]}.dim_organizations',
        expected_value=2,
        blocking=True,
    )
)


dq_assets = dqs.generate()
