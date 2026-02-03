from dagster import AssetExecutionContext
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    AWSRegion,
    ColumnDescription,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    get_all_regions,
    partition_key_ranges_from_context,
)

PRIMARY_KEYS = ["date", "org_id"]

SCHEMA =  [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": ColumnDescription.DATE},
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": ColumnDescription.ORG_ID},
    },
    {
        "name": "sam_number",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": ColumnDescription.SAM_NUMBER},
    },
    {
        "name": "account_id",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "ID of account"},
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
        "name": "account_size_segment_name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Classifies accounts by size - Small Business, Mid Market, Enterprise"
        },
    },
    {
        "name": "release_type",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Calculated from clouddb.organizations.release_type_enum. 0 = Phase 1, 1 = Phase 2, 2 = Early Adopter"},
    },
    {
        "name": "org_category",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Classification of org. One of Internal Orgs, Expansion, Customer, First Purchase Prospect, Failed Trial."},
    },
]

QUERY = """
WITH orgs AS (
    SELECT
        date,
        org_id,
        sam_number,
        salesforce_sam_number,
        COALESCE(account_id, 'No Account ID') AS account_id,
        account_arr_segment,
        account_size_segment_name,
        internal_type,
        release_type
    FROM datamodel_core.dim_organizations
    WHERE
        date = '{PARTITION_START}'
        AND org_name NOT RLIKE '(?i)(\\bold\\b|delete|sandbox|\\btest\\b|\\bdemo\\b|\\bdemonstration\\b|\\bduplicate\\b|\\buat\\b|not in use|do not use|decommissioned|\\bobsolete\\b|\\bvoid\\b|\\binvalid\\b|N\\\\/A|DNU|\\bremoved\\b|\\bretired\\b|wrong dashboard|not active|not using|newly created|\\\\(empty\\\\)|cancelled policy|\\\\(change this name with the correct company name\\\\)|do not open|ok to delete|no longer at|no longer used|can be deleted|\\\\[to be removed\\\\]|migrate dashboard|deactivated|terminated|termed|\\\\(historical data only\\\\)|\\\\(read only\\\\)|\\bunknown\\b|\\bstaging\\b|pre-prod|\\bdev\\b|\\bdeveloper\\b|\\bQA\\b|R&D|\\bbeta\\b|\\btrial\\b|acceptance test|regression testing|firmware testing|migration test|testbed|test site|test drive|customer simulation|holding area|\b\PRUEBA\\b|\\bdupe\\b|Samsara Example|in house|house setup|QE Org|playground|interview|not working|deprecated|live link|dead dash|\\/\\/|\\\\*\\\\*\\\\*|\\\\*\\\\*|\\\\*)' -- regex pattern to filter out internal orgs (from Mo)
),
licences AS (
    SELECT l.*, o.account_id
    FROM {LICENSE_TABLE} l
    JOIN orgs o
        ON l.sam_number = COALESCE(o.salesforce_sam_number, o.sam_number)
),
users AS (
    SELECT duo.*, o.account_id
    FROM datamodel_platform.dim_users_organizations duo
    JOIN datamodel_platform_bronze.raw_clouddb_users_organizations rcuo
        ON duo.org_id = rcuo.organization_id
        AND duo.user_id = rcuo.user_id
    JOIN orgs o
        ON duo.org_id = o.org_id
    WHERE
        duo.date = '{PARTITION_START}'
        AND rcuo.date = '{PARTITION_START}'
        AND COALESCE(DATE(rcuo.expire_at), DATE_ADD(rcuo.date, 1)) >= rcuo.date -- keep to only active users
        AND (
            duo.email NOT LIKE '%samsara.canary%'
            AND duo.email NOT LIKE '%samsara.forms.canary%'
            AND duo.email NOT LIKE '%samsaracanarydevcontractor%'
            AND duo.email NOT LIKE '%samsaratest%'
            AND duo.email NOT LIKE '%@samsara-service-account.com'
            AND duo.email NOT LIKE '%@samsara%'
            AND name != 'Samsara Automations'
        )
),
lic_flags AS (
    SELECT
        account_id,
        MAX(CASE WHEN is_trial = FALSE AND product_net_qty > 0
            AND (DATE(contract_end_date) >= '{PARTITION_START}' OR contract_end_date IS NULL)
            THEN 1 ELSE 0 END) AS has_paid_active,
        MAX(CASE WHEN is_trial = TRUE AND product_net_qty > 0
            AND (DATE(contract_end_date) >= '{PARTITION_START}' OR contract_end_date IS NULL)
            THEN 1 ELSE 0 END) AS has_trial_active,
        MAX(CASE WHEN is_trial = TRUE AND product_net_qty > 0
            AND DATE(contract_end_date) BETWEEN DATEADD(month, -12, '{PARTITION_START}') AND '{PARTITION_START}'
            THEN 1 ELSE 0 END) AS trial_expired_lt12,
        MAX(CASE WHEN is_trial = TRUE AND product_net_qty > 0
            AND DATE(contract_end_date) < DATEADD(month, -12, '{PARTITION_START}')
            THEN 1 ELSE 0 END) AS trial_expired_gt12
    FROM licences
    GROUP BY account_id
),
opportunities AS (
    SELECT DISTINCT
        org.account_id,
        MAX(CASE WHEN stage_name IN ('Decision', 'Proposal', 'Purchase', 'Closing', 'Trial', 'Ready to Ship') THEN 1 ELSE 0 END) AS has_relevant_opportunity
    FROM finopsdb.opportunities o
    JOIN orgs org
        ON o.sam_number = org.sam_number
    GROUP BY org.account_id
),
user_flags AS (
    SELECT
        account_id,
        CASE
            WHEN COUNT(*) = SUM(CASE WHEN is_samsara_email THEN 1 ELSE 0 END)
            THEN 1
            ELSE 0
        END AS all_internal -- tracks whether all users in the org are internal or not
    FROM users
    GROUP BY account_id
)
SELECT
    o.date,
    o.org_id,
    o.sam_number,
    o.account_id,
    o.internal_type,
    o.account_arr_segment,
    o.account_size_segment_name,
    o.release_type,
    CASE
    WHEN o.internal_type <> 0
        OR o.sam_number IN ('C2N65AXKY7', 'CJEU6A8TFD', 'CR5MBC7RNY', 'C2NUBA3HFM', 'CNU9J2BYRW') -- these are the SAMs we filter out in paid customer definition
        OR COALESCE(u.all_internal, 0) = 1
        THEN 'Internal Orgs'
    WHEN f.has_paid_active = 1 AND (f.has_trial_active = 1 OR f.trial_expired_lt12 = 1)
        THEN 'Expansion'
    WHEN f.has_paid_active = 1 AND f.has_trial_active = 0 AND f.trial_expired_lt12 = 0
        THEN 'Customer'
    WHEN f.has_paid_active = 0 AND (f.has_trial_active = 1 OR (f.has_trial_active = 0 AND f.trial_expired_lt12 = 1 AND opp.has_relevant_opportunity = 1))
        THEN 'First Purchase Prospect'
    WHEN f.has_paid_active = 0 AND f.trial_expired_gt12 = 1
        THEN 'Failed Trial'
    ELSE 'Internal Orgs'
    END AS org_category
FROM orgs o
LEFT JOIN lic_flags f USING (account_id)
LEFT JOIN user_flags u USING (account_id)
LEFT JOIN opportunities opp USING (account_id)
"""

@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""(Regional) Classifies orgs based on their licenses and users""",
        row_meaning="""Each row represents an org and its classification""",
        related_table_info={},
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    upstreams=[
        "datamodel_core.dim_organizations",
        "datamodel_platform.dim_users_organizations",
        "datamodel_platform_bronze.raw_clouddb_users_organizations",
        "finopsdb.opportunities",
    ],
    primary_keys=PRIMARY_KEYS,
    partitioning=["date"],
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_dim_organizations_classification"),
        NonNullDQCheck(name="dq_non_null_dim_organizations_classification", non_null_columns=["date", "org_id", "org_category"], block_before_write=True),
        PrimaryKeyDQCheck(name="dq_pk_dim_organizations_classification", primary_keys=PRIMARY_KEYS, block_before_write=True)
    ],
    backfill_start_date="2024-08-01",
    backfill_batch_size=1,
    write_mode=WarehouseWriteMode.OVERWRITE,
)
def dim_organizations_classification(context: AssetExecutionContext) -> str:
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    region = context.asset_key.path[0]
    if region == AWSRegion.US_WEST_2:
        license_table = 'edw.silver.fct_license_orders'
    else:
        license_table = 'edw_delta_share.silver.fct_license_orders'
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        LICENSE_TABLE=license_table,
    )
    context.log.info(f"{query}")
    return query
