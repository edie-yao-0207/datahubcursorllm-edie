from dagster import AssetExecutionContext
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
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
    partition_key_ranges_from_context,
)

PRIMARY_KEYS = ["date", "org_id", "region"]

SCHEMA = [
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": ColumnDescription.ORG_ID},
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
        "metadata": {"comment": ColumnDescription.SAM_NUMBER},
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
            "comment": "Queried from edw.salesforce_sterling.account. Date the account was created"
        },
    },
    {
        "name": "account_last_activity_date",
        "type": "date",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.salesforce_sterling.account. Last activity of the account"
        },
    },
    {
        "name": "account_actual_deactivate_date",
        "type": "date",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.salesforce_sterling.account. Date if the account was deactivated"
        },
    },
    {
        "name": "account_last_modified_date",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.salesforce_sterling.account. Date the account was last modified"
        },
    },
    {
        "name": "account_industry",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.salesforce_sterling.account. Industry classification of the account."
        },
    },
    {
        "name": "account_sub_industry",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.salesforce_sterling.account. Sub industry classification of the account."
        },
    },
    {
        "name": "account_cs_tier",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.salesforce_sterling.account. Account tier - Premier, Plus, Starter, Elite. This will be null if there's no match between the org/account tables."
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
            "comment": "Queried from edw.salesforce_sterling.account. Classifies accounts by size - Small Business, Mid Market, Enterprise"
        },
    },
    {
        "name": "account_size_segment_name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.salesforce_sterling.account. Classifies accounts by size - Small Business, Mid Market, Enterprise"
        },
    },
    {
        "name": "account_customer_in_trial_mode",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.salesforce_sterling.account. Boolean, classifies if account is in trial mode"
        },
    },
    {
        "name": "is_account_delinquent",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.salesforce_sterling.account. Boolean, signaling if the account is delinquent"
        },
    },
    {
        "name": "account_delinquency_status",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.salesforce_sterling.account. Provides additional information on delinquent accounts"
        },
    },
    {
        "name": "account_delinquent_as_of",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.salesforce_sterling.account. Data the account became delinquent"
        },
    },
    {
        "name": "account_first_purchase_date",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.salesforce_sterling.account. Date of initial account purchase"
        },
    },
    {
        "name": "account_billing_country",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.salesforce_sterling.account. Geographic information about the account"
        },
    },
    {
        "name": "account_billing_state",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.salesforce_sterling.account. Geographic information about the account"
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
                        (1) org_id has a non-zero count of non-expired and non-trial licenses in datamodel_platform.dim_licenses for corresponding date
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
                        (1) org_id has a non-zero count of non-expired and non-trial licenses in datamodel_platform.dim_licenses for corresponding date
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
                        (1) org_id has a non-zero count of non-expired and non-trial licenses in datamodel_platform.dim_licenses for corresponding date
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
                        (1) org_id has a non-zero count of non-expired and non-trial licenses in datamodel_platform.dim_licenses for corresponding date
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
        "name": "salesforce_sam_number",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "SAM number as reported by Salesforce for the organization"
        },
    },
    {
        "name": "account_ai_model_industry_archetype",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Calculated from clouddb.organizations.release_type_enum. 0 = Phase 1, 1 = Phase 2, 2 = Early Adopter"
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
    {
        "name": "region",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Samsara cloud region"},
    },
]

QUERY = """
    SELECT
        o.org_id,
        o.org_name,
        o.created_at,
        o.locale,
        o.internal_type,
        o.internal_type_name,
        o.quarantine_enabled,
        o.account_id,
        o.parent_account_id,
        o.account_name,
        o.sam_number,
        o.parent_sam_number,
        o.account_created_date,
        o.account_last_activity_date,
        o.account_actual_deactivate_date,
        o.account_last_modified_date,
        o.account_industry,
        o.account_sub_industry,
        o.account_cs_tier,
        o.account_csm_email,
        o.account_size_segment,
        o.account_size_segment_name,
        o.account_customer_in_trial_mode,
        o.is_account_delinquent,
        o.account_delinquency_status,
        o.account_delinquent_as_of,
        o.account_first_purchase_date,
        o.account_billing_country,
        o.account_billing_state,
        o.account_auto_renewal,
        o.is_paid_customer,
        o.is_paid_telematics_customer,
        o.is_paid_safety_customer,
        o.is_paid_stce_customer,
        o.date,
        o.is_simulated_org,
        o.release_type,
        o.account_arr_segment,
        o.salesforce_sam_number,
        o.account_ai_model_industry_archetype,
        o.account_ai_model_primary_industry,
        o.account_ai_model_secondary_industry,
        'us-west-2' as region
    FROM datamodel_core.dim_organizations o
    WHERE date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'

    UNION

    SELECT
        o.org_id,
        o.org_name,
        o.created_at,
        o.locale,
        o.internal_type,
        o.internal_type_name,
        o.quarantine_enabled,
        o.account_id,
        o.parent_account_id,
        o.account_name,
        o.sam_number,
        o.parent_sam_number,
        o.account_created_date,
        o.account_last_activity_date,
        o.account_actual_deactivate_date,
        o.account_last_modified_date,
        o.account_industry,
        o.account_sub_industry,
        o.account_cs_tier,
        o.account_csm_email,
        o.account_size_segment,
        o.account_size_segment_name,
        o.account_customer_in_trial_mode,
        o.is_account_delinquent,
        o.account_delinquency_status,
        o.account_delinquent_as_of,
        o.account_first_purchase_date,
        o.account_billing_country,
        o.account_billing_state,
        o.account_auto_renewal,
        o.is_paid_customer,
        o.is_paid_telematics_customer,
        o.is_paid_safety_customer,
        o.is_paid_stce_customer,
        o.date,
        o.is_simulated_org,
        o.release_type,
        o.account_arr_segment,
        o.salesforce_sam_number,
        o.account_ai_model_industry_archetype,
        o.account_ai_model_primary_industry,
        o.account_ai_model_secondary_industry,
        'eu-west-1' as region
    FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` o
    WHERE date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'

    UNION

    SELECT
        o.org_id,
        o.org_name,
        o.created_at,
        o.locale,
        o.internal_type,
        o.internal_type_name,
        o.quarantine_enabled,
        o.account_id,
        o.parent_account_id,
        o.account_name,
        o.sam_number,
        o.parent_sam_number,
        o.account_created_date,
        o.account_last_activity_date,
        o.account_actual_deactivate_date,
        o.account_last_modified_date,
        o.account_industry,
        o.account_sub_industry,
        o.account_cs_tier,
        o.account_csm_email,
        o.account_size_segment,
        o.account_size_segment_name,
        o.account_customer_in_trial_mode,
        o.is_account_delinquent,
        o.account_delinquency_status,
        o.account_delinquent_as_of,
        o.account_first_purchase_date,
        o.account_billing_country,
        o.account_billing_state,
        o.account_auto_renewal,
        o.is_paid_customer,
        o.is_paid_telematics_customer,
        o.is_paid_safety_customer,
        o.is_paid_stce_customer,
        o.date,
        o.is_simulated_org,
        o.release_type,
        o.account_arr_segment,
        o.salesforce_sam_number,
        o.account_ai_model_industry_archetype,
        o.account_ai_model_primary_industry,
        o.account_ai_model_secondary_industry,
        'ca-central-1' as region
    FROM data_tools_delta_share_ca.datamodel_core.dim_organizations o
    WHERE date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
"""


@table(
    database=Database.DATAENGINEERING,
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
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=["date"],
    backfill_start_date="2023-09-05",
    backfill_batch_size=5,
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_dim_organizations_global"),
        PrimaryKeyDQCheck(
            name="dq_pk_dim_organizations_global", primary_keys=PRIMARY_KEYS, block_before_write=True
        ),
        NonNullDQCheck(
            name="dq_non_null_dim_organization_global", non_null_columns=PRIMARY_KEYS, block_before_write=True
        ),
    ],
    upstreams=[
        "us-west-2|eu-west-1:datamodel_core.dim_organizations", #also depends on CA, but removing for Dagster purposes
    ],
)
def dim_organizations_global(context: AssetExecutionContext) -> str:
    context.log.info("Updating dim_organizations_global")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    PARTITION_END = partition_keys[1]
    query = QUERY.format(
        FIRST_PARTITION_START=PARTITION_START,
        FIRST_PARTITION_END=PARTITION_END,
    )
    context.log.info(f"{query}")
    return query
