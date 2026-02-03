from dagster import AssetExecutionContext, AssetKey, DailyPartitionsDefinition
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    AWSRegion,
    Database,
    WarehouseWriteMode,
    TableType,
)
from dataweb.userpkgs.utils import build_table_description

daily_partition_def = DailyPartitionsDefinition(start_date="2023-01-01")


@table(
    database=Database.DATAENGINEERING,
    description=build_table_description(
        table_desc="""(Global) Users from customer accounts that have accessed their Samsara cloud dashboard, indexed by email address and calendar date.""",
        row_meaning="""Login records to the Samsara cloud dashboard""",
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=[
        {
            "name": "date",
            "type": "string",
            "nullable": False,
            "metadata": {"comment": "Calendar date"},
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
            "name": "sam_number",
            "type": "string",
            "nullable": False,
            "metadata": {"comment": "SAM number of the associated account_id"},
        },
        {
            "name": "org_id",
            "type": "long",
            "nullable": False,
            "metadata": {
                "comment": "The Samsara cloud dashboard ID that the data belongs to"
            },
        },
        {
            "name": "region",
            "type": "string",
            "nullable": False,
            "metadata": {
                "comment": "The server database (NA/EU/CA) with which the customer's data is associated"
            },
        },
        {
            "name": "org_tenure",
            "type": "integer",
            "nullable": True,
            "metadata": {
                "comment": "Number of elapsed days between when the customer started at Samsara and the current date"
            },
        },
        {
            "name": "account_arr_segment",
            "type": "string",
            "nullable": True,
            "metadata": {
                "comment": "ARR from the previously publicly reporting quarter in the buckets: {<0, 0 - 100k, 100k - 500K, 500K - 1M, 1M+}."
            },
        },
        {
            "name": "account_size_segment_name",
            "type": "string",
            "nullable": True,
            "metadata": {
                "comment": "Classifies accounts by size - Small Business, Mid Market, Enterprise"
            },
        },
        {
            "name": "account_industry",
            "type": "string",
            "nullable": True,
            "metadata": {"comment": "Industry classification of the account"},
        },
        {
            "name": "is_paid_customer",
            "type": "boolean",
            "nullable": True,
            "metadata": {
                "comment": "The customer organization has a non-zero count of non-expired and non-trial licenses"
            },
        },
        {
            "name": "is_paid_safety_customer",
            "type": "boolean",
            "nullable": True,
            "metadata": {
                "comment": "The customer organization has at least one active license within the safety product family"
            },
        },
        {
            "name": "is_paid_stce_customer",
            "type": "boolean",
            "nullable": True,
            "metadata": {
                "comment": "The customer organization has at least one active license within the STCE product family"
            },
        },
        {
            "name": "is_paid_telematics_customer",
            "type": "boolean",
            "nullable": True,
            "metadata": {
                "comment": "The customer organization has at least one active license within the Telematics product family"
            },
        },
        {
            "name": "user_email",
            "type": "string",
            "nullable": True,
            "metadata": {
                "comment": "Customer email used to access the cloud dashboard"
            },
        },
        {
            "name": "user_id",
            "type": "long",
            "nullable": True,
            "metadata": {"comment": "Customer ID"},
        },
        {
            "name": "time",
            "type": "timestamp",
            "nullable": False,
            "metadata": {"comment": "Timestamp of the mixpanel event creation."},
        },
        {
            "name": "accessed_url",
            "type": "string",
            "nullable": False,
            "metadata": {
                "comment": "The url of the cloud dashboard accessed by the user."
            },
        },
    ],
    upstreams=[
        "us-west-2|eu-west-1|ca-central-1:datamodel_core.dim_organizations",
        "us-west-2:datamodel_platform_silver.stg_cloud_routes",
    ],
    primary_keys=[
        "date",
        "org_id",
        "time",
        "user_email",
        "accessed_url",
        "region",
        "user_id",
    ],
    partitioning=daily_partition_def,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_cloud_dashboard_user_activity_global"),
        NonNullDQCheck(
            name="dq_non_null_cloud_dashboard_user_activity_global",
            non_null_columns=["date", "org_id", "time", "accessed_url", "region"],
            block_before_write=True,
        ),
        PrimaryKeyDQCheck(
            name="dq_pk_cloud_dashboard_user_activity_global",
            primary_keys=[
                "date",
                "org_id",
                "time",
                "user_email",
                "accessed_url",
                "region",
                "user_id",
            ],
            block_before_write=True,
        ),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=5,
    notifications=[f"slack:{SLACK_ALERTS_CHANNEL_DATA_ENGINEERING}"],
)
def cloud_dashboard_user_activity_global(context: AssetExecutionContext) -> str:

    query = """
    WITH global_orgs AS (
        SELECT date
        , parent_sam_number
        , sam_number
        , org_id
        , 'us-west-2' AS region
        , DATEDIFF(date, created_at) AS org_tenure
        , account_arr_segment
        , account_size_segment_name
        , account_industry
        , is_paid_customer
        , is_paid_safety_customer
        , is_paid_stce_customer
        , is_paid_telematics_customer
        FROM datamodel_core.dim_organizations
        WHERE date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
        AND internal_type_name = 'Customer Org'

        UNION

        SELECT date
        , parent_sam_number
        , sam_number
        , org_id
        , 'eu-west-1' AS region
        , DATEDIFF(date, created_at) AS org_tenure
        , account_arr_segment
        , account_size_segment_name
        , account_industry
        , is_paid_customer
        , is_paid_safety_customer
        , is_paid_stce_customer
        , is_paid_telematics_customer
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations`
        WHERE date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
        AND internal_type_name = 'Customer Org'

        UNION

        SELECT date
        , parent_sam_number
        , sam_number
        , org_id
        , 'ca-central-1' AS region
        , DATEDIFF(date, created_at) AS org_tenure
        , account_arr_segment
        , account_size_segment_name
        , account_industry
        , is_paid_customer
        , is_paid_safety_customer
        , is_paid_stce_customer
        , is_paid_telematics_customer
        FROM data_tools_delta_share_ca.datamodel_core.dim_organizations
        WHERE date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
        AND internal_type_name = 'Customer Org'
    )
    SELECT DISTINCT
    mp.date::STRING
    , parent_sam_number
    , sam_number
    , mp.org_id
    , region
    , org_tenure
    , account_arr_segment
    , account_size_segment_name
    , account_industry
    , is_paid_customer
    , is_paid_safety_customer
    , is_paid_stce_customer
    , is_paid_telematics_customer
    , LOWER(mp.userEmail) AS user_email
    , mp.user_id
    , mp.time
    , mp.mp_current_url AS accessed_url
    FROM datamodel_platform_silver.stg_cloud_routes AS mp
    INNER JOIN global_orgs USING(date, org_id)   -- filter to customer orgs
    WHERE mp.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
      AND mp.is_customer_event = TRUE
    """

    return query
