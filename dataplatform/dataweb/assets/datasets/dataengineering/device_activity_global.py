from dagster import AssetExecutionContext
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    AWSRegion,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import build_table_description


@table(
    database=Database.DATAENGINEERING,
    description=build_table_description(
        table_desc="""A dataset containing the devices that were active/online on a given day.""",
        row_meaning="""A device that was active or online on a given day""",
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
            "metadata": {
                "comment": "The date on which the device activity took place."
            },
        },
        {
            "name": "org_id",
            "type": "long",
            "nullable": False,
            "metadata": {"comment": "The Internal ID for the customer`s Samsara org."},
        },
        {
            "name": "sam_number",
            "type": "string",
            "nullable": True,
            "metadata": {
                "comment": "This is the internal Samsara Customer account ID."
            },
        },
        {
            "name": "device_id",
            "type": "long",
            "nullable": False,
            "metadata": {
                "comment": "Unique identifier for devices. Joins to datamodel_core.dim_devices"
            },
        },
        {
            "name": "account_size_segment",
            "type": "string",
            "nullable": True,
            "metadata": {
                "comment": "Classifies accounts by size - Small Business, Mid Market, Enterprise"
            },
        },
        {
            "name": "product_id",
            "type": "long",
            "nullable": True,
            "metadata": {
                "comment": "Joins to definitions.products to allow for human-readable product names and Product Group ID"
            },
        },
        {
            "name": "device_type",
            "type": "string",
            "nullable": True,
            "metadata": {
                "comment": "Attribute defining the device type (e.g. VG, CM, AG, ...)"
            },
        },
        {
            "name": "region",
            "type": "string",
            "nullable": False,
            "metadata": {"comment": "AWS cloud region where record comes from"},
        },
        {
            "name": "is_device_active",
            "type": "long",
            "nullable": True,
            "metadata": {"comment": "Whether device is active or not"},
        },
        {
            "name": "is_device_online",
            "type": "long",
            "nullable": True,
            "metadata": {"comment": "Whether device is online or not"},
        },
    ],
    upstreams=[
        "us-west-2|eu-west-1|ca-central-1:datamodel_core.dim_organizations",
        "us-west-2|eu-west-1|ca-central-1:datamodel_core.dim_devices",
        "us-west-2|eu-west-1|ca-central-1:datamodel_core_silver.stg_device_activity_daily",
    ],
    primary_keys=[
        "date",
        "org_id",
        "sam_number",
        "device_id",
        "region",
    ],
    partitioning=["date"],
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_device_activity_global"),
        NonNullDQCheck(name="dq_non_null_device_activity_global", non_null_columns=["date", "org_id", "device_id", "region"], block_before_write=True),
        PrimaryKeyDQCheck(name="dq_pk_device_activity_global", primary_keys=["date", "org_id", "sam_number", "device_id", "region"], block_before_write=True)
    ],
    backfill_start_date="2023-01-01",
    backfill_batch_size=5,
    write_mode=WarehouseWriteMode.OVERWRITE,
)
def device_activity_global(context: AssetExecutionContext) -> str:

    query = """
    WITH
    us_active_devices AS (
        SELECT DATE(ldo.date) AS date,
            ldo.org_id,
            ldo.device_id,
            org.sam_number,
            org.account_size_segment_name AS account_size_segment,
            dev.product_id,
            dev.product_name AS device_type,
            'us-west-2' as region,
            CASE WHEN ldo.has_heartbeat THEN 1 ELSE 0 END AS is_device_online, -- has heartbeat
            CASE WHEN ldo.is_active THEN 1 ELSE 0 END AS is_device_active -- has activity
        FROM datamodel_core_silver.stg_device_activity_daily ldo
        INNER JOIN datamodel_core.dim_devices dev
            ON ldo.device_id = dev.device_id
            AND ldo.org_id = dev.org_id
            AND ldo.date = dev.date
        INNER JOIN datamodel_core.dim_organizations org
            ON dev.org_id = org.org_id
            AND ldo.date = org.date
        WHERE TRUE -- arbitrary for readability
            AND org.internal_type = 0 -- non-internal
            AND org.account_first_purchase_date IS NOT NULL -- has had at least one purchase
            AND ldo.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}' -- grab relavant dates
        GROUP BY 1,2,3,4,5,6,7,8,9,10 -- like "SELECT DISTINCT", but faster
    ),
    eu_active_devices AS (
        SELECT DATE(ldo.date) AS date,
            ldo.org_id,
            ldo.device_id,
            org.sam_number,
            org.account_size_segment_name AS account_size_segment,
            dev.product_id,
            dev.product_name AS device_type,
            'eu-west-1' as region,
            CASE WHEN ldo.has_heartbeat THEN 1 ELSE 0 END AS is_device_online, -- has heartbeat
            CASE WHEN ldo.is_active THEN 1 ELSE 0 END AS is_device_active -- has activity
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core_silver.db/stg_device_activity_daily` ldo
        INNER JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_devices` dev
            ON ldo.device_id = dev.device_id
            AND ldo.org_id = dev.org_id
            AND ldo.date = dev.date
        INNER JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` org
            ON dev.org_id = org.org_id
            AND ldo.date = org.date
        WHERE TRUE -- arbitrary for readability
            AND org.internal_type = 0 -- non-internal
            AND org.account_first_purchase_date IS NOT NULL -- has had at least one purchase
            AND ldo.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}' -- grab relavant dates
        GROUP BY 1,2,3,4,5,6,7,8,9,10 -- like "SELECT DISTINCT", but faster
    ),
    ca_active_devices AS (
        SELECT DATE(ldo.date) AS date,
            ldo.org_id,
            ldo.device_id,
            org.sam_number,
            org.account_size_segment_name AS account_size_segment,
            dev.product_id,
            dev.product_name AS device_type,
            'ca-central-1' as region,
            CASE WHEN ldo.has_heartbeat THEN 1 ELSE 0 END AS is_device_online, -- has heartbeat
            CASE WHEN ldo.is_active THEN 1 ELSE 0 END AS is_device_active -- has activity
        FROM data_tools_delta_share_ca.datamodel_core_silver.stg_device_activity_daily ldo
        INNER JOIN data_tools_delta_share_ca.datamodel_core.dim_devices dev
            ON ldo.device_id = dev.device_id
            AND ldo.org_id = dev.org_id
            AND ldo.date = dev.date
        INNER JOIN data_tools_delta_share_ca.datamodel_core.dim_organizations org
            ON dev.org_id = org.org_id
            AND ldo.date = org.date
        WHERE TRUE -- arbitrary for readability
            AND org.internal_type = 0 -- non-internal
            AND org.account_first_purchase_date IS NOT NULL -- has had at least one purchase
            AND ldo.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}' -- grab relavant dates
        GROUP BY 1,2,3,4,5,6,7,8,9,10 -- like "SELECT DISTINCT", but faster
    ),
    global_active_devices AS (
        SELECT
            date::STRING,
            org_id,
            sam_number,
            device_id,
            account_size_segment,
            product_id,
            device_type,
            region,
            is_device_active::BIGINT,
            is_device_online::BIGINT
        FROM
            us_active_devices

        UNION

        SELECT
            date::STRING,
            org_id,
            sam_number,
            device_id,
            account_size_segment,
            product_id,
            device_type,
            region,
            is_device_active::BIGINT,
            is_device_online::BIGINT
        FROM eu_active_devices

        UNION

        SELECT
            date::STRING,
            org_id,
            sam_number,
            device_id,
            account_size_segment,
            product_id,
            device_type,
            region,
            is_device_active::BIGINT,
            is_device_online::BIGINT
        FROM ca_active_devices
    )
    SELECT *
    FROM global_active_devices
    """
    return query
