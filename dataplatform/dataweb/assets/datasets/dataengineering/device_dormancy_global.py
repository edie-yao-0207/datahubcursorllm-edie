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

PRIMARY_KEYS = ["date", "org_id", "device_id", "region"]

SCHEMA = [
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
        "name": "device_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Attribute defining the device type (e.g. VG, CM, AG, ...)"
        }
    },
    {
        "name": "region",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Samsara cloud region"},
    },
    {
        "name": "device_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": ColumnDescription.DEVICE_ID},
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
        "name": "asset_type",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Used to determine if the asset is Trailer, Equipment, Unpowered, Vehicle"
        },
    },
    {
        "name": "asset_type_name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Based on asset_type. 1 = 'Trailer' 2 = 'Equipment' 3 = 'Unpowered' 4 = 'Vehicle' 0 = 'None'"
        },
    },
    {
        "name": "last_activity_date",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Date when device was last considered to be active"},
    },
    {
        "name": "days_since_last_activity",
        "type": "integer",
        "nullable": False,
        "metadata": {"comment": "Days since device was last considered to be active"},
    },
]

QUERY = """
WITH last_active_date_us as (
        SELECT
        dad.date AS date,
        dd.org_id,
        dd.device_type,
        'us-west-2' AS region,
        dd.device_id,
        p.name AS product_name,
        p.product_id AS product_id,
        dd.asset_type,
        dd.asset_type_name,
        COALESCE(
            CASE WHEN
            (
                dd.device_type LIKE 'AG%'
                AND dd.asset_type NOT IN (0, 3)
                AND dd.asset_type IS NOT NULL
            )
            OR dd.device_type LIKE 'VG%'
            OR dd.device_type LIKE 'CM%'
            OR p.product_id IN (56, 57, 58, 103, 141, 187) -- devices where heartbeat + trip matters
            THEN
            MAX(
                (
                    SELECT MAX(dad_inner.date)
                    FROM datamodel_core_silver.stg_device_activity_daily dad_inner
                    WHERE dad_inner.device_id = dd.device_id
                    AND dad_inner.has_heartbeat = TRUE
                    AND dad_inner.trip_count > 0
                    AND dad_inner.date <= dad.date
                )
            )
            ELSE -- devices where we only care about heartbeat
            MAX(
                (
                    SELECT MAX(dad_inner.date)
                    FROM datamodel_core_silver.stg_device_activity_daily dad_inner
                    WHERE dad_inner.device_id = dd.device_id
                    AND dad_inner.has_heartbeat = TRUE
                    AND dad_inner.date <= dad.date
                )
            ) END,
            NULL
        ) AS last_activity_date
        FROM
        datamodel_core.dim_devices dd
        JOIN
        datamodel_core_silver.stg_device_activity_daily dad
        ON dd.org_id = dad.org_id
        AND dd.device_id = dad.device_id
        JOIN
        definitions.products p
        ON dd.product_id = p.product_id
        WHERE
        dd.gateway_id IS NOT NULL
        AND dd.date = (SELECT MAX(date) FROM datamodel_core.dim_devices)
        AND dad.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
        GROUP BY
        dad.date, dd.org_id, dd.device_type, region, dd.device_id, p.name, p.product_id, dd.asset_type, dd.asset_type_name
    ),
    last_active_date_eu as (
        SELECT
        dad.date AS date,
        dd.org_id,
        dd.device_type,
        'eu-west-1' AS region,
        dd.device_id,
        p.name AS product_name,
        p.product_id AS product_id,
        dd.asset_type,
        dd.asset_type_name,
        COALESCE(
            CASE WHEN
            (
                dd.device_type LIKE 'AG%'
                AND dd.asset_type NOT IN (0, 3)
                AND dd.asset_type IS NOT NULL
            )
            OR dd.device_type LIKE 'VG%'
            OR dd.device_type LIKE 'CM%'
            OR p.product_id IN (56, 57, 58, 103, 141, 187) -- devices where heartbeat + trip matters
            THEN
            MAX(
                (
                    SELECT MAX(dad_inner.date)
                    FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core_silver.db/stg_device_activity_daily` dad_inner
                    WHERE dad_inner.device_id = dd.device_id
                    AND dad_inner.has_heartbeat = TRUE
                    AND dad_inner.trip_count > 0
                    AND dad_inner.date <= dad.date
                )
            )
            ELSE -- devices where we only care about heartbeat
            MAX(
                (
                    SELECT MAX(dad_inner.date)
                    FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core_silver.db/stg_device_activity_daily` dad_inner
                    WHERE dad_inner.device_id = dd.device_id
                    AND dad_inner.has_heartbeat = TRUE
                    AND dad_inner.date <= dad.date
                )
            ) END,
            NULL
        ) AS last_activity_date
        FROM
        delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_devices` dd
        JOIN
        delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core_silver.db/stg_device_activity_daily` dad
        ON dd.org_id = dad.org_id
        AND dd.device_id = dad.device_id
        JOIN
        definitions.products p
        ON dd.product_id = p.product_id
        WHERE
        dd.gateway_id IS NOT NULL
        AND dd.date = (SELECT MAX(date) FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_devices`)
        AND dad.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
        GROUP BY
        dad.date, dd.org_id, dd.device_type, region, dd.device_id, p.name, p.product_id, dd.asset_type, dd.asset_type_name
    ),
    last_active_date_ca as (
        SELECT
        dad.date AS date,
        dd.org_id,
        dd.device_type,
        'ca-central-1' AS region,
        dd.device_id,
        p.name AS product_name,
        p.product_id AS product_id,
        dd.asset_type,
        dd.asset_type_name,
        COALESCE(
            CASE WHEN
            (
                dd.device_type LIKE 'AG%'
                AND dd.asset_type NOT IN (0, 3)
                AND dd.asset_type IS NOT NULL
            )
            OR dd.device_type LIKE 'VG%'
            OR dd.device_type LIKE 'CM%'
            OR p.product_id IN (56, 57, 58, 103, 141, 187) -- devices where heartbeat + trip matters
            THEN
            MAX(
                (
                    SELECT MAX(dad_inner.date)
                    FROM data_tools_delta_share_ca.datamodel_core_silver.stg_device_activity_daily dad_inner
                    WHERE dad_inner.device_id = dd.device_id
                    AND dad_inner.has_heartbeat = TRUE
                    AND dad_inner.trip_count > 0
                    AND dad_inner.date <= dad.date
                )
            )
            ELSE -- devices where we only care about heartbeat
            MAX(
                (
                    SELECT MAX(dad_inner.date)
                    FROM data_tools_delta_share_ca.datamodel_core_silver.stg_device_activity_daily dad_inner
                    WHERE dad_inner.device_id = dd.device_id
                    AND dad_inner.has_heartbeat = TRUE
                    AND dad_inner.date <= dad.date
                )
            ) END,
            NULL
        ) AS last_activity_date
        FROM
        data_tools_delta_share_ca.datamodel_core.dim_devices dd
        JOIN
        data_tools_delta_share_ca.datamodel_core_silver.stg_device_activity_daily dad
        ON dd.org_id = dad.org_id
        AND dd.device_id = dad.device_id
        JOIN
        definitions.products p
        ON dd.product_id = p.product_id
        WHERE
        dd.gateway_id IS NOT NULL
        AND dd.date = (SELECT MAX(date) FROM data_tools_delta_share_ca.datamodel_core.dim_devices)
        AND dad.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
        GROUP BY
        dad.date, dd.org_id, dd.device_type, region, dd.device_id, p.name, p.product_id, dd.asset_type, dd.asset_type_name
    )
    select
        date,
        org_id,
        device_type,
        region,
        device_id,
        product_id,
        asset_type,
        asset_type_name,
        last_activity_date,
        CASE WHEN last_activity_date IS NOT NULL THEN DATEDIFF(date, last_activity_date) ELSE NULL END AS days_since_last_activity
    from last_active_date_us

    UNION ALL

    select
        date,
        org_id,
        device_type,
        region,
        device_id,
        product_id,
        asset_type,
        asset_type_name,
        last_activity_date,
        CASE WHEN last_activity_date IS NOT NULL THEN DATEDIFF(date, last_activity_date) ELSE NULL END AS days_since_last_activity
    from last_active_date_eu

    UNION ALL

    select
        date,
        org_id,
        device_type,
        region,
        device_id,
        product_id,
        asset_type,
        asset_type_name,
        last_activity_date,
        CASE WHEN last_activity_date IS NOT NULL THEN DATEDIFF(date, last_activity_date) ELSE NULL END AS days_since_last_activity
    from last_active_date_ca
"""


@table(
    database=Database.DATAENGINEERING,
    description=build_table_description(
        table_desc="""An extension of device activity to bring in uninstalled devices, used for dormancy calculation""",
        row_meaning="""Each row represents a device and its last activity date""",
        related_table_info={},
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=["date"],
    backfill_start_date="2023-01-01",
    backfill_batch_size=1,
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_device_dormancy_global"),
        NonNullDQCheck(name="dq_non_null_device_dormancy_global", non_null_columns=["date", "org_id", "device_id", "region"], block_before_write=True),
        PrimaryKeyDQCheck(name="dq_pk_device_dormancy_global", primary_keys=["date", "org_id", "device_id", "region"], block_before_write=True)
    ],
    upstreams=[
        "us-west-2|eu-west-1|ca-central-1:datamodel_core.dim_devices",
        "us-west-2|eu-west-1|ca-central-1:datamodel_core_silver.stg_device_activity_daily",
    ],
)
def device_dormancy_global(context: AssetExecutionContext) -> str:
    context.log.info("Updating device_dormancy_global")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    PARTITION_END = partition_keys[1]
    query = QUERY.format(
        FIRST_PARTITION_START=PARTITION_START,
        FIRST_PARTITION_END=PARTITION_END,
    )
    context.log.info(f"{query}")
    return query
