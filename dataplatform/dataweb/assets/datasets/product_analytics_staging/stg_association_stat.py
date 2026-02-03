from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    ColumnDescription,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
)
from pyspark.sql import SparkSession

PRIMARY_KEYS = [
    "date",
    "org_id",
    "central_device_id",
    "peripheral_device_id",
    "start_ms",
    "end_ms"
]

SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.DATE,
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "The Internal ID for the customer`s Samsara org."},
    },
    {
        "name": "peripheral_device_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Device ID of the peripheral device"},
    },
    {
        "name": "peripheral_product_name",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Product name of the peripheral device"},
    },
    {
        "name": "central_device_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Device ID of the central device"},
    },
    {
        "name": "central_product_name",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Product name of the central device"},
    },
    {
        "name": "start_ms",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Start time of association"},
    },
    {
        "name": "end_ms",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "End time of association"},
    },
    {
        "name": "peripheral_distance_moved_with_central_meters",
        "type": "double",
        "nullable": False,
        "metadata": {"comment": "Peripheral distance moved with the central device to start the association"},
    },
    {
        "name": "total_peripheral_distance_moved_meters",
        "type": "double",
        "nullable": False,
        "metadata": {"comment": "Total distance moved by peripheral devices to start the association"},
    },
    {
        "name": "separation_distance_meters",
        "type": "double",
        "nullable": False,
        "metadata": {"comment": "Separation distance between devices at the end of the association"},
    },
]

QUERY = """
    WITH eoa AS (
        SELECT
            eoa.date,
            eoa.org_id,
            eoa.object_id AS peripheral_device_id,
            dd_at.product_name AS peripheral_product_name,
            eoa.value.proto_value.ble_association_end.central_asset_id AS central_device_id,
            dd_vg.product_name AS central_product_name,
            eoa.value.proto_value.ble_association_end.start_ms,
            eoa.value.proto_value.ble_association_end.end_ms,
            eoa.value.proto_value.ble_association_end.relationship_uuid,
            eoa.value.proto_value.ble_association_end.break_reason,
            eoa.value.proto_value.ble_association_end.association_snapshot.separation_distance_meters
        FROM kinesisstats_history.osdbleassociationend eoa
        JOIN (SELECT device_id, product_name FROM datamodel_core.dim_devices WHERE date = (SELECT MAX(date) FROM datamodel_core.dim_devices)) dd_at
            ON dd_at.device_id = eoa.object_id
        JOIN (SELECT device_id, product_name FROM datamodel_core.dim_devices WHERE date = (SELECT MAX(date) FROM datamodel_core.dim_devices)) dd_vg
            ON dd_vg.device_id = eoa.value.proto_value.ble_association_end.central_asset_id
        WHERE
            eoa.date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
    ),
    soa AS (
        SELECT
            soa.org_id,
            soa.object_id AS peripheral_device_id,
            dd_at.product_name AS peripheral_product_name,
            soa.value.proto_value.ble_association_start.central_asset_id AS central_device_id,
            dd_vg.product_name AS central_product_name,
            soa.value.proto_value.ble_association_start.start_ms,
            soa.value.proto_value.ble_association_start.relationship_uuid,
            soa.value.proto_value.ble_association_start.peripheral_distance_moved_with_central_meters,
            soa.value.proto_value.ble_association_start.total_peripheral_distance_moved_meters
        FROM kinesisstats_history.osdbleassociationstart soa
        JOIN (SELECT device_id, product_name FROM datamodel_core.dim_devices WHERE date = (SELECT MAX(date) FROM datamodel_core.dim_devices)) dd_at
            ON dd_at.device_id = soa.object_id
        JOIN (SELECT device_id, product_name FROM datamodel_core.dim_devices WHERE date = (SELECT MAX(date) FROM datamodel_core.dim_devices)) dd_vg
            ON dd_vg.device_id = soa.value.proto_value.ble_association_start.central_asset_id
        WHERE soa.date BETWEEN date_sub('{PARTITION_START}', 90) AND '{PARTITION_END}'
    )
    SELECT
        eoa.date,
        soa.org_id,
        soa.peripheral_device_id,
        soa.peripheral_product_name,
        soa.central_device_id,
        soa.central_product_name,
        soa.start_ms,
        eoa.end_ms,
        soa.peripheral_distance_moved_with_central_meters,
        soa.total_peripheral_distance_moved_meters,
        eoa.separation_distance_meters
    FROM soa
    INNER JOIN eoa -- only want ended associations
        ON soa.peripheral_device_id = eoa.peripheral_device_id
        AND soa.central_device_id = eoa.central_device_id
        AND soa.relationship_uuid = eoa.relationship_uuid
        AND soa.start_ms = eoa.start_ms
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""Stats for closed associations, used for gauging association accuracy""",
        row_meaning="""Each row contains an instance of a BLE device association""",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DailyPartitionsDefinition(start_date="2025-07-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=5,
    upstreams=[
        "datamodel_core.dim_devices",
        "kinesisstats_history.osdbleassociationstart",
        "kinesisstats_history.osdbleassociationend",
    ],
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_stg_association_stat", block_before_write=False), #keeping as non-blocking for now until we get more data in CA
        NonNullDQCheck(name="dq_non_null_stg_association_stat", non_null_columns=PRIMARY_KEYS, block_before_write=False),
        PrimaryKeyDQCheck(name="dq_pk_stg_association_stat", primary_keys=PRIMARY_KEYS, block_before_write=False)
    ]
)
def stg_association_stat(context: AssetExecutionContext) -> str:
    context.log.info("Updating stg_association_stat")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    PARTITION_END = partition_keys[-1]
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        PARTITION_END=PARTITION_END,
    )
    context.log.info(f"{query}")
    return query
