from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.constants import (
    DATAWEB_PARTITION_DATE,
    ProductArea,
)
from dataweb.userpkgs.firmware.metric import (
    Metric,
    MetricEnum,
    StatMetadata,
    StrEnum,
    check_unique_metric_strings,
)
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
)
from dataweb.userpkgs.firmware.table import (
    Definitions,
    KinesisStats,
    ProductAnalyticsStaging,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
    schema_to_columns_with_property,
)


class TableDimension(StrEnum):
    SOURCE_ADDRESS_CLAIMED = "source_address_claimed"
    NO_OPEN_ADDRESS_FOUND = "no_open_address_found"


class TableMetric(MetricEnum):
    SOURCE_ADDRESS_CLAIMED = Metric(
        type=ProductAnalyticsStaging.FCT_OSD_J1939_CLAIMED_ADDRESS,
        field=TableDimension.SOURCE_ADDRESS_CLAIMED,
        label="j1939_source_address_claimed",
        metadata=StatMetadata(
            product_area=ProductArea.VEHICLE_DIAGNOSTICS,
            sub_product_area=ProductArea.GENERAL_INFORMATION,
            signal_name="J1939 Address Claim Information",
            default_priority=1000,
        ),
    )
    NO_OPEN_ADDRESS_FOUND = Metric(
        type=ProductAnalyticsStaging.FCT_OSD_J1939_CLAIMED_ADDRESS,
        field=TableDimension.NO_OPEN_ADDRESS_FOUND,
        label="j1939_no_open_address_found",
    )


check_unique_metric_strings(TableMetric)

SCHEMA = columns_to_schema(
    ColumnType.DATE,
    ColumnType.TIME,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    ColumnType.BUS_ID,
    ColumnType.REQUEST_ID,
    ColumnType.RESPONSE_ID,
    ColumnType.OBD_VALUE,
    ColumnType.VALUE,
    Column(
        name=TableDimension.SOURCE_ADDRESS_CLAIMED,
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="The source address claimed by the device."),
    ),
    Column(
        name=TableDimension.NO_OPEN_ADDRESS_FOUND,
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(comment="Whether an open address was found."),
    ),
)

PRIMARY_KEYS = schema_to_columns_with_property(SCHEMA)
NON_NULL_COLUMNS = schema_to_columns_with_property(SCHEMA, "nullable")

QUERY = """

WITH

stats AS (
    SELECT
        date
        , time
        , org_id
        , object_id AS device_id
        , diagBusToBusId.bus_id
        , COALESCE(value.proto_value.j1939_claimed_address_info.source_address_claimed, 0) AS source_address_claimed
        , COALESCE(value.proto_value.j1939_claimed_address_info.no_open_address_found, false) AS no_open_address_found

    FROM
        kinesisstats.osdj1939claimedaddress

    LEFT JOIN
        definitions.diagnostic_bus_to_bus_id_mapping diagBusToBusId
        ON value.proto_value.j1939_claimed_address_info.bus = diagBusToBusId.diagnostic_bus_id

    WHERE
        date BETWEEN "{date_start}" AND "{date_end}"
        AND NOT value.is_end
        AND NOT value.is_databreak
)

select
    date
    , time
    , org_id
    , device_id
    , bus_id
    , CAST(NULL AS LONG) AS request_id
    , CAST(NULL AS LONG) AS response_id
    , CAST(NULL AS LONG) AS obd_value
    , CAST(NULL AS DOUBLE) AS value
    , source_address_claimed
    , no_open_address_found

FROM
    stats

"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Extract J1939 address claimed per device. Note that this only runs on vehicles with a J1939 cable and J1939 protocol available.",
        row_meaning="J1939 addresses for a device",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(KinesisStats.OSD_J1939_CLAIMED_ADDRESS),
        AnyUpstream(Definitions.DIAGNOSTIC_BUS_TO_BUS_ID_MAPPING),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=5,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_OSD_J1939_CLAIMED_ADDRESS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def fct_osdj1939claimedaddress(context: AssetExecutionContext) -> str:
    partition_keys = partition_key_ranges_from_context(context)[0]

    return QUERY.format(
        date_start=partition_keys[0],
        date_end=partition_keys[-1],
    )
