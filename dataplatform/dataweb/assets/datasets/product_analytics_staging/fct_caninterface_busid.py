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
from dataweb.userpkgs.firmware.metric import StrEnum
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
)
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.utils import (
    build_table_description,
    schema_to_columns_with_property,
)
from pyspark.sql import DataFrame, SparkSession

class TableDimensions(StrEnum):
    CAN_INTERFACE = "can_interface"
    BUS_ID = "bus_id"
    INTERFACE_NAME = "interface_name"
    BUS_NAME = "bus_name"


SCHEMA = columns_to_schema(
    Column(
        name=TableDimensions.CAN_INTERFACE,
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="CAN interface identifier from proto_value.can_bitrate_detection_v2_results.can_interface"
        ),
    ),
    Column(
        name=TableDimensions.BUS_ID,
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="Bus ID corresponding to the CAN interface"),
    ),
    Column(
        name=TableDimensions.INTERFACE_NAME,
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(comment="Human-readable name of the CAN interface"),
    ),
    Column(
        name=TableDimensions.BUS_NAME,
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(comment="Human-readable name of the bus"),
    ),
)

PRIMARY_KEYS = [
    TableDimensions.CAN_INTERFACE,
]

NON_NULL_KEYS = schema_to_columns_with_property(SCHEMA, "nullable")




@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Mapping of CAN interface identifiers to bus IDs for CAN bitrate detection v2.",
        row_meaning="Each row represents a CAN interface and its corresponding bus ID mapping.",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    upstreams=[],
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_CANINTERFACE_BUSID.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_KEYS,
        block_before_write=True,
    ),
)
def fct_caninterface_busid(context: AssetExecutionContext) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    # import proto values from within worker function
    from samsaradev.io.hubproto.vdp_pb2 import (
        CAN_INTERFACE_INVALID,
        CAN_INTERFACE_MCP251X,
        CAN_INTERFACE_VG_MCU_PRIMARY,
        CAN_INTERFACE_VG_MCU_SECONDARY,
        CAN_INTERFACE_VG_MCU_TERTIARY,
        CAN_INTERFACE_GSUSB_FALKO,
        CAN_INTERFACE_GSUSB_DUCKBILL,
        BUSID_INVALID,
        BUSID_CAN0,
        BUSID_CAN1,
        BUSID_CAN2,
        BUSID_DUCKBILL,
    )

    # Static mapping of CAN interface type to bus ID
    data = [
        (CAN_INTERFACE_MCP251X, BUSID_CAN0, "CAN_INTERFACE_MCP251X", "BUSID_CAN0"),
        (CAN_INTERFACE_VG_MCU_PRIMARY, BUSID_CAN0, "CAN_INTERFACE_VG_MCU_PRIMARY", "BUSID_CAN0"),
        (CAN_INTERFACE_VG_MCU_SECONDARY, BUSID_CAN1, "CAN_INTERFACE_VG_MCU_SECONDARY", "BUSID_CAN1"),
        (CAN_INTERFACE_VG_MCU_TERTIARY, BUSID_CAN2, "CAN_INTERFACE_VG_MCU_TERTIARY", "BUSID_CAN2"),
        (CAN_INTERFACE_GSUSB_FALKO, BUSID_CAN1, "CAN_INTERFACE_GSUSB_FALKO", "BUSID_CAN1"),
        (CAN_INTERFACE_GSUSB_DUCKBILL, BUSID_DUCKBILL, "CAN_INTERFACE_GSUSB_DUCKBILL", "BUSID_DUCKBILL"),
        (CAN_INTERFACE_INVALID, BUSID_INVALID, "CAN_INTERFACE_INVALID", "BUSID_INVALID"),
    ]

    context.log.info(f"Mapping data: {data}")

    return spark.createDataFrame(
        data, ["can_interface", "bus_id", "interface_name", "bus_name"]
    )
