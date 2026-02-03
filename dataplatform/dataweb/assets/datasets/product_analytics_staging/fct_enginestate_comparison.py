from dataclasses import replace
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
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_primary_keys,
    get_non_null_columns,
)
from dataweb.userpkgs.firmware.table import KinesisStatsHistory, ProductAnalyticsStaging
from dataweb.userpkgs.firmware.engine_state import interpolate_state_data
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.utils import (
    build_table_description,
)
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F


COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    replace(ColumnType.TIME.value, primary_key=True),
    ColumnType.END_TIME,
    ColumnType.BUS_ID,
    ColumnType.REQUEST_ID,
    ColumnType.RESPONSE_ID,
    ColumnType.OBD_VALUE,
    Column(
        name="prod_state",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Production engine state value as an integer (0 = OFF, 1 = ON, 2 = IDLE)"
        ),
    ),
    Column(
        name="log_only_state",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Log-only engine state value as an integer (0 = OFF, 1 = ON, 2 = IDLE)"
        ),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_KEYS = get_non_null_columns(COLUMNS)

LEGACY_QUERY = """
SELECT
    date,
    org_id,
    object_id AS device_id,
    time,
    value.int_value AS state,
    value.is_start,
    value.is_end,
    value.is_databreak
FROM {kinesisstats_history}.osdenginestate
WHERE date BETWEEN "{date_start}" AND "{date_end}"
"""


ESR_QUERY = """
SELECT
    date,
    org_id,
    object_id AS device_id,
    time,
    value.int_value AS state,
    value.is_start,
    value.is_end,
    value.is_databreak
FROM {kinesisstats_history}.osdenginestatelogonly
WHERE date BETWEEN "{date_start}" AND "{date_end}"
"""




@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Engine state comparison between legacy and ESR engine state implementations",
        row_meaning="Time windows showing legacy vs ESR engine state values for comparison analysis",
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
        AnyUpstream(KinesisStatsHistory.OSD_ENGINE_STATE),
        AnyUpstream(KinesisStatsHistory.OSD_ENGINE_STATE_LOG_ONLY),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_ENGINE_STATE_COMPARISON.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_KEYS,
        block_before_write=True,
    ),
)
def fct_enginestate_comparison(context: AssetExecutionContext) -> DataFrame:
    context.log.info(PRIMARY_KEYS)

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    legacy_df = spark.sql(format_date_partition_query(LEGACY_QUERY, context))
    esr_df = spark.sql(format_date_partition_query(ESR_QUERY, context))

    result_df = interpolate_state_data(legacy_df, "prod_state", esr_df, "log_only_state")

    # Add missing columns as NULL values
    return (
        result_df
        .withColumn("bus_id", F.lit(None).cast("long"))
        .withColumn("request_id", F.lit(None).cast("long"))
        .withColumn("response_id", F.lit(None).cast("long"))
        .withColumn("obd_value", F.lit(None).cast("long"))
    )
