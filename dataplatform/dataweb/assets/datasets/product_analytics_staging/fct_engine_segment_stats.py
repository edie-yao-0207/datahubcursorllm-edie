from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from dagster import AssetExecutionContext
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.firmware.metric import StrEnum
from dataweb import table, build_general_dq_checks
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    columns_to_names,
)
from dataweb.userpkgs.firmware.table import (
    ProductAnalyticsStaging,
    KinesisStatsHistory,
)
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    schema_to_columns_with_property,
)
from dataweb.userpkgs.firmware.metric import Metric, MetricEnum
from dataweb.userpkgs.query import format_date_partition_query

SEGMENT_STATS_QUERY = """
WITH transitions AS (
  SELECT
    date,
    org_id,
    object_id,
    time,
    LAG((time, value.int_value)) OVER (PARTITION BY org_id, object_id ORDER BY time) AS last
  FROM {kinesisstats_history}.osdenginestate
  WHERE date BETWEEN '{date_start}' AND '{date_end}'
)
SELECT
  date,
  org_id,
  object_id AS device_id,
  time,
  CAST(NULL AS LONG) AS bus_id,
  CAST(NULL AS LONG) AS request_id,
  CAST(NULL AS LONG) AS response_id,
  CAST(NULL AS LONG) AS obd_value,
  CAST(time - last.time AS LONG) AS idle_duration_ms
FROM transitions
WHERE last.int_value = 2  -- IDLE state
"""


class TableDimension(StrEnum):
    IDLE_DURATION_MS = "idle_duration_ms"


SCHEMA = columns_to_schema(
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.TIME,
    ColumnType.DEVICE_ID,
    ColumnType.BUS_ID,
    ColumnType.REQUEST_ID,
    ColumnType.RESPONSE_ID,
    ColumnType.OBD_VALUE,
    Column(
        name=TableDimension.IDLE_DURATION_MS,
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Total duration in milliseconds that the engine was in IDLE state (value = 2) for this segment.",
        )
    ),
)

PRIMARY_KEYS = columns_to_names(
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    ColumnType.TIME,
    ColumnType.BUS_ID,
    ColumnType.REQUEST_ID,
    ColumnType.RESPONSE_ID,
    ColumnType.OBD_VALUE
)
NON_NULL_COLUMNS = schema_to_columns_with_property(SCHEMA, "nullable")


class TableMetric(MetricEnum):
    IDLE_DURATION_MS = Metric(
        type=ProductAnalyticsStaging.FCT_ENGINE_SEGMENT_STATS,
        field=TableDimension.IDLE_DURATION_MS,
        label="idle_duration_ms"
    )


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""
        This table contains engine state segment statistics for VG devices, calculated from transitions
        in the engine state data.
        """.strip(),
        row_meaning="""
        Each row represents statistics for a single segment of engine state data.
        """.strip(),
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
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=7,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_ENGINE_SEGMENT_STATS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def fct_engine_segment_stats(context: AssetExecutionContext) -> str:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    return format_date_partition_query(
        SEGMENT_STATS_QUERY,
        context,
    )

