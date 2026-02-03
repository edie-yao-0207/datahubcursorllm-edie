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
)
from dataweb.userpkgs.firmware.table import KinesisStats, ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
    schema_to_columns_with_property,
)
from dataweb.userpkgs.query import (
    format_date_partition_query,
)

SCHEMA = columns_to_schema(
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    ColumnType.BUS_ID,
    ColumnType.BUS_NAME,
    ColumnType.REQUEST_ID,
    ColumnType.HEX_REQUEST_ID,
    ColumnType.RESPONSE_ID,
    ColumnType.HEX_RESPONSE_ID,
    ColumnType.DATA_ID,
    ColumnType.HEX_DATA_ID,
    Column(
        name="request_count",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Minimum int_value of the signal seen in a day.",
        ),
    ),
    Column(
        name="any_response_count",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Maximum int_value of the signal seen in a day.",
        ),
    ),
    Column(
        name="total_derate_offset_ms",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="First int_value of the signal seen in a day.",
        ),
    ),
    Column(
        name="total_derate_instances",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Last int_value of the signal seen in a day.",
        ),
    ),
)

PRIMARY_KEYS = schema_to_columns_with_property(SCHEMA)
NON_NULL_COLUMNS = schema_to_columns_with_property(SCHEMA, "nullable")

QUERY = """

  WITH
    data AS (
      SELECT *
      FROM kinesisstats_history.osDCommandSchedulerStats
      WHERE date BETWEEN "{date_start}" AND "{date_end}"
      AND value.proto_value IS NOT NULL
      LIMIT 1000
    )

  SELECT
    date
    , org_id
    , object_id AS device_id
    , BIGINT(value.proto_value.command_scheduler_stats.bus_id) AS bus_id
    , bus_ids.name AS bus_name
    , element.command.request_id
    , HEX(element.command.request_id) AS hex_request_id
    , element.command.response_id
    , HEX(element.command.response_id) AS hex_response_id
    , element.command.data_identifier AS data_id
    , HEX(element.command.data_identifier) AS hex_data_id
    , COALESCE(SUM(element.request_response_stat.request_count), 0) AS request_count
    , COALESCE(SUM(element.request_response_stat.any_response_count), 0) AS any_response_count
    , COALESCE(SUM(element.derated_stat.latest_derate_offset_ms), 0) AS total_derate_offset_ms
    , COALESCE(SUM(element.derated_stat.overall_derate_instances), 0) AS total_derate_instances
  FROM data
  JOIN definitions.bus_ids
  ON value.proto_value.command_scheduler_stats.bus_id = bus_ids.id
  LATERAL VIEW EXPLODE(value.proto_value.command_scheduler_stats.command_stats) AS element
  GROUP BY ALL
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="This table aggregates OBD command scheduler stats into daily summaries.",
        row_meaning="Diagnostic scheduler performance over a day.",
        related_table_info={},
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(KinesisStats.OSD_COMMAND_SCHEDULER_STATS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=1,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_DAILY_OSD_COMMAND_SCHEDULER_STATS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def agg_daily_osdcommandschedulerstats(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)
