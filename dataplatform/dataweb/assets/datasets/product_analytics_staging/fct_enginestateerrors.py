from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from dagster import AssetExecutionContext
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.firmware.metric import StrEnum
from dataweb import table, build_general_dq_checks, get_databases
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
    DataModelTelematics,
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
from dataweb.userpkgs.firmware.metric import Metric, MetricEnum, StrEnum
from dataweb.userpkgs.query import format_date_partition_query

FIVE_MINUTES_MS = 5 * 60 * 1000

ERROR_COUNTS_QUERY = """
WITH signals AS (
  SELECT
    *,
    -- Use count of 5 minute segments as this approximates measurements from the original EPD algorithm.
    FLOOR((end_time - time) / {five_minutes_ms}) AS segment_count,
    end_time - time AS segment_duration_ms,
    -- Vehicle considered moving if speed > 5 kph
    COALESCE(max_gps_speed, 0) > 2.78 OR COALESCE(max_engine_milliknots, 0) > 540 AS vehicle_moving,
    (
      COALESCE(max_engine_rpm, 0) > 0
      OR COALESCE(max_engine_milliknots, 0) > 0
      OR COALESCE(max_engine_seconds, 0) - COALESCE(min_engine_seconds, 0) > 0
    )
    AS signals_indicate_engine_running
  FROM {product_analytics_staging}.{table_name}
  WHERE
    date BETWEEN '{date_start}' AND '{date_end}'
), canConnectedCounts AS (
  SELECT
    date
    , org_id
    , object_id
    , count_if(value.int_value == 2) as can_connected_count
  FROM {kinesisstats_history}.osdcanconnected
  GROUP BY ALL
), totals AS (
SELECT
  s.date
  , s.org_id
  , s.device_id
  , sum(CASE WHEN engine_state == 0 AND (vehicle_moving OR signals_indicate_engine_running) THEN segment_count ELSE 0 END) AS count_engine_off_errors
  , sum(CASE WHEN engine_state == 0 AND (vehicle_moving OR signals_indicate_engine_running) THEN segment_duration_ms ELSE 0 END) AS duration_engine_off_errors
  , sum(CASE WHEN engine_state == 1 AND NOT signals_indicate_engine_running AND NOT vehicle_moving THEN segment_count ELSE 0 END) AS count_engine_on_errors
  , sum(CASE WHEN engine_state == 1 AND NOT signals_indicate_engine_running AND NOT vehicle_moving THEN segment_duration_ms ELSE 0 END) AS duration_engine_on_errors
  , sum(CASE WHEN engine_state == 1 AND NOT vehicle_moving AND signals_indicate_engine_running THEN segment_count ELSE 0 END) AS count_missed_idle_errors
  , sum(CASE WHEN engine_state == 1 AND NOT vehicle_moving AND signals_indicate_engine_running THEN segment_duration_ms ELSE 0 END) AS duration_missed_idle_errors
  , sum(CASE WHEN engine_state == 2 AND vehicle_moving THEN segment_count ELSE 0 END) AS count_engine_idle_errors
  , sum(CASE WHEN engine_state == 2 AND vehicle_moving THEN segment_duration_ms ELSE 0 END) AS duration_engine_idle_errors

FROM signals s
GROUP BY s.date, s.org_id, s.device_id
)
SELECT
  t.date
  , t.org_id
  , t.device_id
  , t.count_engine_off_errors AS count_engine_off_errors{suffix}
  , COALESCE(t.duration_engine_off_errors / (trips.total_duration_mins * 60000), 0) AS percent_engine_off_errors{suffix}
  , t.count_engine_on_errors AS count_engine_on_errors{suffix}
  , COALESCE(t.duration_engine_on_errors / (trips.total_duration_mins * 60000), 0) AS percent_engine_on_errors{suffix}
  , t.count_missed_idle_errors AS count_missed_idle_errors{suffix}
  , COALESCE(t.duration_missed_idle_errors / (trips.total_duration_mins * 60000), 0) AS percent_missed_idle_errors{suffix}
  , t.count_engine_idle_errors AS count_engine_idle_errors{suffix}
  , COALESCE(t.duration_engine_idle_errors / (trips.total_duration_mins * 60000), 0) AS percent_engine_idle_errors{suffix}
FROM totals t
JOIN canConnectedCounts c
  ON t.org_id = c.org_id AND t.device_id = c.object_id and t.date = c.date
JOIN datamodel_telematics.fct_trips_daily trips
  USING (date, org_id, device_id)
WHERE
    -- Filter out any devices which haven't connected to CAN - if we don't have diagnostics then we won't have engine state data.
    c.can_connected_count > 0
    -- Use GPS-based trips to get total drive time
    AND trips.trip_type == 'location_based'
"""

class TableDimension(StrEnum):
    COUNT_ENGINE_OFF_ERRORS = "count_engine_off_errors"
    COUNT_ENGINE_ON_ERRORS = "count_engine_on_errors"
    COUNT_ENGINE_IDLE_ERRORS = "count_engine_idle_errors"
    COUNT_MISSED_IDLE_ERRORS = "count_missed_idle_errors"
    COUNT_ENGINE_OFF_ERRORS_LOG_ONLY = "count_engine_off_errors_log_only"
    COUNT_ENGINE_ON_ERRORS_LOG_ONLY = "count_engine_on_errors_log_only"
    COUNT_ENGINE_IDLE_ERRORS_LOG_ONLY = "count_engine_idle_errors_log_only"
    COUNT_MISSED_IDLE_ERRORS_LOG_ONLY = "count_missed_idle_errors_log_only"
    PERCENT_ENGINE_OFF_ERRORS = "percent_engine_off_errors"
    PERCENT_ENGINE_ON_ERRORS = "percent_engine_on_errors"
    PERCENT_ENGINE_IDLE_ERRORS = "percent_engine_idle_errors"
    PERCENT_MISSED_IDLE_ERRORS = "percent_missed_idle_errors"
    PERCENT_ENGINE_OFF_ERRORS_LOG_ONLY = "percent_engine_off_errors_log_only"
    PERCENT_ENGINE_ON_ERRORS_LOG_ONLY = "percent_engine_on_errors_log_only"
    PERCENT_ENGINE_IDLE_ERRORS_LOG_ONLY = "percent_engine_idle_errors_log_only"
    PERCENT_MISSED_IDLE_ERRORS_LOG_ONLY = "percent_missed_idle_errors_log_only"

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
        name=TableDimension.COUNT_ENGINE_OFF_ERRORS,
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Number of 5-minute segments where engine is incorrectly reported as OFF but should be ON.",
        )
    ),
    Column(
        name=TableDimension.COUNT_ENGINE_ON_ERRORS,
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Number of 5-minute segments where engine is incorrectly reported as ON but should be OFF.",
        )
    ),
    Column(
        name=TableDimension.COUNT_ENGINE_IDLE_ERRORS,
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Number of 5-minute segments where engine is incorrectly reported as IDLE but should be ON.",
        )
    ),
    Column(
        name=TableDimension.COUNT_MISSED_IDLE_ERRORS,
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Number of 5-minute segments where engine is incorrectly reported as ON but should be IDLE.",
        )
    ),
    Column(
        name=TableDimension.COUNT_ENGINE_OFF_ERRORS_LOG_ONLY,
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Number of 5-minute segments where engine is incorrectly reported as OFF but should be ON (log only data).",
        )
    ),
    Column(
        name=TableDimension.COUNT_ENGINE_ON_ERRORS_LOG_ONLY,
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Number of 5-minute segments where engine is incorrectly reported as ON but should be OFF (log only data).",
        )
    ),
    Column(
        name=TableDimension.COUNT_ENGINE_IDLE_ERRORS_LOG_ONLY,
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Number of 5-minute segments where engine is incorrectly reported as IDLE but should be ON (log only data).",
        )
    ),
    Column(
        name=TableDimension.COUNT_MISSED_IDLE_ERRORS_LOG_ONLY,
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Number of 5-minute segments where engine is incorrectly reported as ON but should be IDLE (log only data).",
        )
    ),
    Column(
        name=TableDimension.PERCENT_ENGINE_OFF_ERRORS,
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Percentage of total trip duration where engine is incorrectly reported as OFF but should be ON.",
        )
    ),
    Column(
        name=TableDimension.PERCENT_ENGINE_ON_ERRORS,
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Percentage of total trip duration where engine is incorrectly reported as ON but should be OFF.",
        )
    ),
    Column(
        name=TableDimension.PERCENT_ENGINE_IDLE_ERRORS,
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Percentage of total trip duration where engine is incorrectly reported as IDLE but should be ON.",
        )
    ),
    Column(
        name=TableDimension.PERCENT_MISSED_IDLE_ERRORS,
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Percentage of total trip duration where engine is incorrectly reported as ON but should be IDLE.",
        )
    ),
    Column(
        name=TableDimension.PERCENT_ENGINE_OFF_ERRORS_LOG_ONLY,
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Percentage of total trip duration where engine is incorrectly reported as OFF but should be ON (log only data).",
        )
    ),
    Column(
        name=TableDimension.PERCENT_ENGINE_ON_ERRORS_LOG_ONLY,
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Percentage of total trip duration where engine is incorrectly reported as ON but should be OFF (log only data).",
        )
    ),
    Column(
        name=TableDimension.PERCENT_ENGINE_IDLE_ERRORS_LOG_ONLY,
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Percentage of total trip duration where engine is incorrectly reported as IDLE but should be ON (log only data).",
        )
    ),
    Column(
        name=TableDimension.PERCENT_MISSED_IDLE_ERRORS_LOG_ONLY,
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Percentage of total trip duration where engine is incorrectly reported as ON but should be IDLE (log only data).",
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
    COUNT_ENGINE_ON_ERRORS = Metric(
        type=ProductAnalyticsStaging.FCT_ENGINE_STATE_ERRORS,
        field=TableDimension.COUNT_ENGINE_ON_ERRORS,
        label="engine_on_errors"
    )
    COUNT_ENGINE_OFF_ERRORS = Metric(
        type=ProductAnalyticsStaging.FCT_ENGINE_STATE_ERRORS,
        field=TableDimension.COUNT_ENGINE_OFF_ERRORS,
        label="engine_off_errors"
    )
    COUNT_ENGINE_IDLE_ERRORS = Metric(
        type=ProductAnalyticsStaging.FCT_ENGINE_STATE_ERRORS,
        field=TableDimension.COUNT_ENGINE_IDLE_ERRORS,
        label="engine_idle_errors"
    )
    COUNT_MISSED_IDLE_ERRORS = Metric(
        type=ProductAnalyticsStaging.FCT_ENGINE_STATE_ERRORS,
        field=TableDimension.COUNT_MISSED_IDLE_ERRORS,
        label="engine_missed_idle_errors"
    )
    COUNT_ENGINE_ON_ERRORS_LOG_ONLY = Metric(
        type=ProductAnalyticsStaging.FCT_ENGINE_STATE_ERRORS,
        field=TableDimension.COUNT_ENGINE_ON_ERRORS_LOG_ONLY,
        label="engine_on_errors_log_only"
    )
    COUNT_ENGINE_OFF_ERRORS_LOG_ONLY = Metric(
        type=ProductAnalyticsStaging.FCT_ENGINE_STATE_ERRORS,
        field=TableDimension.COUNT_ENGINE_OFF_ERRORS_LOG_ONLY,
        label="engine_off_errors_log_only"
    )
    COUNT_ENGINE_IDLE_ERRORS_LOG_ONLY = Metric(
        type=ProductAnalyticsStaging.FCT_ENGINE_STATE_ERRORS,
        field=TableDimension.COUNT_ENGINE_IDLE_ERRORS_LOG_ONLY,
        label="engine_idle_errors_log_only"
    )
    COUNT_MISSED_IDLE_ERRORS_LOG_ONLY = Metric(
        type=ProductAnalyticsStaging.FCT_ENGINE_STATE_ERRORS,
        field=TableDimension.COUNT_MISSED_IDLE_ERRORS_LOG_ONLY,
        label="engine_missed_idle_errors_log_only"
    )
    PERCENT_ENGINE_OFF_ERRORS = Metric(
        type=ProductAnalyticsStaging.FCT_ENGINE_STATE_ERRORS,
        field=TableDimension.PERCENT_ENGINE_OFF_ERRORS,
        label="percent_engine_off_errors"
    )
    PERCENT_ENGINE_ON_ERRORS = Metric(
        type=ProductAnalyticsStaging.FCT_ENGINE_STATE_ERRORS,
        field=TableDimension.PERCENT_ENGINE_ON_ERRORS,
        label="percent_engine_on_errors"
    )
    PERCENT_ENGINE_IDLE_ERRORS = Metric(
        type=ProductAnalyticsStaging.FCT_ENGINE_STATE_ERRORS,
        field=TableDimension.PERCENT_ENGINE_IDLE_ERRORS,
        label="percent_engine_idle_errors"
    )
    PERCENT_MISSED_IDLE_ERRORS = Metric(
        type=ProductAnalyticsStaging.FCT_ENGINE_STATE_ERRORS,
        field=TableDimension.PERCENT_MISSED_IDLE_ERRORS,
        label="percent_missed_idle_errors"
    )
    PERCENT_ENGINE_OFF_ERRORS_LOG_ONLY = Metric(
        type=ProductAnalyticsStaging.FCT_ENGINE_STATE_ERRORS,
        field=TableDimension.PERCENT_ENGINE_OFF_ERRORS_LOG_ONLY,
        label="percent_engine_off_errors_log_only"
    )
    PERCENT_ENGINE_ON_ERRORS_LOG_ONLY = Metric(
        type=ProductAnalyticsStaging.FCT_ENGINE_STATE_ERRORS,
        field=TableDimension.PERCENT_ENGINE_ON_ERRORS_LOG_ONLY,
        label="percent_engine_on_errors_log_only"
    )
    PERCENT_ENGINE_IDLE_ERRORS_LOG_ONLY = Metric(
        type=ProductAnalyticsStaging.FCT_ENGINE_STATE_ERRORS,
        field=TableDimension.PERCENT_ENGINE_IDLE_ERRORS_LOG_ONLY,
        label="percent_engine_idle_errors_log_only"
    )
    PERCENT_MISSED_IDLE_ERRORS_LOG_ONLY = Metric(
        type=ProductAnalyticsStaging.FCT_ENGINE_STATE_ERRORS,
        field=TableDimension.PERCENT_MISSED_IDLE_ERRORS_LOG_ONLY,
        label="percent_missed_idle_errors_log_only"
    )


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""
        This table analyzes signals during segments of time in which osDEngineState is reported in order to
        determine the accuracy of our engine state algorithm.
        """.strip(),
        row_meaning="""
        Each row represents a 2-minute segment of time with:
        - Reported engine state (interpolated based on osDEngineState)
        - Signals (e.g. engine RPM, speed) emitted during this time segment.
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
        AnyUpstream(ProductAnalyticsStaging.FCT_ENGINE_SEGMENT_METRICS_PROD),
        AnyUpstream(ProductAnalyticsStaging.FCT_ENGINE_SEGMENT_METRICS_LOG_ONLY),
        AnyUpstream(KinesisStatsHistory.OSD_CAN_CONNECTED),
        AnyUpstream(DataModelTelematics.FCT_TRIPS_DAILY),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=5,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_ENGINE_STATE_ERRORS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def fct_enginestateerrors(context: AssetExecutionContext) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    prod_query = format_date_partition_query(
        ERROR_COUNTS_QUERY,
        context,
        table_name="fct_enginestate_segment_metrics_prod",
        five_minutes_ms=FIVE_MINUTES_MS,
        suffix="",
    )

    log_only_query = format_date_partition_query(
        ERROR_COUNTS_QUERY,
        context,
        table_name="fct_enginestate_segment_metrics_log_only",
        five_minutes_ms=FIVE_MINUTES_MS,
        suffix="_log_only",
    )

    context.log.info(f"Prod query: {prod_query}")
    context.log.info(f"Log only query: {log_only_query}")

    return (
        spark.sql(prod_query)
        .join(spark.sql(log_only_query), ['date', 'org_id', 'device_id'], 'left')
        # Add missing columns required by schema (these are irrelevant but required downstream)
        .withColumn("time", F.lit(None).cast("long"))
        .withColumn("bus_id", F.lit(None).cast("long"))
        .withColumn("request_id", F.lit(None).cast("long"))
        .withColumn("response_id", F.lit(None).cast("long"))
        .withColumn("obd_value", F.lit(None).cast("long"))
    )
