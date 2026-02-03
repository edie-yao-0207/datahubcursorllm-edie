"""
CAN Frames Asset

This asset extracts CAN frames from sensor replay traces stored in S3.
CAN frames are decoded from protobuf messagebus files and stored as individual rows.
"""

import os
import time
from dagster import AssetExecutionContext
from dataweb import table, NonNullDQCheck, PrimaryKeyDQCheck
from dataweb._core.utils import initialize_datadog, send_datadog_metrics, DatadogMetric, get_run_env
from dataweb.userpkgs.constants import (
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    AWSRegion,
    Database,
    DQCheckMode,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataclasses import replace
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_non_null_columns,
    get_primary_keys,
)
from dataweb.userpkgs.firmware.table import Dynamodb, ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.firmware.worker.fct_sampled_can_frames_worker import wrap_worker_processing, FINAL_SCHEMA, WorkerMetrics

from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.utils import build_table_description, partition_key_ranges_from_context

from typing import List

# Spark imports for dataweb framework
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Define CAN_ID column for reuse across CAN frame tables
CAN_ID = Column(
    name="id",
    type=DataType.LONG,
    nullable=False,
    metadata=Metadata(comment="CAN message identifier (numeric)."),
)

COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    ColumnType.TIMESTAMP_UNIX_US,
    replace(ColumnType.SOURCE_INTERFACE.value, nullable=False, primary_key=True),
    replace(CAN_ID, primary_key=True),
    replace(ColumnType.PAYLOAD.value, nullable=False, primary_key=True),
    replace(ColumnType.DIRECTION.value, nullable=False, primary_key=True),
    Column(
        name="trace_uuid",
        type=DataType.STRING,
        nullable=False,
        primary_key=True,
        metadata=Metadata(comment="UUID of the sensor replay trace containing this frame."),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

WORKER_INPUT_SCHEMA = StructType([
    StructField("TraceUuid", StringType(), False),
    StructField("SourceOrgId", StringType(), False),
    StructField("SourceDeviceId", StringType(), False),
    StructField("conversion_date", StringType(), False),
])

@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="This table contains individual CAN frames extracted from sensor replay traces. "
            "Each row represents a single CAN frame with its raw payload data, timing information, "
            "and metadata. CAN frames are decoded from protobuf messagebus files stored in S3 "
            "and linked to their originating trace and device.",
        row_meaning="Each row represents a single CAN frame from a device's CAN bus during a sensor replay trace.",
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],  # CAN recording only enabled in the US currently
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(Dynamodb.SENSOR_REPLAY_TRACES),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=[
        # specifying checks manually here to remove the NonEmptyDQCheck since it is possible
        # to have no traces converted on the partition date.
        NonNullDQCheck(
            name=f"dq_non_null_{ProductAnalyticsStaging.FCT_SAMPLED_CAN_FRAMES.value}",
            non_null_columns=PRIMARY_KEYS,
            block_before_write=True,
        ),
        PrimaryKeyDQCheck(
            name=f"dq_primary_key_{ProductAnalyticsStaging.FCT_SAMPLED_CAN_FRAMES.value}",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
    ],
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)

def fct_sampled_can_frames(context: AssetExecutionContext) -> DataFrame:
    """
    Extract CAN frames from sensor replay traces.

    This asset processes sensor replay traces from S3, decodes CAN frames from
    protobuf messagebus files, and stores them as individual rows.

    Args:
        context: Dagster asset execution context with partition information

    Returns:
        Spark DataFrame containing CAN frames for the current partition
    """

    # Initialize Spark session
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    # Helper function to return empty Spark DataFrame with proper schema
    def get_empty_dataframe():
        return spark.createDataFrame([], FINAL_SCHEMA)

    # Get the partition date
    partition_keys = partition_key_ranges_from_context(context)[0]
    date_start = partition_keys[0]
    date_end = partition_keys[-1]

    # Query DynamoDB sensor_replay_traces for traces converted on partition_date
    context.log.info(f"ℹ️ QUERYING dynamodb.sensor_replay_traces for traces converted between {date_start} and {date_end}")

    try:
        traces = get_sensor_replay_traces(spark, context)
        context.log.info(f"ℹ️ Found {len(traces)} traces converted between {date_start} and {date_end}")

        if not traces:
            context.log.warning(f"⚠️ No traces converted between {date_start} and {date_end}")
            return get_empty_dataframe()

    except Exception as e:
        context.log.error(f"❌ Failed to query DynamoDB: {e}")
        return get_empty_dataframe()

    # determine the number of worker partitions depending on the number of traces
    MAX_TRACES_PER_WORKER = 5
    num_traces = len(traces)
    num_partitions = num_traces // MAX_TRACES_PER_WORKER + 1

    traces_df = spark.createDataFrame(traces, schema=WORKER_INPUT_SCHEMA).repartition(num_partitions, 'TraceUuid')

    context.log.info(f"🚀 Starting worker processing with {num_traces} traces and {num_partitions} partitions")
    results_df, metrics = wrap_worker_processing(context, traces_df)

    now = time.time()
    datadog_metrics = []

    if metrics.frames_from_messagebus > 0:
        datadog_metrics.append(
            DatadogMetric(
                metric="dataweb.fct_sampled_can_frames.frame_count",
                type="count",
                points=[(now, float(metrics.frames_from_messagebus))],
                tags=["source:messagebus"],
            )
        )

    if metrics.traces_with_messagebus > 0:
        datadog_metrics.append(
            DatadogMetric(
                metric="dataweb.fct_sampled_can_frames.trace_count",
                type="count",
                points=[(now, float(metrics.traces_with_messagebus))],
                tags=["source:messagebus"],
            )
        )

    if metrics.frames_from_telemetry > 0:
        datadog_metrics.append(
            DatadogMetric(
                metric="dataweb.fct_sampled_can_frames.frame_count",
                type="count",
                points=[(now, float(metrics.frames_from_telemetry))],
                tags=["source:telemetry"],
            )
        )

    if metrics.traces_with_telemetry > 0:
        datadog_metrics.append(
            DatadogMetric(
                metric="dataweb.fct_sampled_can_frames.trace_count",
                type="count",
                points=[(now, float(metrics.traces_with_telemetry))],
                tags=["source:telemetry"],
            )
        )

    if metrics.traces_with_none > 0:
        datadog_metrics.append(
            DatadogMetric(
                metric="dataweb.fct_sampled_can_frames.trace_count",
                type="count",
                points=[(now, float(metrics.traces_with_none))],
                tags=["source:none"],
            )
        )

    context.log.info("📊 Datadog metrics:")
    for metric in datadog_metrics:
        context.log.info(
            f"  - {metric.metric}: {metric.points[0][1]} "
            f"(tags: {', '.join(metric.tags)})"
        )

    if get_run_env() == "prod":
        # Send to Datadog in production
        try:
            initialize_datadog()
            send_datadog_metrics(context, metrics=datadog_metrics)
            context.log.info("✅ Successfully sent metrics to Datadog")
        except Exception as e:
            # Don't fail the asset if metrics fail
            context.log.warning(f"⚠️ Failed to send Datadog metrics: {e}")

    return results_df


def get_sensor_replay_traces(spark: SparkSession, context: AssetExecutionContext) -> List[dict]:
    """
    Query DynamoDB sensor_replay_traces for traces converted on the partition date.

    This function implements the incremental processing strategy by finding traces
    that were converted on the specified partition date using ConvertedAtMs.

    Args:
        spark: Spark session for executing SQL queries
        partition_date: Date string in YYYY-MM-DD format

    Returns:
        List of dictionaries containing trace metadata with keys:
        - TraceUuid: UUID of the trace
        - SourceOrgId: Organization ID
        - SourceDeviceId: Device ID
        - ConvertedAtMs: Timestamp when trace was converted
        - AvailableSensorTypes: Available sensor types
        - Description: Trace description
    """

    # Define the SQL query based on the implementation plan
    QUERY = """
    WITH
    sensor_replay_can_library_records AS (
      SELECT srt.TraceUuid
      FROM dynamodb.sensor_replay_traces srt
      WHERE srt.ItemTypeAndIdentifiers = 'collection#library#CAN'
    ),
    can_library_records AS (
      SELECT
        srt.SourceOrgId, srt.SourceDeviceId, srt.SourceAssetMs,
        srt.DurationMs, srt.ConvertedAtMs, srt.AvailableSensorTypes,
        srt.Description, srt.TraceUuid,
        -- Convert ConvertedAtMs to date for daily partitioning
        DATE_FORMAT(DATE(FROM_UNIXTIME(CAST(srt.ConvertedAtMs AS BIGINT) / 1000)), 'yyyy-MM-dd') as conversion_date
      FROM dynamodb.sensor_replay_traces srt
      WHERE srt.ItemTypeAndIdentifiers = 'info'
      AND srt.TraceUuid IN (SELECT TraceUuid FROM sensor_replay_can_library_records)
      AND srt.ConvertedAtMs != "0"  -- Only process converted traces
    )
    SELECT *
    FROM can_library_records
    WHERE conversion_date BETWEEN "{date_start}" AND "{date_end}"
    """

    formatted_query = format_date_partition_query(QUERY, context)
    try:
        # Execute the query
        df = spark.sql(formatted_query)

        # Convert Spark DataFrame directly to list of dictionaries
        traces = [row.asDict() for row in df.collect()]

        return traces

    except Exception as e:
        raise Exception(f"Failed to query DynamoDB sensor_replay_traces: {e}")



