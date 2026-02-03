"""
Engine state processing utilities for firmware data analysis.

This module provides reusable functions for processing engine state data,
including state interpolation and state knowledge determination.
"""

from dataclasses import dataclass, field
from typing import List
from dagster import AssetExecutionContext
from pyspark.sql import DataFrame, Window, SparkSession
import pyspark.sql.functions as F

from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    columns_to_names,
)
from dataweb.userpkgs.utils import schema_to_columns_with_property

# Time constants
MILLISECONDS_IN_SECOND = 1000
MILLISECONDS_IN_MINUTE = 60 * MILLISECONDS_IN_SECOND

MIN_SEGMENT_DURATION_MINUTES = 5
MIN_SEGMENT_DURATION_MS = MIN_SEGMENT_DURATION_MINUTES * MILLISECONDS_IN_MINUTE


@dataclass
class SignalSettings:
    query: str
    """Query must return the following columns:
    - date
    - org_id
    - device_id
    - time
    - value

    No need to filter by date - this filter will be added when the query is executed.
    """

    label: str
    """Column label for the signal (will be prefixed with min/max/avg)"""

    buffer_time_ms: int = field(default=0)
    """Time before or after an engine state transition where this signal will be ignored."""


SIGNAL_SETTINGS = [
    SignalSettings(
        query="""
            SELECT
                date,
                org_id,
                device_id,
                time,
                value.gps_speed_meters_per_second AS value
            FROM {kinesisstats_history}.location
            WHERE date BETWEEN '{date_start}' AND '{date_end}'
                AND value.gps_speed_meters_per_second > 0
                AND value.accuracy_millimeters IS NOT NULL
                AND value.accuracy_millimeters <= 10000
        """,
        label="gps_speed",
        buffer_time_ms=15 * MILLISECONDS_IN_SECOND,
    ),
    SignalSettings(
        query="""
            SELECT
                date,
                org_id,
                object_id AS device_id,
                time,
                value.proto_value.engine_gauge_event.engine_rpm AS value
            FROM {kinesisstats_history}.osdenginegauge
            WHERE date BETWEEN '{date_start}' AND '{date_end}'
              AND NOT value.is_end
              AND NOT value.is_databreak
        """,
        label="engine_rpm",
        buffer_time_ms=3 * MILLISECONDS_IN_SECOND,
    ),
    SignalSettings(
        query="""
            SELECT
                date,
                org_id,
                object_id AS device_id,
                time,
                value.int_value AS value
            FROM {kinesisstats_history}.osdengineseconds
            WHERE date BETWEEN '{date_start}' AND '{date_end}'
              AND NOT value.is_end
              AND NOT value.is_databreak
        """,
        label="engine_seconds",
        buffer_time_ms=3 * MILLISECONDS_IN_SECOND,
    ),
    SignalSettings(
        query="""
            SELECT
                date,
                org_id,
                object_id AS device_id,
                time,
                value.int_value AS value
            FROM {kinesisstats_history}.osdenginemilliknots
            WHERE date BETWEEN '{date_start}' AND '{date_end}'
              AND NOT value.is_end
              AND NOT value.is_databreak
        """,
        label="engine_milliknots",
        buffer_time_ms=3 * MILLISECONDS_IN_SECOND,
    ),
]


# Schema definitions for engine state segment metrics tables
PRIMARY_KEY_COLS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    Column(
        name="time",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="Start time in ms of the engine state segment."),
    ),
]


def _columns_from_signal_settings(signal_settings: List[SignalSettings]):
    """Generate schema columns from signal settings for min/max/avg metrics."""
    cols = []
    for settings in signal_settings:
        cols.extend(
            [
                Column(
                    name=f"min_{settings.label}",
                    type=DataType.DOUBLE,
                    nullable=True,
                    metadata=Metadata(
                        comment=f"Minimum value of {settings.label} during the segment"
                    ),
                ),
                Column(
                    name=f"max_{settings.label}",
                    type=DataType.DOUBLE,
                    nullable=True,
                    metadata=Metadata(
                        comment=f"Maximum value of {settings.label} during the segment"
                    ),
                ),
                Column(
                    name=f"avg_{settings.label}",
                    type=DataType.DOUBLE,
                    nullable=True,
                    metadata=Metadata(
                        comment=f"Average value of {settings.label} during the segment"
                    ),
                ),
            ]
        )
    return cols


# Complete schema for engine state segment metrics tables
ENGINE_STATE_SEGMENT_METRICS_SCHEMA = columns_to_schema(
    *PRIMARY_KEY_COLS,
    ColumnType.END_TIME,
    Column(
        name="engine_state",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="State of the engine during the segment (see osDEngineState for values)."
        ),
    ),
    *_columns_from_signal_settings(SIGNAL_SETTINGS),
)

ENGINE_STATE_SEGMENT_METRICS_PRIMARY_KEYS = columns_to_names(*PRIMARY_KEY_COLS)
ENGINE_STATE_SEGMENT_METRICS_NON_NULL_COLUMNS = schema_to_columns_with_property(
    ENGINE_STATE_SEGMENT_METRICS_SCHEMA, "nullable"
)


def get_is_state_known(df: DataFrame) -> DataFrame:
    """
    Adds information about whether the state of a stateful value is known. This is based off of
    the fields in a standard integer objectStat.

    Required columns in input DataFrame:
    - state: State at a given time.
    - is_start: Whether or not the state is a start event (dropped in output).
    - is_end: Whether or not the state is an end event (dropped in output).
    - is_databreak: Whether or not the state is a data break event (dropped in output).

    Returns additional columns:
    - is_state_known: Whether or not the state is a known value between time and end_time.
    """
    window = Window.partitionBy("org_id", "device_id").orderBy("time")

    return (
        df.withColumn("next_is_databreak", F.lead(F.expr("is_databreak")).over(window))
        .withColumn(
            "is_state_known",
            F.expr("NOT is_end AND NOT is_databreak AND NOT next_is_databreak"),
        )
        .drop("next_is_databreak")
        .drop("is_start")
        .drop("is_end")
        .drop("is_databreak")
    )


def interpolate_state_data(
    df1: DataFrame,
    state_col_a: str,
    df2: DataFrame,
    state_col_b: str,
    remove_databreaks: bool = True,
) -> DataFrame:
    """Interpolates the state of column A when column B changes and vice versa. Columns A and
    B in the dataframe should be integer columns containing discrete states.

    By ensuring we have state values at each datapoint in A and B, we can determine when and
    for how long the states of A and B are different.

    For example:
    A:  0 ------- 1 ----------------
    B:  1 -- 0 ----------- 1 -------

    Would return:
    A:  0 -- 0 -- 1 ------ 1 -------
    B:  1 -- 0 -- 0 ------ 1 -------

    Required columns in input DataFrames:
    - All columns from get_is_state_known

    Returns Dataframe with:
    - State of columns A and B during time segments where both are known.
    - Time and end time of each segment.
    """

    window = Window.partitionBy("org_id", "device_id").orderBy("time")

    df1_renamed = (
        get_is_state_known(df1)
        .withColumnRenamed("state", "state_a")
        .withColumnRenamed("is_state_known", "is_a_known")
    )

    df2_renamed = (
        get_is_state_known(df2)
        .withColumnRenamed("state", "state_b")
        .withColumnRenamed("is_state_known", "is_b_known")
    )

    df = df1_renamed.join(
        df2_renamed, on=["date", "org_id", "device_id", "time"], how="full"
    )

    df = (
        df
        # Forward-fill data so we have a value for each time entry in A and B
        .withColumn("a_filled", F.last("state_a", ignorenulls=True).over(window))
        .withColumn(
            "is_a_known_filled", F.last("is_a_known", ignorenulls=True).over(window)
        )
        .withColumn("b_filled", F.last("state_b", ignorenulls=True).over(window))
        .withColumn(
            "is_b_known_filled", F.last("is_b_known", ignorenulls=True).over(window)
        )
        .withColumn("end_time", F.lead("time").over(window))
    )

    # Remove cases where either state of A or B is unknown
    if remove_databreaks:
        df = df.where("is_a_known_filled AND is_b_known_filled")

    return (
        df.select(
            "date",
            "org_id",
            "device_id",
            "time",
            "end_time",
            "a_filled",
            "b_filled",
            "is_a_known_filled",
            "is_b_known_filled",
        )
        .withColumnRenamed("a_filled", state_col_a)
        .withColumnRenamed("b_filled", state_col_b)
        .drop("is_a_known_filled")
        .drop("is_b_known_filled")
    )


ENGINE_STATE_QUERY = """
SELECT
    date,
    org_id,
    object_id AS device_id,
    time,
    value.int_value AS state,
    value.is_start,
    value.is_end,
    value.is_databreak
FROM {kinesisstats_history}.{table_name}
WHERE date BETWEEN '{date_start}' AND '{date_end}'
"""

POWER_STATE_QUERY = """
SELECT
    date,
    org_id,
    object_id AS device_id,
    time,
    value.int_value AS state,
    value.is_start,
    value.is_end,
    value.is_databreak
FROM {kinesisstats_history}.{table_name}
WHERE date BETWEEN '{date_start}' AND '{date_end}'
"""


def get_engine_state_segment_stats(
    context: AssetExecutionContext,
    spark: SparkSession,
    engine_state_table: str,
    power_state_table: str,
) -> DataFrame:
    """Generate engine state segment metrics by combining engine state, power state, and signal data.

    This function creates time segments based on engine state and power state transitions,
    then aggregates various vehicle signals (GPS speed, engine RPM, engine seconds, engine milli-knots)
    within each segment to provide statistical measures (min, max, avg) for analysis.

    Args:
        context: Dagster asset execution context containing partition information
        spark: SparkSession for executing SQL queries
        engine_state_table: Name of the engine state table (e.g., 'osdenginestate')
        power_state_table: Name of the power state table (e.g., 'osdpowerstate')

    Returns:
        DataFrame with columns:
        - date, org_id, device_id, time, end_time, engine_state (segment identifiers)
        - min_{signal}, max_{signal}, avg_{signal} for each signal in SIGNAL_SETTINGS
          (e.g., min_gps_speed, max_engine_rpm, avg_engine_seconds, etc.)
    """
    # Format the queries with the provided date range and table names
    engine_state_query = format_date_partition_query(
        ENGINE_STATE_QUERY,
        context=context,
        table_name=engine_state_table,
    )
    context.log.info(f"Engine state query: {engine_state_query}")

    power_state_query = format_date_partition_query(
        POWER_STATE_QUERY,
        context=context,
        table_name=power_state_table,
    )
    context.log.info(f"Power state query: {power_state_query}")

    engine_state_segments = (
        interpolate_state_data(
            spark.sql(engine_state_query),
            "engine_state",
            spark.sql(power_state_query),
            "power_state",
            remove_databreaks=False,
        )
        .where(f"end_time - time >= {MIN_SEGMENT_DURATION_MS}")
        .where("power_state == 1")  # power state is ON
        .where(
            "engine_state IS NOT NULL"
        )  # we sometimes get engine state NULL when power state shows up before engine state
        .drop("power_state")
        .repartition("date", "org_id", "device_id")
        .sortWithinPartitions("date", "org_id", "device_id", "time")
        .cache()
    )

    final_df = engine_state_segments
    for signal_setting in SIGNAL_SETTINGS:
        query = format_date_partition_query(signal_setting.query, context)
        context.log.info(f"Signal query: {query}")
        df = spark.sql(query)

        joined = (
            engine_state_segments.hint("range_join", MILLISECONDS_IN_MINUTE * 60)
            .join(
                df.alias("signal"),
                (engine_state_segments["date"] == df["date"]) &
                (engine_state_segments["org_id"] == df["org_id"]) &
                (engine_state_segments["device_id"] == df["device_id"]) &
                (df["time"] > engine_state_segments["time"] + signal_setting.buffer_time_ms) &
                (df["time"] < engine_state_segments["end_time"] - signal_setting.buffer_time_ms),
                how="inner"
            )
            .sortWithinPartitions("signal.date", "signal.org_id", "signal.device_id", "signal.time")
            .groupBy(
                engine_state_segments["date"],
                engine_state_segments["org_id"],
                engine_state_segments["device_id"],
                engine_state_segments["time"],
                engine_state_segments["end_time"],
                engine_state_segments["engine_state"],
            )
            .agg(
                F.min("value").cast("double").alias(f"min_{signal_setting.label}"),
                F.max("value").cast("double").alias(f"max_{signal_setting.label}"),
                F.avg("value").cast("double").alias(f"avg_{signal_setting.label}"),
            )
        )

        final_df = final_df.join(
            joined,
            ["date", "org_id", "device_id", "time", "end_time", "engine_state"],
            how="left",
        )

    return final_df
