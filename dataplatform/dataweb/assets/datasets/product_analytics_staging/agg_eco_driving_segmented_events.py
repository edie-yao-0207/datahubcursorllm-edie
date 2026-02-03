"""
Daily aggregated table for Eco Driving Segmented Events from kinesisstats.

Pre-aggregates the 5 eco driving event tables at the device-day-event_type level,
providing counts and duration statistics for efficient dashboard queries.
"""

from dagster import AssetExecutionContext
from dataweb import NonEmptyDQCheck, NonNullDQCheck, TrendDQCheck, table
from dataweb.userpkgs.constants import (
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    get_all_regions,
    partition_key_ranges_from_context,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from functools import reduce

# Event type mapping: table name -> event type label
EVENT_TABLES = {
    "kinesisstats.osDEcoDrivingEventCruiseControl": "CruiseControl",
    "kinesisstats.osDEcoDrivingEventHardAcceleration": "HardAcceleration",
    "kinesisstats.osDEcoDrivingEventOverSpeed": "OverSpeed",
    "kinesisstats.osDEcoDrivingEventCoasting": "Coasting",
    "kinesisstats.osDEcoDrivingEventWearFreeBraking": "WearFreeBraking",
}

schema = [
    {
        "name": "date",
        "type": "string",
        "metadata": {"comment": "Date partition (YYYY-MM-DD)."},
    },
    {
        "name": "org_id",
        "type": "long",
        "metadata": {"comment": "Organization ID the device belongs to."},
    },
    {
        "name": "object_id",
        "type": "long",
        "metadata": {"comment": "Device/object ID that generated the events."},
    },
    {
        "name": "event_type",
        "type": "string",
        "metadata": {
            "comment": "Type of eco driving event: CruiseControl, HardAcceleration, OverSpeed, Coasting, or WearFreeBraking."
        },
    },
    {
        "name": "event_count",
        "type": "long",
        "metadata": {"comment": "Total number of events for this device/day/type."},
    },
    {
        "name": "valid_count",
        "type": "long",
        "metadata": {"comment": "Number of events with valid proto_value."},
    },
    {
        "name": "invalid_count",
        "type": "long",
        "metadata": {"comment": "Number of events with null proto_value (excluding databreaks)."},
    },
    {
        "name": "databreak_count",
        "type": "long",
        "metadata": {"comment": "Number of databreak events."},
    },
    {
        "name": "avg_duration_ms",
        "type": "double",
        "nullable": True,
        "metadata": {"comment": "Average duration in milliseconds (valid events only)."},
    },
    {
        "name": "min_duration_ms",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "Minimum duration in milliseconds."},
    },
    {
        "name": "max_duration_ms",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "Maximum duration in milliseconds."},
    },
    {
        "name": "sum_duration_ms",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "Sum of durations in milliseconds (for weighted averages)."},
    },
    {
        "name": "duration_count",
        "type": "long",
        "metadata": {"comment": "Number of events with non-null duration (for averaging)."},
    },
]


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""
        Daily aggregated table for eco driving segmented events from kinesisstats.

        Pre-aggregates the 5 eco driving event tables at the device-day-event_type level:
        - osDEcoDrivingEventCruiseControl
        - osDEcoDrivingEventHardAcceleration
        - osDEcoDrivingEventOverSpeed
        - osDEcoDrivingEventCoasting
        - osDEcoDrivingEventWearFreeBraking

        Each row represents one device's daily summary for one event type, including:
        - Event counts (total, valid, invalid, databreak)
        - Duration statistics (avg, min, max, sum, count)

        This enables efficient dashboard queries without scanning raw event tables.
        """,
        row_meaning="Each row represents a device's daily event summary for one eco driving event type.",
        related_table_info={},
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    schema=schema,
    primary_keys=["date", "org_id", "object_id", "event_type"],
    partitioning=["date"],
    upstreams=list(EVENT_TABLES.keys()),
    dq_checks=[
        NonEmptyDQCheck(
            name="dq_non_empty_agg_eco_driving_segmented_events",
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_agg_eco_driving_segmented_events",
            non_null_columns=["date", "org_id", "object_id", "event_type", "event_count"],
            block_before_write=True,
        ),
        TrendDQCheck(
            name="dq_trend_agg_eco_driving_segmented_events",
            lookback_days=1,
            tolerance=0.25,
            period="weeks",
        ),
    ],
    backfill_start_date="2024-01-01",
    backfill_batch_size=7,
    write_mode=WarehouseWriteMode.OVERWRITE,
    regions=get_all_regions(),
    owners=[FIRMWAREVDP],
)
def agg_eco_driving_segmented_events(context: AssetExecutionContext) -> DataFrame:
    """
    Build daily aggregated eco driving events table by device/day/event_type.
    """
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    partition_dates = partition_key_ranges_from_context(context)[0]
    start_date = partition_dates[0]
    end_date = partition_dates[-1]

    def read_and_aggregate_event_table(table_name: str, event_type: str) -> DataFrame:
        """Read a single event table and aggregate at device-day level."""
        raw_df = (
            spark.table(table_name)
            .filter(F.col("date").between(start_date, end_date))
            .select(
                F.col("date"),
                F.col("org_id"),
                F.col("object_id"),
                F.lit(event_type).alias("event_type"),
                F.col("value.proto_value.obd_segmented_event.duration_ms").alias("duration_ms"),
                F.col("value.proto_value").isNotNull().alias("is_valid"),
                F.coalesce(F.col("value.is_databreak"), F.lit(False)).alias("is_databreak"),
            )
        )

        return raw_df.groupBy("date", "org_id", "object_id", "event_type").agg(
            F.count("*").alias("event_count"),
            F.sum(F.when(F.col("is_valid"), 1).otherwise(0)).alias("valid_count"),
            F.sum(
                F.when(~F.col("is_valid") & ~F.col("is_databreak"), 1).otherwise(0)
            ).alias("invalid_count"),
            F.sum(F.when(F.col("is_databreak"), 1).otherwise(0)).alias("databreak_count"),
            F.avg(F.when(F.col("is_valid"), F.col("duration_ms"))).alias("avg_duration_ms"),
            F.min(F.when(F.col("is_valid"), F.col("duration_ms"))).alias("min_duration_ms"),
            F.max(F.when(F.col("is_valid"), F.col("duration_ms"))).alias("max_duration_ms"),
            F.sum(F.when(F.col("is_valid"), F.col("duration_ms"))).alias("sum_duration_ms"),
            F.sum(
                F.when(F.col("is_valid") & F.col("duration_ms").isNotNull(), 1).otherwise(0)
            ).alias("duration_count"),
        )

    # Aggregate each event table and union
    agg_dfs = [
        read_and_aggregate_event_table(table_name, event_type)
        for table_name, event_type in EVENT_TABLES.items()
    ]

    unified_df = reduce(DataFrame.unionByName, agg_dfs)

    return unified_df
