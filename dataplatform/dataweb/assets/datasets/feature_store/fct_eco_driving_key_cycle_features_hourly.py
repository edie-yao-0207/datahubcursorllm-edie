from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import reduce
from typing import Callable, List, Optional, Tuple

from dagster import (
    AssetExecutionContext,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
)
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    SUSTAINABILITY,
    AWSRegion,
    Database,
    InstanceType,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb.userpkgs.utils import (
    build_table_description,
    get_all_regions,
    get_region_from_context,
)
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SCHEMA = [
    {
        "name": "date",
        "type": "date",
        "metadata": {"comment": ""},
    },
    {
        "name": "hour",
        "type": "string",
        "metadata": {
            "comment": "The 3-hour partition window start hour (00, 03, 06, ...)"
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "metadata": {"comment": ""},
    },
    {
        "name": "device_id",
        "type": "long",
        "metadata": {"comment": ""},
    },
    {
        "name": "cycle_id",
        "type": "string",
        "metadata": {"comment": ""},
    },
    {
        "name": "interval_start",
        "type": "timestamp",
        "metadata": {"comment": ""},
    },
    {
        "name": "interval_end",
        "type": "timestamp",
        "metadata": {"comment": ""},
    },
    {
        "name": "enrichment_due_at",
        "type": "timestamp",
        "metadata": {"comment": ""},
    },
    {
        "name": "total_cruise_control_ms",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "total_coasting_ms",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "weight_time",
        "type": "long",
        "metadata": {"comment": ""},
    },
    {
        "name": "weighted_weight",
        "type": "long",
        "metadata": {"comment": ""},
    },
    {
        "name": "average_weight_kg",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "uphill_duration_ms",
        "type": "long",
        "metadata": {"comment": ""},
    },
    {
        "name": "downhill_duration_ms",
        "type": "long",
        "metadata": {"comment": ""},
    },
    {
        "name": "wear_free_braking_duration_ms",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "total_braking_duration_ms",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "on_duration_ms",
        "type": "long",
        "metadata": {"comment": ""},
    },
    {
        "name": "idle_duration_ms",
        "type": "long",
        "metadata": {"comment": ""},
    },
    {
        "name": "fuel_consumed_ml",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "distance_traveled_m",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "avg_altitude_meters",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "stddev_altitude_meters",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "stddev_speed_mps",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "avg_latitude",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "avg_longitude",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "ms_in_speed_0_25mph",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "ms_in_speed_25mph_80kph",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "ms_in_speed_80kph_50mph",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "ms_in_speed_50mph_55mph",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "ms_in_speed_55mph_100kph",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "ms_in_speed_100kph_65mph",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "ms_in_speed_65_plus_mph",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "ms_in_accel_hard_braking",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "ms_in_accel_mod_braking",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "ms_in_accel_cruise_coast",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "ms_in_accel_mod_accel",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "ms_in_accel_hard_accel",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "avg_lanes",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "avg_speed_limit_kph",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "road_type_diversity",
        "type": "long",
        "metadata": {"comment": ""},
    },
    {
        "name": "speed_limit_stddev",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "predominant_road_type",
        "type": "string",
        "metadata": {"comment": ""},
    },
    {
        "name": "accelerator_pedal_time_gt95_ms",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "associated_vehicle_make",
        "type": "string",
        "metadata": {"comment": ""},
    },
    {
        "name": "associated_vehicle_model",
        "type": "string",
        "metadata": {"comment": ""},
    },
    {
        "name": "associated_vehicle_year",
        "type": "long",
        "metadata": {"comment": ""},
    },
    {
        "name": "associated_vehicle_primary_fuel_type",
        "type": "string",
        "metadata": {"comment": ""},
    },
    {
        "name": "associated_vehicle_gross_vehicle_weight_rating",
        "type": "string",
        "metadata": {"comment": ""},
    },
    {
        "name": "associated_vehicle_engine_type",
        "type": "string",
        "metadata": {"comment": ""},
    },
    {
        "name": "locale",
        "type": "string",
        "metadata": {"comment": ""},
    },
    {
        "name": "org_cell_id",
        "type": "string",
        "metadata": {"comment": ""},
    },
    {
        "name": "altitude_delta_meters",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "earliest_heading_degrees",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "heading_change_degrees",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "heading_step_mean_absolute_degrees",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "last_cable_id",
        "type": "long",
        "metadata": {"comment": ""},
    },
    {
        "name": "latest_heading_degrees",
        "type": "double",
        "metadata": {"comment": ""},
    },
    {
        "name": "revgeo_country",
        "type": "string",
        "metadata": {"comment": ""},
    },
    {
        "name": "median_air_temp_milli_c",
        "type": "double",
        "metadata": {
            "comment": "Median engine air temperature, in millionths of a degree C."
        },
    },
    {
        "name": "is_j1939",
        "type": "boolean",
        "metadata": {"comment": "Whether the device's last_cable_id has a J1939 bus type"},
    },
]

# Type mapping for schema conversion
_TYPE_MAP = {
    "date": DateType(),
    "string": StringType(),
    "long": LongType(),
    "double": DoubleType(),
    "timestamp": TimestampType(),
}


# Bin size for range join hints (5 minutes in seconds, matching our interval size)
# For timestamp columns, Databricks interprets the bin size in seconds
RANGE_JOIN_BIN_SIZE_SECONDS = 5 * 60


def _get_spark_schema() -> StructType:
    """Convert schema list to PySpark StructType."""
    fields = [
        StructField(field["name"], _TYPE_MAP[field["type"]], nullable=True)
        for field in SCHEMA
    ]
    return StructType(fields)


def _create_empty_dataframe(spark: SparkSession) -> DataFrame:
    """Create an empty DataFrame with the correct schema."""
    return spark.createDataFrame([], schema=_get_spark_schema())


# Constants from Go code
MAX_FUEL_CONSUMED_ML_BY_HOUR = 454609  # 100 Imperial gallons
MAX_GASEOUS_FUEL_CONSUMED_GRAMS_BY_HOUR = 271000
MAX_ODOMETER_SPEED_KMPH = 150  # Threshold for anomaly detection

# Partition interval in hours. This should be updated based on the eco_key_cycles job frequency.
PARTITION_INTERVAL_HOURS = 3

# Buffer in minutes to accommodate delayed eco-driving stat data arrival
# Intervals with enrichment_due_at in the last ECO_DRIVING_STAT_DATA_DELAY_BUFFER_MINUTES
# of a partition window will be processed in the next run
ECO_DRIVING_STAT_DATA_DELAY_BUFFER_MINUTES = 10

# Maximum lookback for interval data to prevent querying across wide date ranges
# Intervals older than this will not be processed even if enrichment_due_at is recent
# Enrichment_due_at can be older than 3 days because of operator assignment updates, but fuel and distance
# updates have a threshold of max 3 days which is defined in the Go code:
# fleet/engineactivity/engineactivityretriescronjob/clients/utils.go
# TODO: Evaluate the performance impact from a wider lookback window and update this based on that and product requirements.
MAX_INTERVAL_LOOKBACK_DAYS = 6

# Generate hour partition keys, for instance, for 3-hour windows: 00, 03, 06, 09, 12, 15, 18, 21
HOUR_PARTITION_KEYS = [f"{h:02d}" for h in range(0, 24, PARTITION_INTERVAL_HOURS)]

HOURLY_PARTITIONS = MultiPartitionsDefinition(
    {
        "date": DATAWEB_PARTITION_DATE,
        "hour": StaticPartitionsDefinition(partition_keys=HOUR_PARTITION_KEYS),
    }
)


def get_asset_efficiency_intervals(
    spark: SparkSession,
    partition_start: datetime,
    partition_end: datetime,
    org_ids: Optional[List[int]] = None,
    object_ids: Optional[List[int]] = None,
) -> DataFrame:
    """
    Get asset efficiency intervals that were updated within the partition window.

    Filters by enrichment_due_at with a buffer to ensure all the required stat data has arrived.
    The buffer shifts the window back by ECO_DRIVING_STAT_DATA_DELAY_BUFFER_MINUTES, so intervals
    near the end of the window are processed in the next run when stats are available.
    In addition, limits to intervals from the last MAX_INTERVAL_LOOKBACK_DAYS to prevent
    querying across wide date ranges for performance.

    Effective window: [partition_start - buffer, partition_end - buffer)
    """
    buffer = timedelta(minutes=ECO_DRIVING_STAT_DATA_DELAY_BUFFER_MINUTES)
    window_start = partition_start - buffer
    window_end = partition_end - buffer

    window_start_ts = F.lit(window_start)
    window_end_ts = F.lit(window_end)

    # Calculate the earliest date to be considered for interval data with the partition window
    lookback_cutoff = partition_start - timedelta(days=MAX_INTERVAL_LOOKBACK_DAYS)
    lookback_cutoff_ts = F.lit(lookback_cutoff)

    asset_efficiency_intervals_df = (
        spark.table("ecodrivingdb_shards.asset_efficiency_intervals")
        .filter(
            F.col("date").between(
                F.lit(lookback_cutoff.strftime("%Y-%m-%d")),
                F.lit(window_end.strftime("%Y-%m-%d")),
            )
        )
        .filter(F.col("deleted_at").isNull())
        # Filter by enrichment_due_at within the partition window (with buffer)
        .filter(
            (F.col("enrichment_due_at") >= window_start_ts)
            & (F.col("enrichment_due_at") < window_end_ts)
        )
        # Limit to intervals from the last MAX_INTERVAL_LOOKBACK_DAYS days
        .filter((F.col("start_ms") / 1000).cast("timestamp") >= lookback_cutoff_ts)
    )

    if org_ids is not None:
        asset_efficiency_intervals_df = asset_efficiency_intervals_df.filter(
            F.col("org_id").isin(org_ids)
        )

    if object_ids is not None:
        asset_efficiency_intervals_df = asset_efficiency_intervals_df.filter(
            F.col("device_id").isin(object_ids)
        )

    asset_efficiency_intervals_df = asset_efficiency_intervals_df.select(
        "org_id",
        F.col("device_id").alias("object_id"),
        (F.col("start_ms") / 1000).cast("timestamp").alias("cycle_start_time"),
        ((F.col("start_ms") + F.col("duration_ms")) / 1000)
        .cast("timestamp")
        .alias("cycle_end_time"),
        F.col("uuid").alias("cycle_id"),
        F.col("enrichment_due_at"),
    )

    return asset_efficiency_intervals_df


def generate_intervals_from_key_cycles(key_cycles_df: DataFrame) -> DataFrame:
    """Takes a DataFrame of key cycles and explodes it into 5-minute interval start times."""
    five_min_seconds = 5 * 60

    key_cycles_df = key_cycles_df.dropDuplicates(["org_id", "object_id", "cycle_id"])
    intervals_df = key_cycles_df.select(
        "org_id",
        "object_id",
        "cycle_id",
        "enrichment_due_at",
        F.explode(
            F.sequence(
                (
                    F.floor(F.unix_timestamp("cycle_start_time") / five_min_seconds)
                    * five_min_seconds
                ).cast("timestamp"),
                (
                    F.floor(F.unix_timestamp("cycle_end_time") / five_min_seconds)
                    * five_min_seconds
                ).cast("timestamp"),
                F.expr("interval 5 minutes"),
            )
        ).alias("interval_start"),
    )

    intervals_df = intervals_df.withColumn(
        "interval_end", F.col("interval_start") + F.expr("interval 5 minutes")
    )

    return intervals_df


# Helper Functions for Interval Apportionment
def apportion_cumulative_to_intervals(
    data_df: DataFrame, intervals_df: DataFrame, value_column: str, output_column: str
) -> DataFrame:
    """
    Apportion cumulative values (like odometer) to intervals.
    Calculate the difference within each interval.
    """

    # Join data with intervals using range join hint for performance
    # Point value (d.timestamp) must be on the left for range join optimization
    joined = (
        data_df.alias("d")
        .hint("range_join", RANGE_JOIN_BIN_SIZE_SECONDS)
        .join(
            intervals_df.alias("i"),
            (F.col("d.org_id") == F.col("i.org_id"))
            & (F.col("d.object_id") == F.col("i.object_id"))
            & (F.col("d.timestamp") >= F.col("i.interval_start"))
            & (F.col("d.timestamp") < F.col("i.interval_end")),
            "left",
        )
    )

    # Get first and last values in each interval
    interval_values = joined.groupBy(
        "i.org_id", "i.object_id", "i.interval_start", "i.interval_end"
    ).agg(
        F.first(F.col(f"d.{value_column}")).alias("first_value"),
        F.last(F.col(f"d.{value_column}")).alias("last_value"),
        F.count(F.col(f"d.{value_column}")).alias("data_points"),
    )

    # Calculate difference (handle nulls)
    result = (
        interval_values.withColumn(
            output_column,
            F.when(
                (F.col("first_value").isNotNull()) & (F.col("last_value").isNotNull()),
                F.col("last_value") - F.col("first_value"),
            ).otherwise(0),
        )
        .withColumn("has_data", F.col("data_points") > 0)
        .select(
            F.col("org_id"),
            F.col("object_id"),
            F.col("interval_start"),
            F.col("interval_end"),
            F.col(output_column),
            F.col("has_data"),
        )
    )

    return result


def align_delta_to_intervals(
    data_df: DataFrame,
    intervals_df: DataFrame,
    value_column: str,
    output_column: str,
    agg_fn: Callable[[Column], Column] = None,
    aggregates: List[Tuple[Column, str]] = None,
) -> DataFrame:
    """
    Apportion delta values (like segment distances) to intervals.
    Aggregate (default sum) all values within each interval.
    """
    aggregates = aggregates or []

    agg_fn = agg_fn or F.sum
    # Join data with intervals using range join hint for performance
    # Point value (d.timestamp) must be on the left for range join optimization
    joined = (
        data_df.alias("d")
        .hint("range_join", RANGE_JOIN_BIN_SIZE_SECONDS)
        .join(
            intervals_df.alias("i"),
            (F.col("d.org_id") == F.col("i.org_id"))
            & (F.col("d.object_id") == F.col("i.object_id"))
            & (F.col("d.timestamp") >= F.col("i.interval_start"))
            & (F.col("d.timestamp") < F.col("i.interval_end")),
            "left",
        )
    )

    # Aggregate values in each interval
    result = (
        joined.groupBy("i.org_id", "i.object_id", "i.interval_start", "i.interval_end")
        .agg(
            agg_fn(F.col(f"d.{value_column}")).alias(output_column),
            F.count(F.col(f"d.*")).alias("data_points"),
            *[agg.alias(output_col) for agg, output_col in aggregates],
        )
        .withColumn(output_column, F.coalesce(F.col(output_column), F.lit(0)))
        .select(
            "org_id",
            "object_id",
            "interval_start",
            "interval_end",
            F.col(output_column),
            *[output_col for _, output_col in aggregates],
        )
    )

    return result


def apportion_interval_values_to_5min_intervals(
    data_df: DataFrame, intervals_df: DataFrame, value_column: str, output_column: str
) -> DataFrame:
    """
    Apportion interval-based values (treated as time-deltas) to fixed 5-minute buckets.
    Matches original behavior by:
      - Treating inputs as event-to-event deltas over [prev_timestamp, timestamp)
      - Capping gaps >= 10 minutes by shortening the effective interval to 5 minutes
      - Distributing proportionally by overlap ratio with each 5-minute bucket

    Expected columns in data_df: org_id, object_id, prev_timestamp, timestamp, value_column
    Expected columns in intervals_df: org_id, object_id, interval_start, interval_end
    """

    # Compute effective previous timestamp with 10-minute cap like original
    # total_duration_s = min(real gap, 5 minutes) when gap >= 10 minutes
    capped = data_df.withColumn(
        "adjusted_prev_ts",
        F.when(
            (F.unix_timestamp("timestamp") - F.unix_timestamp("prev_timestamp")) >= 600,
            F.col("timestamp") - F.expr("interval 5 minutes"),
        ).otherwise(F.col("prev_timestamp")),
    ).filter(F.col("timestamp") > F.col("adjusted_prev_ts"))

    # Join to 5-min intervals allowing partial overlaps using range join hint
    # This is an interval overlap condition, requires inner join for range join optimization
    joined = (
        capped.alias("d")
        .hint("range_join", RANGE_JOIN_BIN_SIZE_SECONDS)
        .join(
            intervals_df.alias("i"),
            (F.col("d.org_id") == F.col("i.org_id"))
            & (F.col("d.object_id") == F.col("i.object_id"))
            & (F.col("d.timestamp") > F.col("i.interval_start"))
            & (F.col("d.adjusted_prev_ts") < F.col("i.interval_end")),
            "inner",
        )
    )

    # Calculate overlap ratio
    joined = (
        joined.withColumn(
            "overlap_start",
            F.greatest(F.col("i.interval_start"), F.col("d.adjusted_prev_ts")),
        )
        .withColumn(
            "overlap_end", F.least(F.col("i.interval_end"), F.col("d.timestamp"))
        )
        .withColumn(
            "overlap_seconds",
            F.when(
                (F.col("overlap_end") > F.col("overlap_start")),
                (
                    F.unix_timestamp("overlap_end") - F.unix_timestamp("overlap_start")
                ).cast("double"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "total_interval_seconds",
            (
                F.unix_timestamp("d.timestamp") - F.unix_timestamp("d.adjusted_prev_ts")
            ).cast("double"),
        )
        .withColumn(
            "overlap_ratio",
            F.when(
                F.col("total_interval_seconds") > 0,
                F.col("overlap_seconds") / F.col("total_interval_seconds"),
            ).otherwise(0.0),
        )
        .filter(F.col("overlap_ratio") > 0)
        .withColumn(
            "apportioned_value", F.col(f"d.{value_column}") * F.col("overlap_ratio")
        )
    )

    # Aggregate apportioned values per 5-min bucket
    result = joined.groupBy(
        "i.org_id", "i.object_id", F.col("i.interval_start").alias("interval_start")
    ).agg(F.sum("apportioned_value").alias(output_column))

    return result


def haversine_distance_km_expr(lat1_col, lon1_col, lat2_col, lon2_col):
    """Calculate haversine distance using SQL expressions instead of UDF to avoid ClassCastException"""
    R = 6371  # Earth's radius in kilometers

    # Convert to radians using SQL expressions
    lat1_rad = F.radians(lat1_col)
    lat2_rad = F.radians(lat2_col)
    delta_lat = F.radians(lat2_col - lat1_col)
    delta_lon = F.radians(lon2_col - lon1_col)

    # Haversine formula using SQL expressions
    a = F.pow(F.sin(delta_lat / 2), 2) + F.cos(lat1_rad) * F.cos(lat2_rad) * F.pow(
        F.sin(delta_lon / 2), 2
    )
    c = 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))

    return F.when(
        (lat1_col.isNull())
        | (lon1_col.isNull())
        | (lat2_col.isNull())
        | (lon2_col.isNull()),
        F.lit(0.0),
    ).otherwise(R * c)


def calculate_gps_distance_segments(
    location_df: DataFrame,
    speed_filter_mps: float = 0.0,
    max_time_delta_s: int = 300,
    max_segment_distance_km: float = 10.0,
) -> DataFrame:
    """
    Calculate GPS distance segments from location data.
    This is a shared helper to reduce code duplication between grade and distance calculations.

    Parameters:
    - location_df: DataFrame with org_id, object_id, timestamp, latitude, longitude, speed columns
    - speed_filter_mps: Minimum speed in m/s to include (default 0.0 for no filter)
    - max_time_delta_s: Maximum time between points in seconds (default 300)
    - max_segment_distance_km: Maximum distance per segment in km (default 10.0)

    Returns:
    - DataFrame with fields in location_df, plus segment_distance_km
    """

    if speed_filter_mps > 0:
        location_filtered = location_df.filter(
            F.coalesce(F.col("ecu_speed_mps"), F.col("gps_speed_mps"), F.lit(1))
            >= speed_filter_mps
        )
    else:
        location_filtered = location_df

    location_with_prev = location_filtered

    # Calculate distance for each segment with better filtering using SQL expressions
    distance_segments = (
        location_with_prev.filter(F.col("prev_timestamp").isNotNull())
        .withColumn(
            "time_delta_s",
            (F.col("timestamp").cast("long") - F.col("prev_timestamp").cast("long")),
        )
        .filter(F.col("time_delta_s").between(1, max_time_delta_s))
        .withColumn(
            "is_stationary",
            (F.coalesce(F.col("ecu_speed_mps"), F.lit(1)) == 0)
            & (F.coalesce(F.col("prev_ecu_speed"), F.lit(1)) == 0),
        )
        .filter(F.col("is_stationary") == False)
        .withColumn(
            "segment_distance_km",
            haversine_distance_km_expr(
                F.col("prev_latitude"),
                F.col("prev_longitude"),
                F.col("latitude"),
                F.col("longitude"),
            ),
        )
        .filter(F.col("segment_distance_km") < max_segment_distance_km)
    )

    return distance_segments


def calculate_distance_hierarchical(
    spark: SparkSession,
    intervals_df: DataFrame,
    start_date: str,
    end_date: str,
    region: Optional[str] = None,
    object_ids: Optional[List[int]] = None,
) -> DataFrame:
    """
    Calculate distance using hierarchical approach:
    1. Try odometer (most accurate)
    2. Fall back to GPS for anomalous periods
    3. Use location-based calculation as last resort
    """

    odo_df = calculate_odometer_distance(
        spark, intervals_df, start_date, end_date, object_ids
    )

    odo_with_anomalies = (
        odo_df.withColumn(
            "duration_hours",
            (F.col("interval_end").cast("long") - F.col("interval_start").cast("long"))
            / 3600,
        )
        .withColumn(
            "avg_speed_kmph",
            F.when(
                F.col("duration_hours") > 0,
                F.col("odometer_km") / F.col("duration_hours"),
            ).otherwise(0),
        )
        .withColumn(
            "is_anomalous",
            F.when(
                (F.col("avg_speed_kmph") > MAX_ODOMETER_SPEED_KMPH)
                | ((F.col("odometer_km") == 0) & (F.col("has_data") == True)),
                True,
            ).otherwise(False),
        )
    )

    # Create GPS distance calculation for all potential anomalous intervals
    anomalous_intervals = odo_with_anomalies.select(
        "org_id",
        "object_id",
        "interval_start",
        "interval_end",
    )

    gps_df = calculate_gps_distance(
        spark,
        anomalous_intervals,
        start_date,
        end_date,
        object_ids,
        region,
    )

    # Combine: use GPS for anomalous periods, odometer otherwise
    final_distance_df = (
        gps_df.alias("gps")
        .join(
            odo_with_anomalies.alias("odo"),
            ["org_id", "object_id", "interval_start"],
            "left",
        )
        .withColumn(
            "distance_km",
            F.when(
                F.col("odo.is_anomalous"),
                F.coalesce(F.col("gps.gps_distance_km"), F.lit(0)),
            ).otherwise(F.col("odo.odometer_km")),
        )
        .withColumn(
            "distance_source",
            F.when(F.col("odo.is_anomalous"), F.lit("gps")).otherwise(
                F.lit("odometer")
            ),
        )
        .select(
            "distance_km",
            "distance_source",
            "gps.*",
        )
    )

    return final_distance_df


def calculate_odometer_distance(
    spark: SparkSession,
    intervals_df: DataFrame,
    start_date: str,
    end_date: str,
    object_ids: Optional[List[int]] = None,
) -> DataFrame:
    """Calculate distance from odometer stats."""

    # Read odometer data with better filtering
    odo_raw = spark.table("kinesisstats.osdodometer").filter(
        F.col("date").between(F.lit(start_date), F.lit(end_date))
    )

    if object_ids:
        odo_raw = odo_raw.filter(F.col("object_id").isin(object_ids))

    # Get odometer readings with timestamps and add filtering
    odo_df = odo_raw.select(
        "org_id",
        "object_id",
        (F.col("time") / 1000).cast("timestamp").alias("timestamp"),
        F.col("value.double_value").alias("odometer_m"),
    ).filter(
        F.col("odometer_m").isNotNull()
        & (F.col("odometer_m") >= 0)  # Filter negative values
        & (
            F.col("odometer_m") < 10000000000
        )  # Filter unreasonably large values (10M km)
    )

    # Apportion odometer to intervals using helper function
    result_df = apportion_cumulative_to_intervals(
        odo_df, intervals_df, "odometer_m", "odometer_km"
    )

    # Convert meters to kilometers and filter anomalies
    result_df = result_df.withColumn(
        "odometer_km",
        F.greatest(F.col("odometer_km") / 1000, F.lit(0)),  # Ensure non-negative
    ).withColumn(
        "odometer_km",
        F.when(
            F.col("odometer_km") > 1000, F.lit(0)
        ).otherwise(  # Cap at 1000 km per 5-min interval
            F.col("odometer_km")
        ),
    )

    return result_df


def get_osm_attributes(spark: SparkSession, region: Optional[str] = None) -> DataFrame:
    region = region or AWSRegion.US_WEST_2
    osm_table = (
        "data_tools_delta_share.dojo.osm_way_nodes"
        if region in [AWSRegion.EU_WEST_1, AWSRegion.CA_CENTRAL_1]
        else "dojo.osm_way_nodes"
    )
    distinct_ways_df = (
        spark.table(osm_table)
        .groupBy("way_id", "highway_tag_value")
        .agg(F.first("tags").alias("tags"))
    )

    # Extract lanes and speed limit from tags using get() for safe array access
    return (
        distinct_ways_df.withColumn(
            "lanes_str",
            F.get(F.filter(F.col("tags"), lambda t: t["key"] == "lanes"), 0)["value"],
        )
        .withColumn(
            "lanes",
            F.when(
                F.col("lanes_str").isNotNull()
                & F.col("lanes_str").rlike("^[0-9.]+$"),  # Only numeric values
                F.col("lanes_str").cast("float"),
            ).otherwise(None),
        )
        .withColumn(
            "lanes",
            F.when(
                F.col("lanes").isNotNull()
                & (F.col("lanes") >= 1)
                & (F.col("lanes") <= 12),  # Valid range 1-12
                F.col("lanes"),
            ).otherwise(None),
        )
        .withColumn(
            "maxspeed_str",
            F.get(F.filter(F.col("tags"), lambda t: t["key"] == "maxspeed"), 0)[
                "value"
            ],
        )
        .withColumn(
            "maxspeed_kph",
            F.when(
                F.lower(F.col("maxspeed_str")).like("%mph%"),
                # Convert from MPH to KPH
                F.regexp_extract(F.col("maxspeed_str"), r"(\d+)", 1).cast("float")
                * 1.60934,
            )
            .when(
                F.col("maxspeed_str").rlike("^[0-9.]+$"),
                F.col("maxspeed_str").cast("float"),
            )
            .when(
                F.lower(F.col("maxspeed_str")).like("%kph%"),
                F.regexp_extract(F.col("maxspeed_str"), r"(\d+)", 1).cast("float"),
            )
            .otherwise(None),
        )
        .select("way_id", "highway_tag_value", "lanes", "maxspeed_kph")
        .filter(F.col("highway_tag_value").isNotNull())
    )


def calculate_gps_distance(
    spark: SparkSession,
    intervals_df: DataFrame,
    start_date: str,
    end_date: str,
    object_ids: Optional[List[int]] = None,
    region: Optional[str] = None,
) -> DataFrame:
    """Calculate distance from GPS location data using SQL expressions instead of UDF."""

    # Read location data with more selective filtering
    location_raw = spark.table("kinesisstats_window.location").filter(
        F.col("date").between(F.lit(start_date), F.lit(end_date))
    )

    if object_ids:
        location_raw = location_raw.filter(F.col("device_id").isin(object_ids))

    # Extract GPS points with better filtering
    location_df = location_raw.select(
        "org_id",
        F.col("device_id").alias("object_id"),
        (F.col("time") / 1000).cast("timestamp").alias("timestamp"),
        F.col("value.latitude").alias("latitude"),
        F.col("value.longitude").alias("longitude"),
        F.col("value.ecu_speed_meters_per_second").alias("ecu_speed_mps"),
        F.col("value.gps_speed_meters_per_second").alias("gps_speed_mps"),
        F.coalesce(
            F.col("value.gps_speed_meters_per_second"),
            F.col("value.ecu_speed_meters_per_second"),
        ).alias("speed_mps"),
        (F.col("previous.time") / 1000).cast("timestamp").alias("prev_timestamp"),
        F.col("previous.value.latitude").alias("prev_latitude"),
        F.col("previous.value.longitude").alias("prev_longitude"),
        F.col("previous.value.ecu_speed_meters_per_second").alias("prev_ecu_speed"),
        F.col("value.altitude_meters").alias("altitude_meters"),
        F.col("value.heading_degrees").alias("heading_degrees"),
        F.col("previous.value.heading_degrees").alias("previous_heading_degrees"),
        F.col("value.revgeo_country").alias("revgeo_country"),
        F.col("value.way_id").alias("way_id"),
        F.coalesce(F.col("time") - F.col("previous.time"), F.lit(5000)).alias(
            "time_delta_ms"
        ),
    ).filter(
        F.col("latitude").isNotNull()
        & F.col("longitude").isNotNull()
        & F.col("altitude_meters").isNotNull()
        & (F.col("latitude").between(-90, 90))  # Valid latitude range
        & (F.col("longitude").between(-180, 180))  # Valid longitude range
        & (F.abs(F.col("latitude")) > 0.001)  # Filter out (0,0) coordinates
        & (F.abs(F.col("longitude")) > 0.001)
    )

    osm_attributes_df = get_osm_attributes(spark, region)

    distance_segments = (
        calculate_gps_distance_segments(
            location_df,
            speed_filter_mps=0.0,  # No speed filter for distance calculation
            max_time_delta_s=300,
            max_segment_distance_km=10.0,
        )
        .alias("l")
        .join(osm_attributes_df.alias("osm"), on=["way_id"], how="left")
        .select(
            "l.*",
            "osm.highway_tag_value",
            "osm.lanes",
            "osm.maxspeed_kph",
        )
        .withColumn(
            "interval_start",
            F.from_unixtime(F.floor(F.unix_timestamp("timestamp") / 300) * 300).cast(
                "timestamp"
            ),
        )
        .withColumn(
            "time_on_road_type",
            F.sum("time_delta_ms").over(
                Window.partitionBy(
                    "org_id", "object_id", "interval_start", "highway_tag_value"
                )
            ),
        )
    )

    def get_nonnull_ordinal_for_minmax(
        ordinal_column: str,
        value_column: str,
    ) -> Column:
        """
        For `min_by` and `max_by` aggregations, this function transforms
        the ordinal column to be null if the value column is. This
        lets us get the nonnull value closes to the minimum or maximum ordinal value.
        """

        return F.when(F.col(value_column).isNull(), None).otherwise(
            F.col(ordinal_column)
        )

    aggs = [
        # Altitude statistics
        (F.avg("altitude_meters"), "avg_altitude_meters"),
        (F.stddev_pop("altitude_meters"), "stddev_altitude_meters"),
        (
            (
                F.max_by(
                    "altitude_meters",
                    get_nonnull_ordinal_for_minmax("timestamp", "altitude_meters"),
                )
                - F.min_by(
                    "altitude_meters",
                    get_nonnull_ordinal_for_minmax("timestamp", "altitude_meters"),
                )
            ),
            "altitude_delta_meters",
        ),
        # Heading statistics
        (
            F.min_by(
                "heading_degrees",
                get_nonnull_ordinal_for_minmax("timestamp", "heading_degrees"),
            ),
            "earliest_heading_degrees",
        ),
        (
            F.max_by(
                "heading_degrees",
                get_nonnull_ordinal_for_minmax("timestamp", "heading_degrees"),
            ),
            "latest_heading_degrees",
        ),
        (
            (
                (
                    F.max_by(
                        "heading_degrees",
                        get_nonnull_ordinal_for_minmax("timestamp", "heading_degrees"),
                    )
                    - F.min_by(
                        "heading_degrees",
                        get_nonnull_ordinal_for_minmax("timestamp", "heading_degrees"),
                    )
                    + F.lit(540)
                )
                % F.lit(360)
                - F.lit(180)
            ),
            "heading_change_degrees",
        ),
        (
            F.avg(F.abs(F.col("heading_degrees") - F.col("previous_heading_degrees"))),
            "heading_step_mean_absolute_degrees",
        ),
        # Speed variation
        (F.stddev_pop("speed_mps"), "stddev_speed_mps"),
        # Average location
        (F.avg("latitude"), "avg_latitude"),
        (F.avg("longitude"), "avg_longitude"),
        # OSM attributes
        (
            (
                F.sum(F.col("lanes") * F.col("time_delta_ms"))
                / F.when(F.sum("lanes").isNotNull(), F.sum("time_delta_ms"))
            ),
            "avg_lanes",
        ),
        (
            (
                F.sum(F.col("maxspeed_kph") * F.col("time_delta_ms"))
                / F.when(F.sum("maxspeed_kph").isNotNull(), F.sum("time_delta_ms"))
            ),
            "avg_speed_limit_kph",
        ),
        (F.countDistinct("highway_tag_value"), "road_type_diversity"),
        (F.stddev_pop("maxspeed_kph"), "speed_limit_stddev"),
        (F.max_by("highway_tag_value", "time_on_road_type"), "predominant_road_type"),
        # Dimension values
        (F.any_value("revgeo_country"), "revgeo_country"),
    ]

    # Apportion distances to intervals using helper function
    result_df = align_delta_to_intervals(
        distance_segments,
        intervals_df,
        "segment_distance_km",
        "gps_distance_km",
        aggregates=aggs,
    )

    return result_df


def calculate_fuel_consumption(
    spark: SparkSession,
    intervals_df: DataFrame,
    start_date: str,
    end_date: str,
    object_ids: Optional[List[int]] = None,
) -> DataFrame:
    """
    Calculate fuel consumption mirroring Go implementation:
    1. Read raw delta fuel stats (already delta values, not cumulative)
    2. Apply engine state filtering (only ON/IDLE)
    3. Group into hourly windows and filter anomalies
    4. Redistribute valid consumption to 5-min intervals
    """

    # Get conventional fuel consumption
    conventional_fuel_df = calculate_conventional_fuel(
        spark, intervals_df, start_date, end_date, object_ids
    )

    # Combine fuel types
    result_df = intervals_df.join(
        conventional_fuel_df, ["org_id", "object_id", "interval_start"], "left"
    ).select(
        "org_id",
        "object_id",
        "interval_start",
        "interval_end",
        F.coalesce(F.col("fuel_consumed_ml"), F.lit(0)).alias("fuel_consumed_ml"),
    )

    return result_df


def calculate_conventional_fuel(
    spark: SparkSession,
    intervals_df: DataFrame,
    start_date: str,
    end_date: str,
    object_ids: Optional[List[int]] = None,
) -> DataFrame:
    """Calculate conventional fuel consumption with max filtering."""

    # Read delta fuel consumed (already contains delta values, not cumulative)
    fuel_raw = spark.table("kinesisstats.osddeltafuelconsumed").filter(
        F.col("date").between(F.lit(start_date), F.lit(end_date))
    )

    if object_ids:
        fuel_raw = fuel_raw.filter(F.col("object_id").isin(object_ids))

    # Extract fuel values (these are already delta values)
    fuel_df = fuel_raw.select(
        "org_id",
        "object_id",
        (F.col("time") / 1000).cast("timestamp").alias("timestamp"),
        F.col("value.int_value").alias("fuel_delta_ml"),
    ).filter(
        (F.col("fuel_delta_ml").isNotNull())
        & (F.col("fuel_delta_ml") >= 0)  # Filter out negative values
    )

    # Add previous timestamp for interval calculation
    window_spec = Window.partitionBy("org_id", "object_id").orderBy("timestamp")

    fuel_intervals = (
        fuel_df.withColumn("prev_timestamp", F.lag("timestamp").over(window_spec))
        .filter(F.col("prev_timestamp").isNotNull())
        .select("org_id", "object_id", "prev_timestamp", "timestamp", "fuel_delta_ml")
    )

    # Apply engine engage event filtering
    fuel_with_engine = filter_by_engine_engage_events(
        spark, fuel_intervals, start_date, end_date, object_ids
    )

    # Apply max hourly filtering
    fuel_filtered = filter_max_fuel_by_hour(
        fuel_with_engine,
        MAX_FUEL_CONSUMED_ML_BY_HOUR,
        "fuel_delta_ml",
        "fuel_consumed_ml",
    )

    # Apportion to 5-minute intervals using helper function
    result_df = apportion_interval_values_to_5min_intervals(
        fuel_filtered, intervals_df, "fuel_consumed_ml", "fuel_consumed_ml"
    )

    return result_df


def get_engine_off_intervals(
    spark: SparkSession,
    start_date: str,
    end_date: str,
    object_ids: Optional[List[int]] = None,
) -> DataFrame:
    """
    Build engine OFF intervals from osDEngineState.
    EngineState_Off = 0 (enginestateaccessor.EngineState_Off)

    Replicates buildEngineStateIntervals logic in consumption_info_fetcher.go:
    1. Time offset correction: Adjusts timestamp by offset_ms for valid offset types
       - Only applies for MIN_IDLE_DURATION and ENGINE_ACTIVITY_TIMEOUT
       - Caps offset at 10 minutes
    2. DataBreak handling: If a stat has is_databreak=true, the previous state
       is treated as OFF
    3. Filter to only OFF intervals

    Returns DataFrame with columns: org_id, object_id, off_start_ts, off_end_ts
    """
    OFFSET_TYPE_MIN_IDLE_DURATION = (
        1  # hubproto.ObjectStatBinaryMessage_EngineState_MIN_IDLE_DURATION
    )
    OFFSET_TYPE_ENGINE_ACTIVITY_TIMEOUT = (
        2  # hubproto.ObjectStatBinaryMessage_EngineState_ENGINE_ACTIVITY_TIMEOUT
    )
    MAX_ENGINE_STATE_OFFSET_MS = 10 * 60 * 1000

    engine_state_raw = spark.table("kinesisstats.osdenginestate").filter(
        F.col("date").between(F.lit(start_date), F.lit(end_date))
    )

    if object_ids:
        engine_state_raw = engine_state_raw.filter(F.col("object_id").isin(object_ids))

    # Extract engine state values including offset and databreak info
    engine_states = engine_state_raw.select(
        "org_id",
        "object_id",
        F.col("time").alias(
            "time_ms"
        ),  # Keep original time in ms for offset calculation
        F.col("value.int_value").alias("engine_state"),
        F.coalesce(
            F.col("value.proto_value.engine_state.offset_type"),
            F.lit(0),
        ).alias("offset_type"),
        F.coalesce(
            F.col("value.proto_value.engine_state.offset_ms"),
            F.lit(0),
        ).alias("offset_ms"),
        F.coalesce(F.col("value.is_databreak"), F.lit(False)).alias("is_databreak"),
    )

    # Apply time offset correction in engineStateTimeWithOffset
    engine_states = engine_states.withColumn(
        "timestamp",
        (
            F.when(
                (F.col("offset_type") == OFFSET_TYPE_MIN_IDLE_DURATION)
                | (F.col("offset_type") == OFFSET_TYPE_ENGINE_ACTIVITY_TIMEOUT),
                F.col("time_ms")
                - F.least(F.col("offset_ms"), F.lit(MAX_ENGINE_STATE_OFFSET_MS)),
            ).otherwise(F.col("time_ms"))
            / 1000
        ).cast("timestamp"),
    )

    window_spec = Window.partitionBy("org_id", "object_id").orderBy("timestamp")

    engine_intervals = engine_states.select(
        "org_id",
        "object_id",
        "timestamp",
        "engine_state",
        "is_databreak",
        # All window functions computed in single pass
        F.lead("timestamp").over(window_spec).alias("next_timestamp"),
        F.lead("is_databreak")
        .over(window_spec)
        .alias("next_is_databreak"),
        F.lag("timestamp").over(window_spec).alias("prev_timestamp"),
    ).filter(
        # Must have a next state to form an interval
        F.col("next_timestamp").isNotNull()
    )

    # Handle overlapping timestamps after offset adjustment and apply DataBreak logic
    # Go: if correctedStat.Time <= correctedStats[prevIdx].Time, keep most recent
    # DataBreak: if next stat is databreak, treat current state as OFF
    engine_intervals = engine_intervals.filter(
        # Remove rows where current timestamp <= previous (overlap after offset adjustment)
        F.col("prev_timestamp").isNull()
        | (F.col("timestamp") > F.col("prev_timestamp"))
    ).withColumn(
        "effective_engine_state",
        F.when(
            F.col("next_is_databreak") == True,
            F.lit(0),
        ).otherwise(F.col("engine_state")),
    )

    # Filter to get only engine OFF intervals
    off_intervals = engine_intervals.filter(
        F.col("effective_engine_state") == 0
    ).select(
        "org_id",
        "object_id",
        F.col("timestamp").alias("off_start_ts"),
        F.col("next_timestamp").alias("off_end_ts"),
    )

    return off_intervals


def calculate_off_duration_per_fuel_interval(
    fuel_df: DataFrame,
    engine_off_intervals_df: DataFrame,
) -> DataFrame:
    """
    For each fuel interval, calculate the total engine OFF duration within it.

    Returns fuel_df with additional column: off_duration_seconds
    """
    # Join fuel intervals with engine OFF intervals to find overlaps
    # Using left join to keep fuel intervals without OFF overlaps (those get off_duration=0)
    joined = (
        fuel_df.alias("f")
        .join(
            engine_off_intervals_df.alias("o"),
            (F.col("f.org_id") == F.col("o.org_id"))
            & (F.col("f.object_id") == F.col("o.object_id"))
            & (F.col("f.prev_timestamp") < F.col("o.off_end_ts"))
            & (F.col("f.timestamp") > F.col("o.off_start_ts")),
            "left",
        )
    )

    # Calculate overlap duration with each engine OFF interval
    joined = joined.withColumn(
        "off_overlap_seconds",
        F.when(
            F.col("o.off_start_ts").isNotNull(),
            (
                F.least(F.col("f.timestamp"), F.col("o.off_end_ts")).cast("long")
                - F.greatest(F.col("f.prev_timestamp"), F.col("o.off_start_ts")).cast(
                    "long"
                )
            ),
        ).otherwise(0),
    )

    # Group by fuel interval to sum all engine OFF overlaps
    result = joined.groupBy(
        F.col("f.org_id").alias("org_id"),
        F.col("f.object_id").alias("object_id"),
        F.col("f.prev_timestamp").alias("prev_timestamp"),
        F.col("f.timestamp").alias("timestamp"),
    ).agg(
        F.sum("off_overlap_seconds").alias("off_duration_seconds"),
        F.first(F.col("f.fuel_delta_ml")).alias("fuel_delta_ml"),
    )

    # Replace null off_duration with 0
    result = result.withColumn(
        "off_duration_seconds",
        F.coalesce(F.col("off_duration_seconds"), F.lit(0)),
    )

    return result


def filter_by_engine_engage_events(
    spark: SparkSession,
    fuel_df: DataFrame,
    start_date: str,
    end_date: str,
    object_ids: Optional[List[int]] = None,
) -> DataFrame:
    """
    Filter fuel consumption to only include periods within engine engage events.

    Replicates engine activity logic in consumption_info_fetcher.go:
    overlap_pct = overlap_duration / (total_duration - off_duration)

    1. Engine OFF intervals are calculated from osDEngineState
    2. OFF duration is subtracted from the denominator in overlap calculation
    3. This effectively "redistributes" fuel from OFF periods to ON periods
    """

    # Get engine OFF intervals
    off_intervals = get_engine_off_intervals(spark, start_date, end_date, object_ids)

    # Calculate OFF duration for each fuel interval to be used in overlap calculation
    fuel_with_off = calculate_off_duration_per_fuel_interval(fuel_df, off_intervals)

    # Read engine engage events data
    engage_raw = spark.table("engineactivitydb_shards.engine_engage_events").filter(
        F.col("date").between(F.lit(start_date), F.lit(end_date))
    )

    if object_ids:
        engage_raw = engage_raw.filter(F.col("device_id").isin(object_ids))

    engage_raw = engage_raw.select(
        "org_id",
        F.col("device_id").alias("object_id"),
        "start_ms",
        "duration_ms",
        "uuid",
    )

    # Deduplicate engage events by uuid when present; fallback to start/duration
    engage_raw = engage_raw.dropDuplicates(["org_id", "object_id", "uuid"]).drop("uuid")

    # Create engage event intervals
    engage_events = (
        engage_raw.withColumn(
            "event_start_ts", (F.col("start_ms") / 1000).cast("timestamp")
        )
        .withColumn(
            "event_end_ts",
            F.from_unixtime(((F.col("start_ms") + F.col("duration_ms")) / 1000)).cast(
                "timestamp"
            ),
        )
        .select("org_id", "object_id", "event_start_ts", "event_end_ts")
    )

    # Join fuel with engine engage event intervals using aliases to avoid ambiguous references
    # Only keep fuel consumption during engine engage events including engine OFF overlaps
    result = (
        fuel_with_off.alias("f")
        .hint("range_join", RANGE_JOIN_BIN_SIZE_SECONDS)
        .join(
            engage_events.alias("e"),
            (F.col("f.org_id") == F.col("e.org_id"))
            & (F.col("f.object_id") == F.col("e.object_id"))
            & (F.col("f.prev_timestamp") < F.col("e.event_end_ts"))
            & (F.col("f.timestamp") > F.col("e.event_start_ts")),
            "inner",
        )
    )

    # Calculate overlap percentage using formula:
    # overlap_pct = overlap_duration / (interval_duration - engine_off_duration)
    result = (
        result.withColumn(
            "overlap_start",
            F.greatest(F.col("f.prev_timestamp"), F.col("e.event_start_ts")),
        )
        .withColumn(
            "overlap_end", F.least(F.col("f.timestamp"), F.col("e.event_end_ts"))
        )
        .withColumn(
            "overlap_duration",
            (F.col("overlap_end").cast("long") - F.col("overlap_start").cast("long")),
        )
        .withColumn(
            "total_duration",
            (
                F.col("f.timestamp").cast("long")
                - F.col("f.prev_timestamp").cast("long")
            ),
        )
        .withColumn(
            "effective_duration",
            F.col("total_duration") - F.col("f.off_duration_seconds"),
        )
        # effective_duration <= 0 should only happen if OFF duration >= total_duration,
        # which should not happen since result DF only includes fuel intervals with ENGAGE events.
        # Just to be safe, return 0 if effective_duration <= 0.
        .withColumn(
            "overlap_pct",
            F.when(
                F.col("effective_duration") > 0,
                F.col("overlap_duration") / F.col("effective_duration"),
            ).otherwise(
                0.0
            ),
        )
    )

    # Apply overlap percentage to fuel consumption
    select_cols = [
        F.col("f.org_id"),
        F.col("f.object_id"),
        F.col("f.prev_timestamp"),
        F.col("f.timestamp"),
        (F.col("f.fuel_delta_ml") * F.col("overlap_pct")).alias("fuel_delta_ml"),
    ]

    result = result.select(*select_cols)

    # Group by fuel interval to sum overlaps
    group_cols = ["org_id", "object_id", "prev_timestamp", "timestamp"]

    result = result.groupBy(*group_cols).agg(
        F.sum("fuel_delta_ml").alias("fuel_delta_ml")
    )

    return result


def filter_max_fuel_by_hour(
    fuel_df: DataFrame, max_value_by_hour: float, input_column: str, output_column: str
) -> DataFrame:
    """
    Filter fuel consumption that exceeds max hourly threshold.
    Mirrors Go logic: group by hour, filter anomalies.
    """

    # Add hour bucket
    fuel_with_hour = fuel_df.withColumn(
        "hour_bucket", F.date_trunc("hour", F.col("timestamp"))
    )

    # Calculate duration and rate
    fuel_with_rate = (
        fuel_with_hour.withColumn(
            "duration_seconds",
            F.col("timestamp").cast("long") - F.col("prev_timestamp").cast("long"),
        )
        .withColumn("duration_hours", F.col("duration_seconds") / 3600.0)
        .withColumn(
            "rate_per_hour",
            F.when(
                F.col("duration_hours") > 0,
                F.col(input_column) / F.col("duration_hours"),
            ).otherwise(0),
        )
    )

    # Group by hour and sum consumption
    hourly_consumption = fuel_with_rate.groupBy(
        "org_id", "object_id", "hour_bucket"
    ).agg(
        F.sum(input_column).alias("hourly_total"),
        F.collect_list(
            F.struct(
                "prev_timestamp",
                "timestamp",
                F.col(input_column).alias("value"),
                "duration_hours",
            )
        ).alias("segments"),
    )

    # Filter hours that exceed max
    valid_hours = hourly_consumption.filter(F.col("hourly_total") < max_value_by_hour)

    # Explode back to segments
    result = valid_hours.select(
        "org_id", "object_id", F.explode("segments").alias("segment")
    ).select(
        "org_id",
        "object_id",
        F.col("segment.prev_timestamp").alias("prev_timestamp"),
        F.col("segment.timestamp").alias("timestamp"),
        F.col("segment.value").alias(output_column),
    )

    return result


def calculate_grade_metrics(
    spark: SparkSession,
    intervals_df: DataFrame,
    start_date: str,
    end_date: str,
    object_ids: Optional[List[int]] = None,
) -> DataFrame:
    """
    Calculate grade metrics (uphill/downhill duration) from location altitude data.
    """

    # Read location data
    location_raw = spark.table("kinesisstats.location").filter(
        F.col("date").between(F.lit(start_date), F.lit(end_date))
    )

    if object_ids:
        location_raw = location_raw.filter(F.col("device_id").isin(object_ids))

    # Extract location data with speed and altitude
    location_df = location_raw.select(
        "org_id",
        F.col("device_id").alias("object_id"),
        F.col("time"),
        (F.col("time") / 1000).cast("timestamp").alias("timestamp"),
        F.col("value.latitude").alias("latitude"),
        F.col("value.longitude").alias("longitude"),
        F.col("value.gps_speed_meters_per_second").alias("gps_speed_mps"),
        F.col("value.ecu_speed_meters_per_second").alias("ecu_speed_mps"),
        F.col("value.altitude_meters").alias("altitude_meters"),
    ).filter(
        F.col("latitude").isNotNull()
        & F.col("longitude").isNotNull()
        & F.col("altitude_meters").isNotNull()
        & (F.col("latitude").between(-90, 90))
        & (F.col("longitude").between(-180, 180))
    )

    # Filter to only locations within key cycle intervals and carry interval_start
    location_in_cycles = (
        location_df.alias("loc")
        .hint("range_join", RANGE_JOIN_BIN_SIZE_SECONDS)
        .join(
            intervals_df.alias("i"),
            (F.col("loc.org_id") == F.col("i.org_id"))
            & (F.col("loc.object_id") == F.col("i.object_id"))
            & (F.col("loc.timestamp") >= F.col("i.interval_start"))
            & (F.col("loc.timestamp") < F.col("i.interval_end")),
            "inner",
        )
        .select(
            F.col("loc.org_id").alias("org_id"),
            F.col("loc.object_id").alias("object_id"),
            F.col("loc.time").alias("time"),
            F.col("loc.timestamp").alias("timestamp"),
            F.col("loc.latitude").alias("latitude"),
            F.col("loc.longitude").alias("longitude"),
            F.col("loc.gps_speed_mps").alias("gps_speed_mps"),
            F.col("loc.ecu_speed_mps").alias("ecu_speed_mps"),
            F.col("loc.altitude_meters").alias("altitude_meters"),
            F.col("i.interval_start").alias("interval_start"),
        )
    )

    # Apply speed filter (>= 2.2352 m/s which is 5 mph)
    location_filtered = location_in_cycles.filter(
        F.coalesce(F.col("gps_speed_mps"), F.col("ecu_speed_mps"), F.lit(0)) >= 2.2352
    )

    # Add lagged altitude and time (6 rows back, ~30 seconds)
    window_spec = Window.partitionBy("org_id", "object_id").orderBy("time")

    location_with_lag = (
        location_filtered.withColumn(
            "altitude_6_rows_back", F.lag("altitude_meters", 6).over(window_spec)
        )
        .withColumn("time_6_rows_back", F.lag("time", 6).over(window_spec))
        .withColumn("timestamp_6_rows_back", F.lag("timestamp", 6).over(window_spec))
        .withColumn("latitude_6_rows_back", F.lag("latitude", 6).over(window_spec))
        .withColumn("longitude_6_rows_back", F.lag("longitude", 6).over(window_spec))
        .withColumn("duration_ms", F.col("time") - F.lag("time", 1).over(window_spec))
        .withColumn("duration_6_rows_ms", F.col("time") - F.col("time_6_rows_back"))
    )

    # Filter for valid data
    location_valid = location_with_lag.filter(
        (F.col("duration_ms") <= 10000)  # Max 10 seconds between points
        & (F.col("duration_6_rows_ms") >= 30000)  # At least 30 seconds
        & (F.col("duration_6_rows_ms") <= 60000)  # At most 60 seconds
        & F.col("altitude_meters").isNotNull()
        & F.col("altitude_6_rows_back").isNotNull()
        & F.col("latitude_6_rows_back").isNotNull()
        & F.col("longitude_6_rows_back").isNotNull()
    )

    # Calculate real GPS distance over the 6-row window using haversine formula
    grade_with_distance = location_valid.withColumn(
        "cumulative_distance_6_rows_km",
        haversine_distance_km_expr(
            F.col("latitude_6_rows_back"),
            F.col("longitude_6_rows_back"),
            F.col("latitude"),
            F.col("longitude"),
        ),
    ).withColumn(
        "cumulative_distance_6_rows_m", F.col("cumulative_distance_6_rows_km") * 1000
    )

    # Apply distance filters and calculate grade percentage
    grade_calculated = (
        grade_with_distance.filter(
            (
                F.col("cumulative_distance_6_rows_m") >= 67.056
            )  # Min distance threshold (220 feet)
            & (
                F.pow(F.col("cumulative_distance_6_rows_m"), 2)
                > F.pow(F.col("altitude_meters") - F.col("altitude_6_rows_back"), 2)
            )
        )
        .withColumn(
            "vertical_rise", F.col("altitude_meters") - F.col("altitude_6_rows_back")
        )
        .withColumn(
            "horizontal_distance",
            F.sqrt(
                F.pow(F.col("cumulative_distance_6_rows_m"), 2)
                - F.pow(F.col("vertical_rise"), 2)
            ),
        )
        .withColumn(
            "grade_percentage",
            (F.col("vertical_rise") / F.col("horizontal_distance")) * 100,
        )
    )

    # Aggregate using carried interval_start from intervals_df
    result_df = grade_calculated.groupBy("org_id", "object_id", "interval_start").agg(
        F.sum(
            F.when(F.col("grade_percentage") > 2, F.col("duration_ms")).otherwise(0)
        ).alias("uphill_duration_ms"),
        F.sum(
            F.when(F.col("grade_percentage") < -2, F.col("duration_ms")).otherwise(0)
        ).alias("downhill_duration_ms"),
    )

    # Ensure all intervals are present (fill missing with 0)
    final_result = intervals_df.join(
        result_df, ["org_id", "object_id", "interval_start"], "left"
    ).select(
        "org_id",
        "object_id",
        "interval_start",
        F.coalesce(F.col("uphill_duration_ms"), F.lit(0)).alias("uphill_duration_ms"),
        F.coalesce(F.col("downhill_duration_ms"), F.lit(0)).alias(
            "downhill_duration_ms"
        ),
    )

    return final_result


def calculate_braking_metrics(
    spark: SparkSession,
    intervals_df: DataFrame,
    start_date: str,
    end_date: str,
    object_ids: Optional[List[int]] = None,
) -> DataFrame:
    """Calculate braking metrics including wear-free and total braking durations."""

    # Define braking types and their tables
    braking_types = [
        ("regen", "kinesisstats.osDRegenBrakingMs"),
        ("retarder", "kinesisstats.osDAggregateDurationRetarderBrakingMs"),
        ("friction", "kinesisstats.osDAggregateDurationFrictionBrakingMs"),
    ]

    all_braking_dfs = []

    for brake_type, table_name in braking_types:
        # Read braking data
        braking_df = spark.table(table_name)

        if object_ids:
            braking_df = braking_df.filter(F.col("object_id").isin(object_ids))

        # Clean and prepare braking data
        cleaned_df = (
            braking_df.select(
                "org_id",
                "object_id",
                "time",
                F.col("value.int_value").alias("duration_ms"),
            )
            .filter(F.col("date").between(F.lit(start_date), F.lit(end_date)))
            .filter(F.col("duration_ms").isNotNull())
        )

        # Add timestamp
        cleaned_df = cleaned_df.withColumn(
            "timestamp", (F.col("time") / 1000).cast("timestamp")
        )

        # Treat value.int_value as delta like original; no cumulative diff
        window_spec = Window.partitionBy("org_id", "object_id").orderBy("time")

        delta_df = (
            cleaned_df.withColumn(
                "prev_timestamp", F.lag("timestamp").over(window_spec)
            )
            .withColumn("delta_ms", F.col("duration_ms"))
            .filter(F.col("delta_ms").isNotNull())
            .filter(F.col("prev_timestamp").isNotNull())
            .filter(F.col("delta_ms") > 0)
            .select(
                "org_id",
                "object_id",
                "prev_timestamp",
                "timestamp",
                F.col("delta_ms").alias(f"{brake_type}_braking_ms"),
            )
        )

        # Apportion to 5-minute intervals using proper overlap logic
        interval_df = apportion_interval_values_to_5min_intervals(
            delta_df,
            intervals_df,
            f"{brake_type}_braking_ms",
            f"{brake_type}_braking_ms",
        )

        all_braking_dfs.append(interval_df)

    combined_df = all_braking_dfs[0]

    for df in all_braking_dfs[1:]:
        combined_df = combined_df.join(
            df, on=["org_id", "object_id", "interval_start"], how="full"
        )

    # Calculate aggregated metrics
    result_df = (
        combined_df.withColumn(
            "wear_free_braking_duration_ms",
            F.coalesce(F.col("regen_braking_ms"), F.lit(0))
            + F.coalesce(F.col("retarder_braking_ms"), F.lit(0)),
        )
        .withColumn(
            "total_braking_duration_ms",
            F.coalesce(F.col("regen_braking_ms"), F.lit(0))
            + F.coalesce(F.col("retarder_braking_ms"), F.lit(0))
            + F.coalesce(F.col("friction_braking_ms"), F.lit(0)),
        )
        .select(
            "org_id",
            "object_id",
            "interval_start",
            "wear_free_braking_duration_ms",
            "total_braking_duration_ms",
        )
    )

    final_df = intervals_df.join(
        result_df, on=["org_id", "object_id", "interval_start"], how="left"
    ).select(
        "org_id",
        "object_id",
        "interval_start",
        F.coalesce(F.col("wear_free_braking_duration_ms"), F.lit(0)).alias(
            "wear_free_braking_duration_ms"
        ),
        F.coalesce(F.col("total_braking_duration_ms"), F.lit(0)).alias(
            "total_braking_duration_ms"
        ),
    )

    return final_df


def calculate_cruise_coasting_metrics(
    spark: SparkSession,
    intervals_df: DataFrame,
    start_date: str,
    end_date: str,
    object_ids: Optional[List[int]] = None,
) -> DataFrame:
    """Calculate cruise control and coasting durations from derived stats."""

    # Define metrics and their tables
    metrics = [
        ("cruise_control", "kinesisstats.osdderivedcruisecontrolms"),
        ("coasting", "kinesisstats.osdderivedcoastingtimems"),
    ]

    all_metrics_dfs = []

    for metric_name, table_name in metrics:
        # Read metric data
        metric_df = spark.table(table_name)

        if object_ids:
            metric_df = metric_df.filter(F.col("object_id").isin(object_ids))

        # Clean and prepare metric data
        cleaned_df = (
            metric_df.select(
                "org_id",
                "object_id",
                "time",
                F.col("value.int_value").alias("cumulative_ms"),
            )
            .filter(F.col("date").between(F.lit(start_date), F.lit(end_date)))
            .filter(F.col("cumulative_ms").isNotNull())
        )

        # Add timestamp
        cleaned_df = cleaned_df.withColumn(
            "timestamp", (F.col("time") / 1000).cast("timestamp")
        )

        # Calculate delta from cumulative value
        window_spec = Window.partitionBy("org_id", "object_id").orderBy("time")

        delta_df = (
            cleaned_df.withColumn(
                "prev_cumulative", F.lag("cumulative_ms").over(window_spec)
            )
            .withColumn("prev_timestamp", F.lag("timestamp").over(window_spec))
            .withColumn(
                "delta_ms",
                F.when(
                    F.col("cumulative_ms") >= F.col("prev_cumulative"),
                    F.col("cumulative_ms") - F.col("prev_cumulative"),
                ).otherwise(
                    F.col("cumulative_ms")
                ),  # Handle counter reset
            )
            .filter(F.col("delta_ms").isNotNull())
            .filter(F.col("prev_timestamp").isNotNull())
            .filter(F.col("delta_ms") > 0)
            .filter(F.col("delta_ms") <= 300000)  # Max 5 minutes between points
            .select(
                "org_id",
                "object_id",
                "prev_timestamp",
                "timestamp",
                F.col("delta_ms").alias(f"total_{metric_name}_ms"),
            )
        )

        # Apportion to 5-minute intervals using proper overlap logic
        interval_df = apportion_interval_values_to_5min_intervals(
            delta_df, intervals_df, f"total_{metric_name}_ms", f"total_{metric_name}_ms"
        )

        all_metrics_dfs.append(interval_df)

    # Combine all metrics
    if all_metrics_dfs:
        # Start with the first DataFrame
        combined_df = all_metrics_dfs[0]

        # Full outer join with the rest
        for df in all_metrics_dfs[1:]:
            combined_df = combined_df.join(
                df, on=["org_id", "object_id", "interval_start"], how="full"
            )

        # Ensure all intervals have metrics
        result_df = intervals_df.join(
            combined_df, on=["org_id", "object_id", "interval_start"], how="left"
        ).select(
            "org_id",
            "object_id",
            "interval_start",
            F.coalesce(F.col("total_cruise_control_ms"), F.lit(0)).alias(
                "total_cruise_control_ms"
            ),
            F.coalesce(F.col("total_coasting_ms"), F.lit(0)).alias("total_coasting_ms"),
        )

        return result_df
    else:
        # Return empty metrics if no data
        return intervals_df.withColumn("total_cruise_control_ms", F.lit(0)).withColumn(
            "total_coasting_ms", F.lit(0)
        )


def calculate_vehicle_weight_metrics(
    spark: SparkSession,
    key_cycles_df: DataFrame,
    intervals_df: DataFrame,
    start_date: str,
    end_date: str,
    key_cycle_filters: Optional[DataFrame] = None,
    object_ids: Optional[List[int]] = None,
) -> DataFrame:
    """
    Calculate vehicle weight metrics on the provided 5-minute intervals using
    time-weighted averaging and a 20-minute carry-forward cap.

    Behavior:
      - Build weight segments from successive readings [timestamp, next_timestamp)
      - Cap segment length to at most 20 minutes to limit carry-forward
      - For each interval, compute time-weighted average:
            average_weight_kg = sum(weight_kg * overlap_ms) / sum(overlap_ms)
      - Join directly to intervals_df; no pre-bucketing by timestamp
      - Forward-fill missing vehicle weight values for intervals between weight reports
    """

    # Read raw weights (no history table, as requested)
    weight_df = spark.table(
        "kinesisstats.osdj1939cvwgrosscombinationvehicleweightkilograms"
    )

    if object_ids:
        weight_df = weight_df.filter(F.col("object_id").isin(object_ids))

    cleaned_df = (
        weight_df.select(
            "org_id", "object_id", "time", F.col("value.int_value").alias("weight_kg")
        )
        .filter(F.col("date").between(F.date_sub(F.lit(start_date), 1), F.lit(end_date)))
        .filter(F.col("weight_kg").isNotNull())
        .filter((F.col("weight_kg") > 0) & (F.col("weight_kg") < 100000))
    )

    # Optional filter: filter by key_cycle_filters if provided (reduces data before left join)
    if key_cycle_filters is not None:
        cleaned_df = cleaned_df.join(
            key_cycle_filters,
            on=["org_id", "object_id"],
            how="inner",
        )

    # Add reading timestamp
    cleaned_df = cleaned_df.withColumn(
        "timestamp", (F.col("time") / 1000).cast("timestamp")
    )

    # Build capped segments per device: [segment_start, segment_end)
    window_spec = Window.partitionBy("org_id", "object_id").orderBy("timestamp")

    segments_df = (
        cleaned_df.withColumn("next_timestamp", F.lead("timestamp").over(window_spec))
        .withColumn("segment_start", F.col("timestamp"))
        .withColumn("carry_cap_end", F.col("timestamp") + F.expr("interval 3 hours"))
        .withColumn(
            "segment_end",
            F.when(
                F.col("next_timestamp").isNotNull(),
                F.least(F.col("next_timestamp"), F.col("carry_cap_end")),
            ).otherwise(F.col("carry_cap_end")),
        )
        .select("org_id", "object_id", "segment_start", "segment_end", "weight_kg")
    )

    joined = (
        intervals_df.alias("i")
        .hint("range_join", RANGE_JOIN_BIN_SIZE_SECONDS)
        .join(
            segments_df.alias("w"),
            (F.col("i.org_id") == F.col("w.org_id"))
            & (F.col("i.object_id") == F.col("w.object_id"))
            & (F.col("w.segment_end") > F.col("i.interval_start"))
            & (F.col("w.segment_start") < F.col("i.interval_end")),
            "inner",
        )
        .withColumn(
            "overlap_start",
            F.greatest(F.col("i.interval_start"), F.col("w.segment_start")),
        )
        .withColumn(
            "overlap_end", F.least(F.col("i.interval_end"), F.col("w.segment_end"))
        )
        .withColumn(
            "overlap_ms",
            F.when(
                (F.col("overlap_end") > F.col("overlap_start")),
                (F.unix_timestamp("overlap_end") - F.unix_timestamp("overlap_start"))
                * F.lit(1000),
            ).otherwise(F.lit(0)),
        )
        .withColumn("weighted_weight", F.col("w.weight_kg") * F.col("overlap_ms"))
    )

    # Aggregate per interval
    weight_metrics_df = joined.groupBy(
        "i.org_id", "i.object_id", "i.interval_start"
    ).agg(
        F.sum("overlap_ms").alias("weight_time"),
        F.sum("weighted_weight").alias("weighted_weight"),
    )

    # Join back to intervals to ensure full coverage, compute average
    fill_forward_window_spec = (
        Window.partitionBy("org_id", "object_id")
        .orderBy("interval_start")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    result_df = (
        intervals_df.join(
            weight_metrics_df,
            ["org_id", "object_id", "interval_start"],
            "left",
        )
        .select(
            "org_id",
            "object_id",
            "interval_start",
            F.coalesce(
                F.last(
                    F.when(F.col("weight_time") != 0, F.col("weight_time")),
                    ignorenulls=True,
                ).over(fill_forward_window_spec),
                F.lit(0),
            ).alias("weight_time"),
            F.coalesce(
                F.last(
                    F.when(F.col("weighted_weight") != 0, F.col("weighted_weight")),
                    ignorenulls=True,
                ).over(fill_forward_window_spec),
                F.lit(0),
            ).alias("weighted_weight"),
        )
        .withColumn(
            "average_weight_kg",
            F.when(
                F.col("weight_time") > 0,
                F.col("weighted_weight") / F.col("weight_time"),
            ),
        )
    )
    return result_df


def calculate_speed_acceleration_buckets(
    spark,
    key_cycles_df: DataFrame,
    intervals_df: DataFrame,
    start_date: str,
    end_date: str,
    object_ids: Optional[List[int]] = None,
) -> DataFrame:
    """Time-in-bucket metrics using forward Δt with midpoint speed (physically consistent)."""

    # --- Load & filter by date / devices
    location_df = spark.table("kinesisstats.location")
    filtered_df = location_df.filter(
        F.col("date").between(F.lit(start_date), F.lit(end_date))
    )

    if object_ids:
        filtered_df = filtered_df.filter(F.col("device_id").isin(object_ids))

    # --- Clean: keep valid lat/lon; build speed in m/s (no imputation); add timestamp
    cleaned_df = (
        filtered_df.select(
            "org_id",
            F.col("device_id").alias("object_id"),
            "time",
            F.col("value.latitude").alias("latitude"),
            F.col("value.longitude").alias("longitude"),
            F.coalesce(
                F.col("value.gps_speed_meters_per_second"),
                F.col("value.ecu_speed_meters_per_second"),
            ).alias("speed_mps"),
        )
        .filter(F.col("latitude").isNotNull())
        .filter(F.col("longitude").isNotNull())
        .filter(F.col("speed_mps").isNotNull())
        .withColumn("timestamp", (F.col("time") / 1000).cast("timestamp"))
        .withColumn(
            "speed_mph", F.col("speed_mps") * F.lit(2.23694)
        )  # optional; not used for bucketing
    )

    # --- Restrict to key cycles
    in_cycles_df = cleaned_df.join(
        key_cycles_df, on=["org_id", "object_id"], how="inner"
    ).filter(
        (F.col("timestamp") >= F.col("cycle_start_time"))
        & (F.col("timestamp") <= F.col("cycle_end_time"))
    )

    # --- Forward Δt (current → next), midpoint speed, and acceleration over the same span
    w = Window.partitionBy("org_id", "object_id").orderBy("time")

    with_pairs = (
        in_cycles_df.withColumn("next_time", F.lead("time").over(w))
        .withColumn("next_speed_mps", F.lead("speed_mps").over(w))
        .withColumn("delta_s", (F.col("next_time") - F.col("time")) / F.lit(1000.0))
        # Require a proper pair, small gap, and finite values
        .filter(F.col("delta_s") > 0)
        .filter(F.col("delta_s") <= 60)
        .filter(F.col("next_speed_mps").isNotNull())
        .withColumn(
            "avg_speed_mps", (F.col("speed_mps") + F.col("next_speed_mps")) / F.lit(2.0)
        )
        .withColumn(
            "acceleration_mps2",
            (F.col("next_speed_mps") - F.col("speed_mps")) / F.col("delta_s"),
        )
        .withColumn("time_delta_ms", F.col("delta_s") * F.lit(1000.0))
    )

    @dataclass
    class BucketThreshold:
        threshold_value: float
        label_value: str

        @staticmethod
        def from_mph(mph_value: float, column_name: str) -> "BucketThreshold":
            return BucketThreshold(mph_value * 0.44704, column_name)

        @staticmethod
        def from_kph(kph_value: float, column_name: str) -> "BucketThreshold":
            return BucketThreshold(kph_value * 0.277778, column_name)

        def as_lit(self):
            return F.lit(self.threshold_value)

        def to_case(
            self, filter_column_name: str, previous_case: Optional[Column] = None
        ) -> Column:
            if previous_case is None:
                return F.when(
                    F.col(filter_column_name) < self.as_lit(), self.label_value
                )

            return previous_case.when(
                F.col(filter_column_name) < self.as_lit(), self.label_value
            )

    def get_bucket_case_statements(
        buckets: List[BucketThreshold], filter_column_name: str, default_value: str
    ) -> Column:
        head = buckets[0].to_case(filter_column_name)
        return reduce(
            lambda case_col, bucket: bucket.to_case(filter_column_name, case_col),
            buckets[1:],
            head,
        ).otherwise(default_value)

    speed_thresholds = [
        BucketThreshold.from_mph(25, "speed_0_25mph"),
        BucketThreshold.from_kph(80, "speed_25mph_80kph"),
        BucketThreshold.from_mph(50, "speed_80kph_50mph"),
        BucketThreshold.from_mph(55, "speed_50mph_55mph"),
        BucketThreshold.from_kph(100, "speed_55mph_100kph"),
        BucketThreshold.from_mph(65, "speed_100kph_65mph"),
    ]

    with_speed_buckets = with_pairs.withColumn(
        "speed_bin",
        get_bucket_case_statements(speed_thresholds, "avg_speed_mps", "speed_65_plus"),
    )

    # --- Acceleration buckets (m/s^2)
    with_accel_buckets = with_speed_buckets.withColumn(
        "accel_bin",
        F.when(F.col("acceleration_mps2") < -1.5, "accel_hard_braking")
        .when(F.col("acceleration_mps2") < -0.5, "accel_mod_braking")
        .when(F.col("acceleration_mps2") <= 0.5, "accel_cruise_coast")
        .when(F.col("acceleration_mps2") <= 1.5, "accel_mod_accel")
        .otherwise("accel_hard_accel"),
    )

    # --- 5-minute interval
    with_interval = with_accel_buckets.withColumn(
        "interval_start",
        F.from_unixtime(F.floor(F.unix_timestamp("timestamp") / 300) * 300).cast(
            "timestamp"
        ),
    )

    # --- Aggregate ms by bucket within each interval
    aggregated = with_interval.groupBy("org_id", "object_id", "interval_start").agg(
        # Speed buckets
        F.sum(
            F.when(
                F.col("speed_bin") == "speed_0_25mph", F.col("time_delta_ms")
            ).otherwise(F.lit(0))
        ).alias("ms_in_speed_0_25mph"),
        F.sum(
            F.when(
                F.col("speed_bin") == "speed_25mph_80kph", F.col("time_delta_ms")
            ).otherwise(F.lit(0))
        ).alias("ms_in_speed_25mph_80kph"),
        F.sum(
            F.when(
                F.col("speed_bin") == "speed_80kph_50mph", F.col("time_delta_ms")
            ).otherwise(F.lit(0))
        ).alias("ms_in_speed_80kph_50mph"),
        F.sum(
            F.when(
                F.col("speed_bin") == "speed_50mph_55mph", F.col("time_delta_ms")
            ).otherwise(F.lit(0))
        ).alias("ms_in_speed_50mph_55mph"),
        F.sum(
            F.when(
                F.col("speed_bin") == "speed_55mph_100kph", F.col("time_delta_ms")
            ).otherwise(F.lit(0))
        ).alias("ms_in_speed_55mph_100kph"),
        F.sum(
            F.when(
                F.col("speed_bin") == "speed_100kph_65mph", F.col("time_delta_ms")
            ).otherwise(F.lit(0))
        ).alias("ms_in_speed_100kph_65mph"),
        F.sum(
            F.when(
                F.col("speed_bin") == "speed_65_plus", F.col("time_delta_ms")
            ).otherwise(F.lit(0))
        ).alias("ms_in_speed_65_plus_mph"),
        # Accel buckets
        F.sum(
            F.when(
                F.col("accel_bin") == "accel_hard_braking", F.col("time_delta_ms")
            ).otherwise(F.lit(0))
        ).alias("ms_in_accel_hard_braking"),
        F.sum(
            F.when(
                F.col("accel_bin") == "accel_mod_braking", F.col("time_delta_ms")
            ).otherwise(F.lit(0))
        ).alias("ms_in_accel_mod_braking"),
        F.sum(
            F.when(
                F.col("accel_bin") == "accel_cruise_coast", F.col("time_delta_ms")
            ).otherwise(F.lit(0))
        ).alias("ms_in_accel_cruise_coast"),
        F.sum(
            F.when(
                F.col("accel_bin") == "accel_mod_accel", F.col("time_delta_ms")
            ).otherwise(F.lit(0))
        ).alias("ms_in_accel_mod_accel"),
        F.sum(
            F.when(
                F.col("accel_bin") == "accel_hard_accel", F.col("time_delta_ms")
            ).otherwise(F.lit(0))
        ).alias("ms_in_accel_hard_accel"),
    )

    # --- Join to intervals table (ensure zeros for missing buckets)
    result_df = intervals_df.join(
        aggregated, on=["org_id", "object_id", "interval_start"], how="left"
    ).select(
        "org_id",
        "object_id",
        "interval_start",
        F.coalesce(F.col("ms_in_speed_0_25mph"), F.lit(0)).alias("ms_in_speed_0_25mph"),
        F.coalesce(F.col("ms_in_speed_25mph_80kph"), F.lit(0)).alias(
            "ms_in_speed_25mph_80kph"
        ),
        F.coalesce(F.col("ms_in_speed_80kph_50mph"), F.lit(0)).alias(
            "ms_in_speed_80kph_50mph"
        ),
        F.coalesce(F.col("ms_in_speed_50mph_55mph"), F.lit(0)).alias(
            "ms_in_speed_50mph_55mph"
        ),
        F.coalesce(F.col("ms_in_speed_55mph_100kph"), F.lit(0)).alias(
            "ms_in_speed_55mph_100kph"
        ),
        F.coalesce(F.col("ms_in_speed_100kph_65mph"), F.lit(0)).alias(
            "ms_in_speed_100kph_65mph"
        ),
        F.coalesce(F.col("ms_in_speed_65_plus_mph"), F.lit(0)).alias(
            "ms_in_speed_65_plus_mph"
        ),
        F.coalesce(F.col("ms_in_accel_hard_braking"), F.lit(0)).alias(
            "ms_in_accel_hard_braking"
        ),
        F.coalesce(F.col("ms_in_accel_mod_braking"), F.lit(0)).alias(
            "ms_in_accel_mod_braking"
        ),
        F.coalesce(F.col("ms_in_accel_cruise_coast"), F.lit(0)).alias(
            "ms_in_accel_cruise_coast"
        ),
        F.coalesce(F.col("ms_in_accel_mod_accel"), F.lit(0)).alias(
            "ms_in_accel_mod_accel"
        ),
        F.coalesce(F.col("ms_in_accel_hard_accel"), F.lit(0)).alias(
            "ms_in_accel_hard_accel"
        ),
    )

    return result_df


def calculate_engine_durations(
    spark: SparkSession,
    intervals_df: DataFrame,
    engine_events_table: str,
    start_date: str,
    end_date: str,
    key_cycle_filters: Optional[DataFrame] = None,
    object_ids: Optional[List[int]] = None,
) -> DataFrame:
    """
    Compute on_duration_ms by apportioning engage events onto the provided 5-minute intervals.

    - on_duration_ms: from engineactivitydb_shards.engine_engage_events

    Returns a DataFrame with: org_id, object_id, interval_start, on_duration_ms, idle_duration_ms
    """

    # ------------------ Engage events -> on_duration_ms ------------------
    engage_raw = spark.table(engine_events_table).filter(
        F.col("date").between(F.lit(start_date), F.lit(end_date))
    )

    if object_ids:
        engage_raw = engage_raw.filter(F.col("device_id").isin(object_ids))

    engage_raw = engage_raw.select(
        "org_id",
        F.col("device_id").alias("object_id"),
        "start_ms",
        "duration_ms",
        "uuid",
    )

    # Filter by key_cycle_filters if provided (reduces data before left join)
    if key_cycle_filters is not None:
        device_filter = key_cycle_filters.select("org_id", "object_id").distinct()
        engage_raw = engage_raw.join(device_filter, ["org_id", "object_id"], "inner")

    # Deduplicate engage events by uuid when present; fallback to start/duration
    engage_raw = engage_raw.dropDuplicates(["org_id", "object_id", "uuid"]).drop("uuid")

    engage_events = engage_raw.withColumn(
        "event_start_ts", (F.col("start_ms") / 1000).cast("timestamp")
    ).withColumn(
        "event_end_ts",
        F.from_unixtime(((F.col("start_ms") + F.col("duration_ms")) / 1000)).cast(
            "timestamp"
        ),
    )

    # Left join for interval overlap - range_join hint doesn't help here,
    # so we rely on key_cycle_filters to reduce data volume instead
    engage_joined = (
        intervals_df.alias("i")
        .join(
            engage_events.alias("e"),
            (F.col("i.org_id") == F.col("e.org_id"))
            & (F.col("i.object_id") == F.col("e.object_id"))
            & (F.col("e.event_end_ts") > F.col("i.interval_start"))
            & (F.col("e.event_start_ts") < F.col("i.interval_end")),
            "left",
        )
        .withColumn(
            "overlap_start",
            F.when(
                F.col("e.event_start_ts").isNotNull()
                & F.col("e.event_end_ts").isNotNull(),
                F.greatest(F.col("i.interval_start"), F.col("e.event_start_ts")),
            ),
        )
        .withColumn(
            "overlap_end",
            F.when(
                F.col("e.event_start_ts").isNotNull()
                & F.col("e.event_end_ts").isNotNull(),
                F.least(F.col("i.interval_end"), F.col("e.event_end_ts")),
            ),
        )
        .withColumn(
            "overlap_ms",
            F.when(
                F.col("overlap_end").isNotNull()
                & F.col("overlap_start").isNotNull()
                & (F.col("overlap_end") > F.col("overlap_start")),
                (F.unix_timestamp("overlap_end") - F.unix_timestamp("overlap_start"))
                * F.lit(1000),
            ).otherwise(F.lit(0)),
        )
    )

    on_durations = engage_joined.groupBy(
        "i.org_id", "i.object_id", "i.interval_start"
    ).agg(F.sum("overlap_ms").alias("duration_ms"))

    return on_durations


def calculate_accelerator_pedal_metrics(
    spark: SparkSession,
    intervals_df: DataFrame,
    start_date: str,
    end_date: str,
    object_ids: Optional[List[int]] = None,
) -> DataFrame:
    """Calculate accelerator pedal > 95% time from derived stats."""

    # Read accelerator pedal data
    accel_df = spark.table(
        "kinesisstats.osDDerivedAcceleratorPedalTimeGreaterThan95PercentMs"
    )

    if object_ids:
        accel_df = accel_df.filter(F.col("object_id").isin(object_ids))

    # Clean and prepare data
    cleaned_df = (
        accel_df.select(
            "org_id",
            "object_id",
            "time",
            F.col("value.int_value").alias("cumulative_ms"),
        )
        .filter(F.col("date").between(F.lit(start_date), F.lit(end_date)))
        .filter(F.col("cumulative_ms").isNotNull())
    )

    # Add timestamp
    cleaned_df = cleaned_df.withColumn(
        "timestamp", (F.col("time") / 1000).cast("timestamp")
    )

    # Calculate delta from cumulative value
    window_spec = Window.partitionBy("org_id", "object_id").orderBy("time")

    delta_df = (
        cleaned_df.withColumn(
            "prev_cumulative", F.lag("cumulative_ms").over(window_spec)
        )
        .withColumn("prev_timestamp", F.lag("timestamp").over(window_spec))
        .withColumn(
            "delta_ms",
            F.when(
                F.col("cumulative_ms") >= F.col("prev_cumulative"),
                F.col("cumulative_ms") - F.col("prev_cumulative"),
            ).otherwise(
                F.col("cumulative_ms")
            ),  # Handle counter reset
        )
        .filter(F.col("delta_ms").isNotNull())
        .filter(F.col("prev_timestamp").isNotNull())
        .filter(F.col("delta_ms") > 0)
        .filter(F.col("delta_ms") <= 300000)  # Max 5 minutes between points
        .select(
            "org_id",
            "object_id",
            "prev_timestamp",
            "timestamp",
            F.col("delta_ms").alias("accelerator_pedal_time_gt95_ms"),
        )
    )

    # Apportion to 5-minute intervals using proper overlap logic
    result_df = apportion_interval_values_to_5min_intervals(
        delta_df,
        intervals_df,
        "accelerator_pedal_time_gt95_ms",
        "accelerator_pedal_time_gt95_ms",
    )

    # Ensure all intervals have metrics
    final_result = intervals_df.join(
        result_df, on=["org_id", "object_id", "interval_start"], how="left"
    ).select(
        "org_id",
        "object_id",
        "interval_start",
        F.coalesce(F.col("accelerator_pedal_time_gt95_ms"), F.lit(0)).alias(
            "accelerator_pedal_time_gt95_ms"
        ),
    )

    return final_result


def calculate_engine_temperature(
    spark: SparkSession,
    start_date: str,
    end_date: str,
    intervals_df: DataFrame,
) -> DataFrame:
    temperature_table = (
        spark.table("kinesisstats.osdenginegauge")
        .filter(
            F.col("date").between(F.lit(start_date), F.lit(end_date))
            & F.col("value.proto_value.engine_gauge_event.air_temp_milli_c").isNotNull()
        )
        .select(
            "org_id",
            "object_id",
            (F.col("time") / 1000).cast("timestamp").alias("timestamp"),
            F.col("value.proto_value.engine_gauge_event.air_temp_milli_c").alias(
                "air_temp_milli_c"
            ),
        )
        .withColumn(
            "prev_timestamp",
            F.lag("timestamp").over(
                Window.partitionBy("org_id", "object_id").orderBy("timestamp")
            ),
        )
        .filter(F.col("prev_timestamp").isNotNull())
    )

    return align_delta_to_intervals(
        temperature_table,
        intervals_df,
        "air_temp_milli_c",
        "air_temp_milli_c",
        agg_fn=F.median,
    )


def generate_eco_driving_report(
    spark: SparkSession,
    partition_start: datetime,
    partition_end: datetime,
    org_ids: Optional[List[int]] = None,
    object_ids: Optional[List[int]] = None,
    region: Optional[str] = None,
) -> DataFrame:
    """
    Generate complete eco driving report with all metrics.

    Processes all data in a single pass, letting Spark handle parallelism
    natively across executors. The maximum interval lookback is bounded by
    MAX_INTERVAL_LOOKBACK_DAYS.

    Parameters:
    - partition_start: Start of the partition window (datetime)
    - partition_end: End of the partition window (datetime)
    - org_ids: Optional list of organization IDs to filter
    - object_ids: Optional list of device/object IDs to filter
    - region: Optional region to filter

    Returns:
    - DataFrame with eco driving metrics aggregated by 5-minute intervals
    """

    asset_efficiency_intervals_df = get_asset_efficiency_intervals(
        spark, partition_start, partition_end, org_ids, object_ids
    )

    asset_efficiency_intervals_df = asset_efficiency_intervals_df.cache()

    # Derive date range for kinesisstats queries (handles cross-midnight cycles)
    date_range = asset_efficiency_intervals_df.select(
        F.min(F.to_date("cycle_start_time")).alias("min_date"),
        F.max(F.to_date("cycle_end_time")).alias("max_date"),
    ).first()

    if date_range["min_date"] is None:
        asset_efficiency_intervals_df.unpersist()
        return _create_empty_dataframe(spark)

    start_date = str(date_range["min_date"])
    end_date = str(date_range["max_date"])

    # Load dimension tables (broadcast small ones for efficient joins)
    snapshot_date = F.date_sub(F.current_date(), 2)

    j1939_cable_ids = spark.table("definitions.cable_to_bus_type").filter(
        F.col("bus_type").ilike("%j1939%")
    ).agg(F.collect_set("cable_id").alias("cid")).first()["cid"]

    dim_devices_df = (
        spark.table("datamodel_core.dim_devices")
        .filter(F.col("date") == snapshot_date)
        .select(
            "org_id",
            F.col("device_id").alias("object_id"),
            "associated_vehicle_make",
            "associated_vehicle_model",
            "associated_vehicle_year",
            "associated_vehicle_primary_fuel_type",
            "associated_vehicle_gross_vehicle_weight_rating",
            "associated_vehicle_engine_type",
            "last_cable_id",
        )
        .withColumn(
            "is_j1939", F.col("last_cable_id").isin(j1939_cable_ids)
        )
    )

    dim_organizations_df = F.broadcast(
        spark.table("datamodel_core.dim_organizations")
        .filter(F.col("date") == snapshot_date)
        .select("org_id", "locale")
    )

    org_cells_df = F.broadcast(
        spark.table("clouddb.org_cells").select(
            "org_id", F.col("cell_id").alias("org_cell_id")
        )
    )

    # Create key_cycle_filters for functions where range join doesn't help (left joins)
    key_cycle_filters = asset_efficiency_intervals_df.select(
        "org_id", "object_id"
    ).distinct()

    intervals_df = generate_intervals_from_key_cycles(
        asset_efficiency_intervals_df
    ).cache()

    distance_df = calculate_distance_hierarchical(
        spark, intervals_df, start_date, end_date, region, object_ids
    ).drop("interval_end")

    fuel_df = calculate_fuel_consumption(
        spark, intervals_df, start_date, end_date, object_ids
    ).drop("interval_end")

    grade_df = calculate_grade_metrics(
        spark, intervals_df, start_date, end_date, object_ids
    )

    on_durations_df = calculate_engine_durations(
        spark,
        intervals_df,
        "engineactivitydb_shards.engine_engage_events",
        start_date,
        end_date,
        key_cycle_filters,
        object_ids,
    ).withColumnRenamed("duration_ms", "on_duration_ms")

    idle_durations_df = calculate_engine_durations(
        spark,
        intervals_df,
        "engineactivitydb_shards.engine_idle_events",
        start_date,
        end_date,
        key_cycle_filters,
        object_ids,
    ).withColumnRenamed("duration_ms", "idle_duration_ms")

    weight_df = calculate_vehicle_weight_metrics(
        spark,
        asset_efficiency_intervals_df,
        intervals_df,
        start_date,
        end_date,
        key_cycle_filters,
        object_ids,
    )

    braking_df = calculate_braking_metrics(
        spark, intervals_df, start_date, end_date, object_ids
    )

    cruise_coasting_df = calculate_cruise_coasting_metrics(
        spark, intervals_df, start_date, end_date, object_ids
    )

    speed_accel_df = calculate_speed_acceleration_buckets(
        spark,
        asset_efficiency_intervals_df,
        intervals_df,
        start_date,
        end_date,
        object_ids,
    )

    accel_pedal_df = calculate_accelerator_pedal_metrics(
        spark, intervals_df, start_date, end_date, object_ids
    )

    temperature_df = calculate_engine_temperature(
        spark, start_date, end_date, intervals_df
    )

    final_df = (
        intervals_df.join(
            distance_df, ["org_id", "object_id", "interval_start"], "left"
        )
        .join(fuel_df, ["org_id", "object_id", "interval_start"], "left")
        .join(grade_df, ["org_id", "object_id", "interval_start"], "left")
        .join(on_durations_df, ["org_id", "object_id", "interval_start"], "left")
        .join(idle_durations_df, ["org_id", "object_id", "interval_start"], "left")
        .join(weight_df, ["org_id", "object_id", "interval_start"], "left")
        .join(braking_df, ["org_id", "object_id", "interval_start"], "left")
        .join(cruise_coasting_df, ["org_id", "object_id", "interval_start"], "left")
        .join(speed_accel_df, ["org_id", "object_id", "interval_start"], "left")
        .join(accel_pedal_df, ["org_id", "object_id", "interval_start"], "left")
        .join(temperature_df, ["org_id", "object_id", "interval_start"], "left")
        .join(dim_devices_df, ["org_id", "object_id"], "left")
        .join(dim_organizations_df, ["org_id"], "left")
        .join(org_cells_df, ["org_id"], "left")
    )

    final_df = final_df.withColumn("date", F.to_date(F.col("interval_start")))

    # Reorder columns for clarity
    final_df = final_df.select(
        "org_id",
        F.col("object_id").alias("device_id"),
        "cycle_id",
        "interval_start",
        "date",
        "i.interval_end",
        "enrichment_due_at",
        # Cruise/coasting metrics
        F.coalesce(F.col("total_cruise_control_ms"), F.lit(0)).alias(
            "total_cruise_control_ms"
        ),
        F.coalesce(F.col("total_coasting_ms"), F.lit(0)).alias("total_coasting_ms"),
        # Weight metrics
        F.coalesce(F.col("weight_time"), F.lit(0)).alias("weight_time"),
        F.coalesce(F.col("weighted_weight"), F.lit(0)).alias("weighted_weight"),
        F.col("average_weight_kg"),
        # Grade metrics
        F.coalesce(F.col("uphill_duration_ms"), F.lit(0)).alias("uphill_duration_ms"),
        F.coalesce(F.col("downhill_duration_ms"), F.lit(0)).alias(
            "downhill_duration_ms"
        ),
        # Braking metrics
        F.coalesce(F.col("wear_free_braking_duration_ms"), F.lit(0)).alias(
            "wear_free_braking_duration_ms"
        ),
        F.coalesce(F.col("total_braking_duration_ms"), F.lit(0)).alias(
            "total_braking_duration_ms"
        ),
        # Engine state metrics
        F.coalesce(F.col("on_duration_ms"), F.lit(0)).alias("on_duration_ms"),
        F.coalesce(F.col("idle_duration_ms"), F.lit(0)).alias("idle_duration_ms"),
        # Fuel metrics
        F.coalesce(F.col("fuel_consumed_ml"), F.lit(0)).alias("fuel_consumed_ml"),
        # Distance metrics
        F.coalesce(F.col("distance_km") * F.lit(1000), F.lit(0)).alias(
            "distance_traveled_m"
        ),
        # Location statistics
        F.col("avg_altitude_meters"),
        F.col("stddev_altitude_meters"),
        F.col("stddev_speed_mps"),
        F.col("avg_latitude"),
        F.col("avg_longitude"),
        "altitude_delta_meters",
        "earliest_heading_degrees",
        "latest_heading_degrees",
        "heading_change_degrees",
        "heading_step_mean_absolute_degrees",
        "revgeo_country",
        # Speed buckets
        "ms_in_speed_0_25mph",
        "ms_in_speed_25mph_80kph",
        "ms_in_speed_80kph_50mph",
        "ms_in_speed_50mph_55mph",
        "ms_in_speed_55mph_100kph",
        "ms_in_speed_100kph_65mph",
        F.coalesce(F.col("ms_in_speed_65_plus_mph"), F.lit(0)).alias(
            "ms_in_speed_65_plus_mph"
        ),
        # Acceleration buckets
        F.coalesce(F.col("ms_in_accel_hard_braking"), F.lit(0)).alias(
            "ms_in_accel_hard_braking"
        ),
        F.coalesce(F.col("ms_in_accel_mod_braking"), F.lit(0)).alias(
            "ms_in_accel_mod_braking"
        ),
        F.coalesce(F.col("ms_in_accel_cruise_coast"), F.lit(0)).alias(
            "ms_in_accel_cruise_coast"
        ),
        F.coalesce(F.col("ms_in_accel_mod_accel"), F.lit(0)).alias(
            "ms_in_accel_mod_accel"
        ),
        F.coalesce(F.col("ms_in_accel_hard_accel"), F.lit(0)).alias(
            "ms_in_accel_hard_accel"
        ),
        # OSM road attributes
        F.col("avg_lanes"),
        F.col("avg_speed_limit_kph"),
        F.col("road_type_diversity"),
        F.col("speed_limit_stddev"),
        F.col("predominant_road_type"),
        # Accelerator pedal
        F.coalesce(F.col("accelerator_pedal_time_gt95_ms"), F.lit(0)).alias(
            "accelerator_pedal_time_gt95_ms"
        ),
        # Vehicle information
        F.col("associated_vehicle_make"),
        F.col("associated_vehicle_model"),
        F.col("associated_vehicle_year"),
        F.col("associated_vehicle_primary_fuel_type"),
        F.col("associated_vehicle_gross_vehicle_weight_rating"),
        F.col("associated_vehicle_engine_type"),
        F.col("last_cable_id"),
        F.col("is_j1939"),
        # Organization information
        F.col("locale"),
        F.col("org_cell_id"),
        # Temperature
        F.col("air_temp_milli_c").alias("median_air_temp_milli_c"),
    )

    # Cleanup cached DataFrames
    asset_efficiency_intervals_df.unpersist()
    intervals_df.unpersist()

    return final_df


@table(
    database=Database.FEATURE_STORE,
    description=build_table_description(
        table_desc="""
        Table containing key cycles (engine state change events) and their corresponding eco driving features
        for use in model inference. This table is similar to fct_eco_driving_augmented_reports, but is based
        on key cycle ID + 5 minute interval instead of device + interval.

        This is the hourly-partitioned version of the dataset, partitioned by date and PARTITION_INTERVAL_HOURS-hour windows.
        """,
        row_meaning="""Each row corresponds a 5 minute interval that elapsed during a specific key cycle.""",
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    schema=SCHEMA,
    primary_keys=["date", "hour", "cycle_id", "interval_start"],
    partitioning=HOURLY_PARTITIONS,
    upstreams=[],
    dq_checks=[
        NonEmptyDQCheck(
            name="dq_non_empty_fct_eco_driving_key_cycle_features_hourly",
            # This job processes any newly updated intervals, so an empty resultant dataframe is possible. Hence, make this non-blocking.
            block_before_write=False,
        ),
        NonNullDQCheck(
            name="dq_non_null_fct_eco_driving_key_cycle_features_hourly",
            non_null_columns=[
                "date",
                "hour",
                "org_id",
                "device_id",
                "interval_start",
            ],
            block_before_write=True,
        ),
        PrimaryKeyDQCheck(
            name="dq_pk_fct_eco_driving_key_cycle_features_hourly",
            primary_keys=["date", "hour", "cycle_id", "interval_start"],
        ),
    ],
    backfill_batch_size=1,
    write_mode=WarehouseWriteMode.OVERWRITE,
    regions=get_all_regions(),
    owners=[DATAENGINEERING, SUSTAINABILITY],
    run_config_overrides=create_run_config_overrides(
        driver_instance_type=InstanceType.RD_FLEET_2XLARGE,
        worker_instance_type=InstanceType.RD_FLEET_4XLARGE,
        max_workers=32,
    ),
    notifications=["slack:alerts-sustainability-low-urgency"],
)
def fct_eco_driving_key_cycle_features_hourly(
    context: AssetExecutionContext,
) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    region = get_region_from_context(context)

    # Parse multi-partition key: "date|hour" format (e.g., "2024-12-12|09")
    partition_key = context.partition_key
    date_str, hour_str = partition_key.split("|")

    # The partition key represents when the job runs, but we process the previous
    # completed window. E.g., job runs at 03:00 -> processes 00:00-03:00 window
    # Job runs at 00:00 -> processes 21:00-00:00 (previous day's last window)
    partition_time = datetime.strptime(
        f"{date_str} {hour_str}:00:00", "%Y-%m-%d %H:%M:%S"
    )
    # Look back: partition_end is the partition time, partition_start is PARTITION_INTERVAL_HOURS before
    partition_end = partition_time
    partition_start = partition_end - timedelta(hours=PARTITION_INTERVAL_HOURS)

    # Effective enrichment_due_at window with buffer for delayed stat data
    buffer = timedelta(minutes=ECO_DRIVING_STAT_DATA_DELAY_BUFFER_MINUTES)
    effective_window_start = partition_start - buffer
    effective_window_end = partition_end - buffer

    # Closed Beta Org IDs
    ORG_IDS = [
        874,  # Dohrn for internal testing
        562949953429581,
        20413,
        562949953424899,
        562949953426058,
        562949953427487,
        7000469,
        562949953429933,
        562949953429983,
        562949953425845,
        8004681,
        83585,
        10005811,
        5001627,
        11003326,
        14171,
        4006319,
        70531,
        8002592,
        8936,
    ]

    # Allow configuration of org Ids via run tags.
    if "org_ids" in context.run.tags:
        ORG_IDS = [
            int(org_id.strip())
            for org_id in context.run.tags["org_ids"].split(",")
            if org_id.strip()
        ]

    # Override org Ids to process all orgs if the "all_orgs" tag is set to true.
    if (
        "all_orgs" in context.run.tags
        and context.run.tags["all_orgs"].lower() == "true"
    ):
        ORG_IDS = None

    context.log.info(
        f"Processing partition: date={date_str}, hour={hour_str}"
        f", partition window: {partition_start} to {partition_end}"
        f", effective enrichment_due_at window: {effective_window_start} to {effective_window_end}"
        f", org_ids: {ORG_IDS if ORG_IDS else 'all'}"
    )

    result_df = generate_eco_driving_report(
        spark=spark,
        partition_start=partition_start,
        partition_end=partition_end,
        org_ids=ORG_IDS,
        region=region,
    )

    # Add partition columns: date and hour - represent when data was processed, not when intervals occurred
    # Both date and hour come from the partition key to ensure consistency
    result_df = result_df.withColumn("date", F.to_date(F.lit(date_str))).withColumn(
        "hour", F.lit(hour_str)
    )

    return result_df
