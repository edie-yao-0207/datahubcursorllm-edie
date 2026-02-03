from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    DATAENGINEERING,
    FRESHNESS_SLO_12PM_PST,
    AWSRegion,
    Database,
    InstanceType,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
    get_region_from_context,
)
from dataweb.assets.datasets.product_analytics.fct_eco_driving_reports import (
    schema as ORIGINAL_SCHEMA,
    apportion_time_delta_metric,
    ApportionMetric,
)
from pyspark.sql import SparkSession, DataFrame, functions as F, Window
from functools import reduce

augmented_columns = [
    {
        "name": "ms_in_speed_0_25_mph",
        "type": "double",
        "metadata": {
            "comment": "Time in milliseconds the device spent driving between 0 and 25 MPH."
        },
    },
    {
        "name": "ms_in_speed_25_50_mph",
        "type": "double",
        "metadata": {
            "comment": "Time in milliseconds the device spent driving between 25 and 50 MPH."
        },
    },
    {
        "name": "ms_in_speed_50_65_mph",
        "type": "double",
        "metadata": {
            "comment": "Time in milliseconds the device spent driving between 50 and 65 MPH."
        },
    },
    {
        "name": "ms_in_speed_65_plus_mph",
        "type": "double",
        "metadata": {
            "comment": "Time in milliseconds the device spent driving above 65 MPH."
        },
    },
    {
        "name": "ms_in_accel_hard_braking",
        "type": "double",
        "metadata": {
            "comment": "Time in milliseconds the device spent braking harshly."
        },
    },
    {
        "name": "ms_in_accel_mod_braking",
        "type": "double",
        "metadata": {
            "comment": "Time in milliseconds the device spent braking moderately."
        },
    },
    {
        "name": "ms_in_accel_cruise_coast",
        "type": "double",
        "metadata": {
            "comment": "Time in milliseconds the device spent at a low rate of acceleration (coasting or cruise control)."
        },
    },
    {
        "name": "ms_in_accel_mod_accel",
        "type": "double",
        "metadata": {
            "comment": "Time in milliseconds the device spent accelerating at a moderate rate."
        },
    },
    {
        "name": "ms_in_accel_hard_accel",
        "type": "double",
        "metadata": {
            "comment": "Time in milliseconds the device spent accelerating at a high rate."
        },
    },
    {
        "name": "avg_lanes",
        "type": "double",
        "metadata": {
            "comment": "The average number of lanes the device traveled in during the 5 minute interval."
        },
    },
    {
        "name": "avg_speed_limit_kph",
        "type": "double",
        "metadata": {
            "comment": "The average speed limit, in KPH, of the roads the device traveled on during the 5 minute interval (from OSM)."
        },
    },
    {
        "name": "road_type_diversity",
        "type": "double",
        "metadata": {
            "comment": "The number of different types of roads that the device traveled on during the 5 minute interval (from OSM highway type)."
        },
    },
    {
        "name": "speed_limit_stddev",
        "type": "double",
        "metadata": {
            "comment": "The standard deviation of the traveled roads' speed limits."
        },
    },
    {
        "name": "predominant_road_type",
        "type": "string",
        "metadata": {
            "comment": "The type of road the device spent most time traveling on during the 5 minute interval."
        },
    },
    {
        "name": "accelerator_pedal_time_gt95_ms",
        "type": "double",
        "metadata": {
            "comment": "The amount of time the device's accelerator was depressed more than 95 percent of the way in milliseconds."
        },
    },
    {
        "name": "avg_altitude_meters",
        "type": "double",
        "metadata": {
            "comment": "The device's average altitude, in meters, over the 5 minute interval."
        },
    },
    {
        "name": "stddev_altitude_meters",
        "type": "double",
        "metadata": {
            "comment": "The standard deviation of the device's altitude, in meters, over the 5 minute interval."
        },
    },
    {
        "name": "stddev_speed_mps",
        "type": "double",
        "metadata": {
            "comment": "The standard deviation of the device's speed, in meters per second, over the 5 minute interval."
        },
    },
    {
        "name": "avg_latitude",
        "type": "double",
        "metadata": {
            "comment": "The device's average latitude over the 5 minute interval."
        },
    },
    {
        "name": "avg_longitude",
        "type": "double",
        "metadata": {
            "comment": "The device's average longitude over the 5 minute interval."
        },
    },
]

schema = ORIGINAL_SCHEMA + augmented_columns


def calculate_speed_from_locations(
    start_date: str, end_date: str, spark: SparkSession = None
) -> DataFrame:
    """
    Calculate device speed and acceleration from GPS speed from adjacent
    location stats.
    """

    return (
        spark.table("kinesisstats_window.location")
        .filter(F.col("date").between(F.lit(start_date), F.lit(end_date)))
        .withColumn("time_delta_ms", F.col("value.time") - F.col("previous.value.time"))
        .filter((F.col("time_delta_ms") > 0) & (F.col("time_delta_ms") < 300000))
        .withColumn(
            "acceleration_mps2",
            (
                F.col("value.gps_speed_meters_per_second")
                - F.col("previous.value.gps_speed_meters_per_second")
            )
            / F.nullif(F.col("time_delta_ms") / 1000.0, F.lit(0)),
        )
        .withColumn(
            "avg_speed_mps",
            (
                F.col("value.gps_speed_meters_per_second")
                + F.col("previous.value.gps_speed_meters_per_second")
            )
            / 2.0,
        )
        .withColumn(
            "avg_altitude_meters",
            (F.col("value.altitude_meters") + F.col("previous.value.altitude_meters"))
            / 2.0,
        )
        .withColumn(
            "avg_latitude",
            (F.col("value.latitude") + F.col("previous.value.latitude")) / 2.0,
        )
        .withColumn(
            "avg_longitude",
            (F.col("value.longitude") + F.col("previous.value.longitude")) / 2.0,
        )
        .select(
            "date",
            "org_id",
            F.col("device_id"),
            F.col("value.time").alias("time"),
            F.col("value.way_id").alias("way_id"),
            F.col("value.latitude").alias("latitude"),
            "time_delta_ms",
            "acceleration_mps2",
            "avg_speed_mps",
            "avg_altitude_meters",
            "avg_latitude",
            "avg_longitude",
        )
    )


def summarize_time_in_speed_and_harsh_action_categories(
    location_with_speeds_df: DataFrame, spark: SparkSession = None
) -> DataFrame:

    binned_points_df = (
        location_with_speeds_df.filter(F.col("acceleration_mps2").isNotNull())
        .withColumn(
            "speed_bin",
            # Bucket speeds in miles per hour, based on meters per second
            # from the source data.
            F.when(F.col("avg_speed_mps") < 11.2, "speed_0_25")
            .when(F.col("avg_speed_mps") < 22, "speed_25_50")
            .when(F.col("avg_speed_mps") < 29, "speed_50_65")
            .otherwise("speed_65_plus"),
        )
        .withColumn(
            "accel_bin",
            F.when(F.col("acceleration_mps2") < -1.5, "accel_hard_braking")
            .when(F.col("acceleration_mps2") < -0.5, "accel_mod_braking")
            .when(F.col("acceleration_mps2") <= 0.5, "accel_cruise_coast")
            .when(F.col("acceleration_mps2") <= 1.5, "accel_mod_accel")
            .otherwise("accel_hard_accel"),
        )
        .withColumn(
            "interval_start",
            F.from_unixtime(F.floor(F.col("time") / (1000 * 300)) * 300).cast(
                "timestamp"
            ),
        )
    )

    return binned_points_df.groupBy(
        "date", "org_id", F.col("device_id").alias("object_id"), "interval_start"
    ).agg(
        F.sum(
            F.when(
                F.col("speed_bin") == "speed_0_25", F.col("time_delta_ms")
            ).otherwise(0)
        ).alias("ms_in_speed_0_25_mph"),
        F.sum(
            F.when(
                F.col("speed_bin") == "speed_25_50", F.col("time_delta_ms")
            ).otherwise(0)
        ).alias("ms_in_speed_25_50_mph"),
        F.sum(
            F.when(
                F.col("speed_bin") == "speed_50_65", F.col("time_delta_ms")
            ).otherwise(0)
        ).alias("ms_in_speed_50_65_mph"),
        F.sum(
            F.when(
                F.col("speed_bin") == "speed_65_plus", F.col("time_delta_ms")
            ).otherwise(0)
        ).alias("ms_in_speed_65_plus_mph"),
        F.sum(
            F.when(
                F.col("accel_bin") == "accel_hard_braking", F.col("time_delta_ms")
            ).otherwise(0)
        ).alias("ms_in_accel_hard_braking"),
        F.sum(
            F.when(
                F.col("accel_bin") == "accel_mod_braking", F.col("time_delta_ms")
            ).otherwise(0)
        ).alias("ms_in_accel_mod_braking"),
        F.sum(
            F.when(
                F.col("accel_bin") == "accel_cruise_coast", F.col("time_delta_ms")
            ).otherwise(0)
        ).alias("ms_in_accel_cruise_coast"),
        F.sum(
            F.when(
                F.col("accel_bin") == "accel_mod_accel", F.col("time_delta_ms")
            ).otherwise(0)
        ).alias("ms_in_accel_mod_accel"),
        F.sum(
            F.when(
                F.col("accel_bin") == "accel_hard_accel", F.col("time_delta_ms")
            ).otherwise(0)
        ).alias("ms_in_accel_hard_accel"),
    )


def calculate_roadway_attributes(
    processed_location_df: DataFrame, region: str, spark: SparkSession = None
) -> DataFrame:
    """
    Calculates time-weighted roadway attributes, filtering out points with
    missing or zero-value data before aggregation.
    """

    if region in [AWSRegion.EU_WEST_1, AWSRegion.CA_CENTRAL_1]:
        osm_way_nodes_table = "data_tools_delta_share.dojo.osm_way_nodes"
    else:
        osm_way_nodes_table = "dojo.osm_way_nodes"

    distinct_ways_df = (
        spark.table(osm_way_nodes_table)
        .groupBy("way_id", "highway_tag_value")
        .agg(F.first("tags").alias("tags"))
    )

    way_attributes_df = (
        distinct_ways_df.withColumn(
            "lanes",
            F.element_at(F.filter(F.col("tags"), lambda t: t["key"] == "lanes"), 1)[
                "value"
            ].cast("int"),
        )
        .withColumn(
            "maxspeed_str",
            F.element_at(F.filter(F.col("tags"), lambda t: t["key"] == "maxspeed"), 1)[
                "value"
            ],
        )
        .withColumn(
            "maxspeed_kph",
            F.when(
                F.lower(F.col("maxspeed_str")).like("%mph%"),
                # Convert from MPH to KPH
                F.regexp_extract(F.col("maxspeed_str"), "(\d+)", 1).cast("float")
                * 1.60934,
            )
            .when(
                F.col("maxspeed_str").rlike("^[0-9.]+$"),
                F.col("maxspeed_str").cast("float"),
            )
            .when(
                F.lower(F.col("maxspeed_str")).like("%kph%"),
                F.regexp_extract(F.col("maxspeed_str"), "(\d+)", 1).cast("float"),
            )
            .otherwise(None),  # Default to NULL
        )
        .select("way_id", "highway_tag_value", "lanes", "maxspeed_kph")
    )

    classified_location_points_df = (
        processed_location_df.alias("loc")
        .join(
            way_attributes_df.alias("osm"),
            F.col("loc.way_id") == F.col("osm.way_id"),
            "left",
        )
        .select(
            F.col("loc.date"),
            F.col("loc.org_id"),
            F.col("loc.device_id").alias("object_id"),
            F.col("loc.time"),
            "time_delta_ms",
            "highway_tag_value",
            "lanes",
            "maxspeed_kph",
        )
    )

    valid_road_points_df = classified_location_points_df.filter(
        F.col("highway_tag_value").isNotNull()
        & F.col("lanes").isNotNull()
        & (F.col("lanes") > 0)
        & F.col("maxspeed_kph").isNotNull()
        & (F.col("maxspeed_kph") > 0)
    )

    # Aggregate attributes into 5-minute buckets
    binned_road_df = valid_road_points_df.withColumn(
        "interval_start",
        F.from_unixtime(F.floor(F.col("time") / (1000 * 300)) * 300).cast("timestamp"),
    )

    agg_road_attrs_df = binned_road_df.groupBy(
        "date", "org_id", "object_id", "interval_start"
    ).agg(
        (F.sum(F.col("lanes") * F.col("time_delta_ms")) / F.sum("time_delta_ms")).alias(
            "avg_lanes"
        ),
        (
            F.sum(F.col("maxspeed_kph") * F.col("time_delta_ms"))
            / F.sum("time_delta_ms")
        ).alias("avg_speed_limit_kph"),
        F.countDistinct("highway_tag_value").alias("road_type_diversity"),
        F.stddev_pop("maxspeed_kph").alias("speed_limit_stddev"),
    )

    time_per_road_type_df = binned_road_df.groupBy(
        "date", "org_id", "object_id", "interval_start", "highway_tag_value"
    ).agg(F.sum("time_delta_ms").alias("time_on_road_type"))

    window_spec_predominant = Window.partitionBy(
        "date", "org_id", "object_id", "interval_start"
    ).orderBy(F.desc("time_on_road_type"))

    predominant_road_type_df = (
        time_per_road_type_df.withColumn(
            "rn", F.row_number().over(window_spec_predominant)
        )
        .filter(F.col("rn") == 1)
        .select(
            "date",
            "org_id",
            "object_id",
            "interval_start",
            F.col("highway_tag_value").alias("predominant_road_type"),
        )
    )

    return agg_road_attrs_df.join(
        predominant_road_type_df,
        ["date", "org_id", "object_id", "interval_start"],
        "left",
    )


def calculate_accelerator_summary(
    start_date: str, end_date: str, spark: SparkSession
) -> DataFrame:
    """
    Calculates the apportioned time with accelerator pedal > 95% from an
    accumulating counter, mirroring the main pipeline's interval capping logic.

    Args:
        spark: The active SparkSession.
        start_date: The start date for the query range.
        end_date: The end date for the query range.

    Returns:
        A DataFrame with the accelerator metric aggregated by 5-minute intervals.
    """

    accel_events_df = (
        spark.table(
            "kinesisstats_history.osDDerivedAcceleratorPedalTimeGreaterThan95PercentMs"
        )
        .filter((F.col("date") >= start_date) & (F.col("date") <= end_date))
        .select(
            "date",
            "org_id",
            "object_id",
            (F.col("time") / 1000).cast("timestamp").alias("event_time"),
            F.col("value.int_value").alias("accumulating_ms"),
        )
    )

    # Calculate the delta and the real interval between consecutive points
    window_spec = Window.partitionBy("date", "org_id", "object_id").orderBy(
        "event_time"
    )

    accel_deltas_df = (
        accel_events_df.withColumn(
            "previous_event_time", F.lag("event_time").over(window_spec)
        )
        .withColumn("previous_value", F.lag("accumulating_ms").over(window_spec))
        .dropna()
        .withColumn("delta_ms", F.col("accumulating_ms") - F.col("previous_value"))
        .filter(F.col("delta_ms") >= 0)
    )  # Filter out counter resets

    # Artificially adjust the start time for long gaps, mirroring the main pipeline
    events_with_adjusted_interval_df = accel_deltas_df.withColumn(
        "adjusted_previous_event_time",
        F.when(
            F.col("event_time") - F.col("previous_event_time")
            >= F.expr("interval 10 minutes"),
            F.col("event_time") - F.expr("interval 5 minutes"),
        ).otherwise(F.col("previous_event_time")),
    ).filter(F.col("event_time") > F.col("adjusted_previous_event_time"))

    # 4. Apportion the calculated delta across all relevant 5-minute buckets
    apportioned_df = (
        events_with_adjusted_interval_df.withColumn(
            "n",
            F.explode(
                F.sequence(
                    F.lit(0),
                    F.floor(
                        (
                            F.unix_timestamp("event_time")
                            - F.unix_timestamp("adjusted_previous_event_time")
                        )
                        / 300
                    ),
                    F.lit(1),
                )
            ),
        )
        .withColumn(
            "interval_start",
            F.from_unixtime(
                F.floor(F.unix_timestamp("adjusted_previous_event_time") / 300) * 300
                + (F.col("n") * 300)
            ).cast("timestamp"),
        )
        .withColumn(
            "interval_end", F.col("interval_start") + F.expr("interval 5 minutes")
        )
        .withColumn(
            "overlap_ratio",
            (
                F.unix_timestamp(F.least(F.col("event_time"), F.col("interval_end")))
                - F.unix_timestamp(
                    F.greatest(
                        F.col("adjusted_previous_event_time"), F.col("interval_start")
                    )
                )
            )
            / F.nullif(
                (
                    F.unix_timestamp(F.col("event_time"))
                    - F.unix_timestamp(F.col("adjusted_previous_event_time"))
                ),
                F.lit(0),
            ),
        )
        .withColumn("assigned_ms", F.col("delta_ms") * F.col("overlap_ratio"))
    )

    return apportioned_df.groupBy("date", "org_id", "object_id", "interval_start").agg(
        F.sum("assigned_ms").alias("accelerator_pedal_time_gt95_ms")
    )


@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="""
        Augmented version of `product_analytics.fct_eco_driving_reports` with additional features
        for device speed, road type, and accelerator pedal depression.
        """,
        row_meaning="""
        Each row provides the eco driving metrics for a specific device during a single 5 minute interval.
        """,
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_12PM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[DATAENGINEERING],
    schema=schema,
    upstreams=[
        "product_analytics.fct_eco_driving_reports",
        "kinesisstats_window.location",
        "dojo.osm_way_nodes",
        "kinesisstats_history.osDDerivedAcceleratorPedalTimeGreaterThan95PercentMs",
    ],
    primary_keys=["date", "org_id", "device_id", "interval_start"],
    partitioning=DailyPartitionsDefinition(start_date="2024-01-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    run_config_overrides=create_run_config_overrides(
        driver_instance_type=InstanceType.RD_FLEET_2XLARGE,
        worker_instance_type=InstanceType.RD_FLEET_4XLARGE,
        max_workers=16,
    ),
    backfill_batch_size=4,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_fct_eco_driving_augmented_reports"),
        NonNullDQCheck(
            name="dq_non_null_fct_eco_driving_augmented_reports",
            non_null_columns=[
                "date",
                "org_id",
                "device_id",
                "interval_start",
                "interval_end",
            ],
            block_before_write=True,
        ),
        PrimaryKeyDQCheck(
            name="dq_pk_fct_eco_driving_augmented_reports",
            primary_keys=["date", "org_id", "device_id", "interval_start"],
            # TODO set as blocking after investigating duplicate rows in EU asset
            block_before_write=False,
        ),
    ],
)
def fct_eco_driving_augmented_reports(context: AssetExecutionContext) -> DataFrame:
    region = get_region_from_context(context)
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    partition_dates = partition_key_ranges_from_context(context)[0]
    start_date = partition_dates[0]
    end_date = partition_dates[-1]

    existing_features_df = (
        spark.table("product_analytics.fct_eco_driving_reports")
        .withColumnRenamed("device_id", "object_id")
        .filter(
            F.col("date").between(F.lit(start_date), F.lit(end_date))
            & (F.col("interval_start") >= F.to_timestamp(F.lit(start_date)))
            & (F.col("interval_start") < F.to_timestamp(F.date_add(F.lit(end_date), 1)))
        )
    )

    processed_location_df = calculate_speed_from_locations(start_date, end_date, spark)

    drive_cycle_df = summarize_time_in_speed_and_harsh_action_categories(
        processed_location_df, spark
    )

    roadway_attributes_df = calculate_roadway_attributes(
        processed_location_df, region, spark
    )

    accelerator_df = calculate_accelerator_summary(start_date, end_date, spark)

    metrics_to_apportion = [
        ApportionMetric(
            metric_column_name="avg_altitude_meters",
            agg_fn=F.stddev_pop,
            output_column_name="stddev_altitude_meters",
        ),
        ApportionMetric(
            metric_column_name="avg_altitude_meters",
            agg_fn=F.avg,
            output_column_name="avg_altitude_meters",
        ),
        ApportionMetric(
            metric_column_name="avg_speed_mps",
            agg_fn=F.stddev_pop,
            output_column_name="stddev_speed_mps",
        ),
        ApportionMetric(
            metric_column_name="avg_latitude",
            agg_fn=F.avg,
            output_column_name="avg_latitude",
        ),
        ApportionMetric(
            metric_column_name="avg_longitude",
            agg_fn=F.avg,
            output_column_name="avg_longitude",
        ),
    ]
    processed_location_df = processed_location_df.withColumn(
        "event_time", (F.col("time") / 1000).cast("timestamp")
    ).withColumnRenamed("device_id", "object_id")

    apportioned_location_df = apportion_time_delta_metric(
        processed_location_df,
        metrics_to_apportion=metrics_to_apportion,
        spark=spark,
    )

    dfs_to_augment = [
        drive_cycle_df.alias("dc"),
        roadway_attributes_df.alias("ra"),
        accelerator_df.alias("accel"),
        apportioned_location_df.alias("loc"),
    ]

    join_cols = ["date", "org_id", "object_id", "interval_start"]

    augmented_df = reduce(
        lambda left_df, right_df: left_df.join(right_df, join_cols, "left"),
        dfs_to_augment,
        existing_features_df.alias("existing"),
    )

    new_drive_cycle_cols = [
        F.coalesce(F.col(f"dc.{c}"), F.lit(0)).cast("double").alias(c)
        for c in drive_cycle_df.columns
        if c not in join_cols
    ]
    new_roadway_cols = [
        F.coalesce(
            F.col(f"ra.{c}"),
            F.lit(0) if c not in ["predominant_road_type"] else F.lit("Unknown"),
        )
        .cast("double" if c != "predominant_road_type" else "string")
        .alias(c)
        for c in roadway_attributes_df.columns
        if c not in join_cols
    ]
    new_accelerator_cols = [
        F.coalesce(F.col(f"accel.{c}"), F.lit(0)).cast("double").alias(c)
        for c in accelerator_df.columns
        if c not in join_cols
    ]

    final_df = augmented_df.select(
        F.col("existing.*"),
        F.coalesce(F.col("avg_altitude_meters"), F.lit(0))
        .cast("double")
        .alias("avg_altitude_meters"),
        F.coalesce(F.col("stddev_altitude_meters"), F.lit(0))
        .cast("double")
        .alias("stddev_altitude_meters"),
        F.coalesce(F.col("stddev_speed_mps"), F.lit(0))
        .cast("double")
        .alias("stddev_speed_mps"),
        F.coalesce(F.col("avg_latitude"), F.lit(0))
        .cast("double")
        .alias("avg_latitude"),
        F.coalesce(F.col("avg_longitude"), F.lit(0))
        .cast("double")
        .alias("avg_longitude"),
        *new_drive_cycle_cols,
        *new_roadway_cols,
        *new_accelerator_cols,
    ).withColumnRenamed("object_id", "device_id")

    return final_df
