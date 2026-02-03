from dataclasses import dataclass
from typing import Callable, List
from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    Database,
    InstanceType,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
)
from pyspark.sql import SparkSession, DataFrame, functions as F, Window
from functools import reduce

schema = [
    {
        "name": "date",
        "type": "string",
        "metadata": {"comment": "The date the eco driving data was reported."},
    },
    {
        "name": "org_id",
        "type": "long",
        "metadata": {
            "comment": "The Samsara ID of the organization that the data, device and driver belong to."
        },
    },
    {
        "name": "device_id",
        "type": "long",
        "metadata": {
            "comment": "The Samsara ID of the device that the eco driving data was recorded against."
        },
    },
    {
        "name": "driver_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The Samsara ID of the driver associated with the device at this time."
        },
    },
    {
        "name": "interval_start",
        "type": "timestamp",
        "metadata": {
            "comment": "The timestamp at the beginning of this 5 minute bucket."
        },
    },
    {
        "name": "interval_end",
        "type": "timestamp",
        "metadata": {"comment": "The timestamp at the end of this 5 minute bucket."},
    },
    {
        "name": "on_duration_ms",
        "type": "double",
        "metadata": {
            "comment": "Sum of the millisecond duration spent with the engine ON during this 5 minute interval for a device and driver."
        },
    },
    {
        "name": "idle_duration_ms",
        "type": "double",
        "metadata": {
            "comment": "Sum of the millisecond duration spent with the engine IDLE during this 5 minute interval for a device and driver."
        },
    },
    {
        "name": "aux_during_idle_ms",
        "type": "double",
        "metadata": {
            "comment": "Sum of the millisecond duration spent with the AUX engine ON whilst engine is IDLE during this 5 minute interval for a device and driver."
        },
    },
    {
        "name": "fuel_consumed_ml",
        "type": "double",
        "metadata": {
            "comment": "Sum of the millilitres of fuel consumed during this 5 minute interval for a device and driver."
        },
    },
    {
        "name": "weight_time",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Sum of the millisecond duration within 5 minute interval for a device when we had weight reported."
        },
    },
    {
        "name": "weighted_weight",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Average weight reported on the interval multiplied by the duration of that interval (weight_time). This value is reported in Kilograms * milliseconds. The average weight can be calculated as weighted_weight/weight_time."
        },
    },
    {
        "name": "uphill_duration_ms",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Sum of the millisecond duration spent on driving in uphill incline during this 5 minute interval for a device."
        },
    },
    {
        "name": "downhill_duration_ms",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Sum of the millisecond duration spent on driving in downhill incline during this 5 minute interval for a device."
        },
    },
    {
        "name": "wear_free_braking_duration_ms",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "Sum of the millisecond duration spent braking through wear-free means during this 5 minute interval for a device."
        },
    },
    {
        "name": "average_weight_kg",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "Average weight, in kilograms, of the device over the 5 minute interval."
        },
    },
    {
        "name": "distance_traveled_m",
        "type": "double",
        "nullable": True,
        "metadata": {"comment": "Total distance traveled over the 5 minute interval."},
    },
    {
        "name": "gaseous_fuel_consumed_grams",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "Gaseous fuel consumed, in grams, over the 5 minute interval."
        },
    },
    {
        "name": "total_braking_duration_ms",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "Sum of duration where the device was braking during the 5 minute interval."
        },
    },
    {
        "name": "total_coasting_ms",
        "type": "double",
        "nullable": True,
        "metadata": {"comment": "Sum of coasting time over the 5 minute interval."},
    },
    {
        "name": "total_cruise_control_ms",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "Sum of time the device was in cruise control over the 5 minute interval."
        },
    },
]


@dataclass
class ApportionMetric:
    metric_column_name: str
    agg_fn: Callable
    output_column_name: str


def apportion_time_delta_metric(
    input_df: DataFrame,
    metric_column_name: str = None,
    grouping_columns: list = None,
    agg_fn: Callable = None,
    metrics_to_apportion: list = None,
    spark: SparkSession = None,
) -> DataFrame:
    """
    Apportions a time-delta metric into fixed 5 minute-aligned buckets.
    If the interval between events is >= 10 minutes it's artificially capped at 5 minutes.
    """
    agg_fn = agg_fn or F.sum
    metrics_to_apportion = metrics_to_apportion or []
    grouping_columns = grouping_columns or []
    all_grouping_cols = ["date", "org_id", "object_id"] + grouping_columns
    window_spec = Window.partitionBy("date", "org_id", "object_id").orderBy(
        "event_time"
    )

    df_with_previous_event_time = input_df.withColumn(
        "previous_event_time", F.lag("event_time").over(window_spec)
    )

    df_with_capped_event_interval = df_with_previous_event_time.withColumn(
        "adjusted_previous_event_time",
        F.when(
            F.unix_timestamp("event_time") - F.unix_timestamp("previous_event_time")
            >= 600,
            F.expr("event_time") - F.expr("interval 5 minutes"),
        ).otherwise(F.col("previous_event_time")),
    ).filter(F.col("event_time") > F.col("adjusted_previous_event_time"))

    df_with_bucket_starts = df_with_capped_event_interval.withColumn(
        "bucket_start",
        F.explode(
            F.sequence(
                F.from_unixtime(
                    (F.unix_timestamp("adjusted_previous_event_time") / 300).cast("int")
                    * 300
                ).cast("timestamp"),
                F.from_unixtime(
                    (F.unix_timestamp("event_time") / 300).cast("int") * 300
                ).cast("timestamp"),
                F.expr("interval 5 minutes"),
            )
        ),
    )

    overlaps_with_fixed_buckets = (
        df_with_bucket_starts.withColumn(
            "bucket_end", F.col("bucket_start") + F.expr("interval 5 minutes")
        )
        .withColumn(
            "overlap_start", F.greatest("adjusted_previous_event_time", "bucket_start")
        )
        .withColumn("overlap_end", F.least("event_time", "bucket_end"))
        .withColumn(
            "overlap_seconds",
            (F.unix_timestamp("overlap_end") - F.unix_timestamp("overlap_start")).cast(
                "double"
            ),
        )
        .withColumn(
            "total_interval_seconds",
            (
                F.unix_timestamp("event_time")
                - F.unix_timestamp("adjusted_previous_event_time")
            ).cast("double"),
        )
        .withColumn(
            "overlap_ratio",
            F.when(
                F.col("total_interval_seconds") > 0,
                F.col("overlap_seconds") / F.col("total_interval_seconds"),
            ).otherwise(0),
        )
        .filter(F.col("overlap_ratio") > 0)
    )

    # TODO purnam: convert all other calls to use the metrics_to_apportion list
    # and remove the single metric case
    if any(metrics_to_apportion):
        apportioned_df = reduce(
            lambda df, metric: df.withColumn(
                metric.output_column_name,
                F.col(metric.metric_column_name) * F.col("overlap_ratio"),
            ),
            metrics_to_apportion,
            overlaps_with_fixed_buckets,
        )

        aggregated_apportioned_df = apportioned_df.groupBy(
            *all_grouping_cols, F.col("bucket_start").alias("interval_start")
        ).agg(
            *[
                m.agg_fn(m.output_column_name).alias(m.output_column_name)
                for m in metrics_to_apportion
            ]
        )
    else:
        apportioned_df = overlaps_with_fixed_buckets.withColumn(
            "assigned_ms", F.col(metric_column_name) * F.col("overlap_ratio")
        )

        aggregated_apportioned_df = apportioned_df.groupBy(
            *all_grouping_cols, F.col("bucket_start").alias("interval_start")
        ).agg(agg_fn("assigned_ms").alias("total_apportioned_ms"))

    return aggregated_apportioned_df


def calculate_grade_metrics(
    start_date: str, end_date: str, spark: SparkSession = None
) -> DataFrame:

    engine_intervals_df = spark.table("engine_state.intervals").filter(
        (F.col("date").between(F.lit(start_date), F.lit(end_date)))
        & (F.col("state") == "ON")
    )
    location_df = spark.table("kinesisstats_window.location").filter(
        (F.col("date").between(F.lit(start_date), F.lit(end_date)))
    )
    location_when_engine_on_df = (
        location_df.select(
            F.col("value.gps_speed_meters_per_second").alias(
                "gps_speed_meters_per_second"
            ),
            F.col("value.altitude_meters").alias("altitude_meters"),
            "org_id",
            "device_id",
            "time",
            "date",
        )
        .alias("l")
        .hint(
            # Close to 90th percentile value of difference between start and end ms from engine intervals.
            "range_join",
            2800000,
        )
        .join(
            engine_intervals_df.alias("i"),
            (F.col("l.org_id") == F.col("i.org_id"))
            & (F.col("l.device_id") == F.col("i.object_id"))
            & (F.col("l.time") >= F.col("i.start_ms"))
            & (F.col("l.time") <= F.col("i.end_ms")),
            "left",
        )
        .filter(F.col("i.object_id").isNotNull())
        .select("l.*")
    )
    lag_window = Window.partitionBy("date", "org_id", "device_id").orderBy("time")
    relevant_location_data_df = (
        location_when_engine_on_df.filter(
            F.col("gps_speed_meters_per_second") >= 2.2352
        )
        .withColumn("altitude_meters", F.col("altitude_meters"))
        # Consider 6 row window which would be approximately 30 seconds
        .withColumn(
            "altitude_6_rows_back_meters", F.lag("altitude_meters", 6).over(lag_window)
        )
        .withColumn("duration_ms", F.col("time") - F.lag("time", 1).over(lag_window))
        .withColumn(
            "duration_6_rows_back_ms", F.col("time") - F.lag("time", 6).over(lag_window)
        )
    )
    filtered_location_data_df = relevant_location_data_df.filter(
        (F.col("duration_ms") <= 10000)
        & (F.col("duration_6_rows_back_ms") >= 30000)
        & (F.col("duration_6_rows_back_ms") <= 60000)
        & F.col("altitude_meters").isNotNull()
        & F.col("altitude_6_rows_back_meters").isNotNull()
    )
    total_distance_df = spark.table("canonical_distance.total_distance").filter(
        F.col("date").between(F.lit(start_date), F.lit(end_date))
    )
    distance_window = (
        Window.partitionBy("ld.date", "ld.org_id", "ld.device_id")
        .orderBy("ld.time")
        .rowsBetween(-5, Window.currentRow)
    )
    joined_df = filtered_location_data_df.alias("ld").join(
        total_distance_df.alias("td"),
        (F.col("ld.org_id") == F.col("td.org_id"))
        & (F.col("ld.device_id") == F.col("td.device_id"))
        & (F.col("ld.date") == F.col("td.date"))
        & (F.col("ld.time") == F.col("td.time_ms")),
        "left",
    )
    ld_columns = [F.col(f"ld.{c}") for c in filtered_location_data_df.columns]
    location_with_cumulative_distance_df = (
        # Filter low distances traveled to improve reliability of grade calculation.
        # The below filters are consistent with the cloud dash grade calculation code.
        joined_df.filter(F.col("td.distance") > 11)
        .withColumn("distance", F.sum("td.distance").over(distance_window))
        .select(*ld_columns, "distance")
    )
    distance_with_grade_percentage_df = (
        location_with_cumulative_distance_df.filter(
            (F.col("distance") >= 67.056)
            & (
                F.pow(F.col("distance"), 2)
                > F.pow(
                    F.col("altitude_meters") - F.col("altitude_6_rows_back_meters"), 2
                )
            )
        )
        .withColumn(
            "vertical_rise",
            F.col("altitude_meters") - F.col("altitude_6_rows_back_meters"),
        )
        .withColumn(
            "horizontal_distance",
            F.sqrt(F.pow(F.col("distance"), 2) - F.pow(F.col("vertical_rise"), 2)),
        )
        .withColumn(
            "grade_percentage",
            (F.col("vertical_rise") / F.col("horizontal_distance")) * 100,
        )
    )
    grade_final_df = distance_with_grade_percentage_df.groupBy(
        "date",
        "org_id",
        "device_id",
        F.from_unixtime(F.floor(F.col("time") / (1000 * 300)) * 300).alias(
            "interval_start"
        ),
    ).agg(
        F.sum(
            F.when(F.col("grade_percentage") > 2, F.col("duration_ms")).otherwise(0)
        ).alias("uphill_duration_ms"),
        F.sum(
            F.when(F.col("grade_percentage") < -2, F.col("duration_ms")).otherwise(0)
        ).alias("downhill_duration_ms"),
    )

    return grade_final_df


def calculate_engine_fuel_metrics(start_date: str, end_date: str, spark: SparkSession):
    fuel_intervals_df = spark.table(
        "engine_state_report.engine_state_pto_fuel_intervals"
    ).filter(F.col("date").between(F.lit(start_date), F.lit(end_date)))
    ecu_segments_df = spark.table("fuel_energy_efficiency_report.ecu_filter_segments")
    fuel_with_ecu_segments_df = (
        fuel_intervals_df.alias("fuel")
        .hint("range_join", 600000)
        .join(
            ecu_segments_df.alias("seg"),
            (F.col("fuel.start_ms") <= F.col("seg.segment_end"))
            & (F.col("fuel.end_ms") >= F.col("seg.segment_start"))
            & (F.col("seg.device_id") == F.col("fuel.object_id"))
            & (F.col("seg.org_id") == F.col("fuel.org_id")),
            "left",
        )
        .select("fuel.*", "seg.segment_start")
    )
    grouping_cols = [
        "date",
        "org_id",
        "object_id",
        "start_ms",
        "end_ms",
        "engine_state",
        "productive_pto_state",
        "fuel_consumed_ml",
        "gaseous_fuel_consumed_grams",
    ]
    filtered_fuel_df = (
        fuel_with_ecu_segments_df.groupBy(*grouping_cols)
        .agg(F.max(F.col("segment_start").isNotNull()).alias("has_ecu_overlap"))
        .withColumn(
            "fuel_consumed_ml",
            F.when(F.col("has_ecu_overlap"), F.lit(0)).otherwise(
                F.col("fuel_consumed_ml")
            ),
        )
        .withColumn(
            "gaseous_fuel_consumed_grams",
            F.when(F.col("has_ecu_overlap"), F.lit(0)).otherwise(
                F.col("gaseous_fuel_consumed_grams")
            ),
        )
        .drop("has_ecu_overlap")
    )
    exploded_intervals_df = filtered_fuel_df.withColumn(
        "covering_5_min_start",
        F.explode(
            F.sequence(
                F.from_unixtime(F.floor(F.col("start_ms") / (1000 * 300)) * 300).cast(
                    "timestamp"
                ),
                F.from_unixtime(
                    F.floor((F.col("end_ms") - 1) / (1000 * 300)) * 300
                ).cast("timestamp"),
                F.expr("INTERVAL 5 MINUTES"),
            )
        ),
    )
    distributed_fuel_base_df = (
        exploded_intervals_df.withColumn("original_start_ms", F.col("start_ms"))
        .withColumn(
            "subset_start_ms",
            F.greatest(
                F.unix_timestamp(F.col("covering_5_min_start")) * 1000,
                F.col("start_ms"),
            ),
        )
        .withColumn(
            "subset_end_ms",
            F.least(
                (
                    F.unix_timestamp(
                        F.col("covering_5_min_start") + F.expr("interval 5 minute")
                    )
                )
                * 1000,
                F.col("end_ms"),
            ),
        )
        .withColumn(
            "interval_length_ms", F.col("subset_end_ms") - F.col("subset_start_ms")
        )
    )
    apportionment_window = Window.partitionBy(
        "date", "org_id", "object_id", "original_start_ms"
    )
    distributed_fuel_df = (
        distributed_fuel_base_df.withColumn(
            "total_interval_length_ms",
            F.sum("interval_length_ms").over(apportionment_window),
        )
        .withColumn(
            "apportioned_fuel_ml",
            F.col("fuel_consumed_ml")
            * (
                F.col("interval_length_ms")
                / F.nullif(F.col("total_interval_length_ms"), F.lit(0))
            ),
        )
        .withColumn(
            "apportioned_gaseous_grams",
            F.col("gaseous_fuel_consumed_grams")
            * (
                F.col("interval_length_ms")
                / F.nullif(F.col("total_interval_length_ms"), F.lit(0))
            ),
        )
    )
    engine_fuel_final_df = distributed_fuel_df.groupBy(
        "date",
        "org_id",
        "object_id",
        F.col("covering_5_min_start").alias("interval_start"),
    ).agg(
        F.sum(
            F.when(
                F.col("engine_state").isin(["ON", "IDLE"]), F.col("interval_length_ms")
            ).otherwise(0)
        ).alias("on_duration_ms"),
        F.sum(
            F.when(
                F.col("engine_state") == "IDLE", F.col("interval_length_ms")
            ).otherwise(0)
        ).alias("idle_duration_ms"),
        F.sum(
            F.when(
                (F.col("engine_state") == "IDLE")
                & (F.col("productive_pto_state") == 1),
                F.col("interval_length_ms"),
            ).otherwise(0)
        ).alias("aux_during_idle_ms"),
        F.when(F.sum("apportioned_fuel_ml") < 454609, F.sum("apportioned_fuel_ml"))
        .otherwise(F.lit(0))
        .cast("long")
        .alias("fuel_consumed_ml"),
        F.when(
            F.sum("apportioned_gaseous_grams") < 271000,
            F.sum("apportioned_gaseous_grams"),
        )
        .otherwise(F.lit(0))
        .cast("long")
        .alias("gaseous_fuel_consumed_grams"),
    )
    return engine_fuel_final_df


def calculate_segment_distance(
    start_date: str, end_date: str, spark: SparkSession = None
) -> DataFrame:

    dist_df = spark.table("canonical_distance.total_distance").filter(
        (F.col("date").between(F.lit(start_date), F.lit(end_date)))
    )
    ecu_df = spark.table("fuel_energy_efficiency_report.ecu_filter_segments").filter(
        (F.col("date").between(F.lit(start_date), F.lit(end_date)))
    )
    dist_with_ecu_df = (
        dist_df.alias("dist")
        .hint("range_join", 600000)
        .join(
            ecu_df.alias("seg"),
            (F.col("dist.date") == F.col("seg.date"))
            & (F.col("dist.org_id") == F.col("seg.org_id"))
            & (F.col("dist.device_id") == F.col("seg.device_id"))
            & (F.col("dist.time_ms") >= F.col("seg.segment_start"))
            & (F.col("dist.time_ms") <= F.col("seg.segment_end")),
            "left",
        )
    )
    input_for_apportion_df = dist_with_ecu_df.select(
        F.col("dist.date"),
        F.col("dist.org_id"),
        F.col("dist.device_id").alias("object_id"),
        (F.col("dist.time_ms") / 1000).cast("timestamp").alias("event_time"),
        F.when(F.col("seg.segment_start").isNotNull(), F.lit(0))
        .otherwise(F.col("dist.distance"))
        .alias("distance_traveled_m"),
    )
    apportioned_distance_df = apportion_time_delta_metric(
        input_df=input_for_apportion_df,
        metric_column_name="distance_traveled_m",
    )
    final_df = apportioned_distance_df.withColumnRenamed(
        "total_apportioned_ms", "distance_traveled_m"
    )

    return final_df


@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="""
        Eco driving report table with metrics aggregated to 5 minute intervals. Metric values
        are interpolated within each 5 minute bucket so that the reporting interval is aligned across
        all metrics.
        """,
        row_meaning="""
        Each row provides the eco driving metrics for a specific device during a single 5 minute interval.
        """,
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[DATAENGINEERING],
    schema=schema,
    upstreams=[
        "engine_state.intervals",
        "kinesisstats_window.location",
        "canonical_distance.total_distance",
        "engine_state_report.engine_state_pto_fuel_intervals",
        "fuel_energy_efficiency_report.ecu_filter_segments",
        "canonical_distance.total_distance",
        "fuel_energy_efficiency_report.ecu_filter_segments",
        "objectstat_diffs.cruise_control",
        "objectstat_diffs.coasting",
        "kinesisstats_history.osdj1939cvwgrosscombinationvehicleweightkilograms",
        "kinesisstats_history.osDRegenBrakingMs",
        "kinesisstats_history.osDAggregateDurationRetarderBrakingMs",
        "kinesisstats_history.osDAggregateDurationFrictionBrakingMs",
        "fuel_energy_efficiency_report.driver_assignments",
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
        NonEmptyDQCheck(name="dq_non_empty_fct_eco_driving_reports"),
        NonNullDQCheck(
            name="dq_non_null_fct_eco_driving_reports",
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
            name="dq_pk_fct_eco_driving_reports",
            primary_keys=["date", "org_id", "device_id", "interval_start"],
            block_before_write=True,
        ),
    ],
)
def fct_eco_driving_reports(context: AssetExecutionContext) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    partition_dates = partition_key_ranges_from_context(context)[0]
    start_date = partition_dates[0]
    end_date = partition_dates[-1]

    # --- Cruise Control ---
    cruise_events_df = (
        spark.table("objectstat_diffs.cruise_control")
        .filter(F.col("date").between(F.lit(start_date), F.lit(end_date)))
        .withColumn("event_time", (F.col("time") / 1000).cast("timestamp"))
        .select("date", "org_id", "object_id", "event_time", "cruise_control_ms")
    )

    cruise_control_final_df = apportion_time_delta_metric(
        cruise_events_df, "cruise_control_ms"
    ).withColumnRenamed("total_apportioned_ms", "total_cruise_control_ms")

    # --- Coasting ---
    coasting_events_df = (
        spark.table("objectstat_diffs.coasting")
        .filter(F.col("date").between(F.lit(start_date), F.lit(end_date)))
        .withColumn("event_time", (F.col("time") / 1000).cast("timestamp"))
        .select("date", "org_id", "object_id", "event_time", "coasting_time_ms")
    )

    coasting_final_df = apportion_time_delta_metric(
        coasting_events_df, "coasting_time_ms"
    ).withColumnRenamed("total_apportioned_ms", "total_coasting_ms")

    # --- Weight ---
    weight_events_df = (
        spark.table(
            "kinesisstats_history.osdj1939cvwgrosscombinationvehicleweightkilograms"
        )
        .filter(
            F.col("date").between(
                F.expr(f"date_sub('{start_date}', 1)"), F.lit(end_date)
            )
        )
        .filter(
            ~F.col("value.is_end")
            & ~F.col("value.is_databreak")
            & (F.col("value.int_value") > 0)
            & (F.col("value.int_value") < 100000)
        )
        .withColumn("event_time", (F.col("time") / 1000).cast("timestamp"))
        .withColumn("weight", F.col("value.int_value"))
        .select("date", "org_id", "object_id", "event_time", "weight")
    )

    binned_weights = weight_events_df.withColumn(
        "interval_start",
        F.from_unixtime((F.unix_timestamp("event_time") / 300).cast("int") * 300).cast(
            "timestamp"
        ),
    )

    latest_observation_per_interval = (
        binned_weights.withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy(
                    "date", "org_id", "object_id", "interval_start"
                ).orderBy(F.col("event_time").desc())
            ),
        )
        .filter(F.col("rn") == 1)
        .select("date", "org_id", "object_id", "interval_start", "weight")
    )

    # Generate full set of 5 minute buckets per device
    min_max_times = weight_events_df.groupBy("date", "org_id", "object_id").agg(
        F.min("event_time").alias("min_time"), F.max("event_time").alias("max_time")
    )

    expanded_intervals_df = min_max_times.withColumn(
        "interval_start",
        F.explode(
            F.sequence(
                F.from_unixtime(
                    (F.unix_timestamp("min_time") / 300).cast("int") * 300
                ).cast("timestamp"),
                F.from_unixtime(
                    (F.unix_timestamp("max_time") / 300).cast("int") * 300
                ).cast("timestamp"),
                F.expr("interval 5 minutes"),
            )
        ),
    ).select("date", "org_id", "object_id", "interval_start")

    # Join and forward-fill weights
    joined_df = (
        expanded_intervals_df.alias("expanded")
        .join(
            latest_observation_per_interval,
            on=["date", "org_id", "object_id", "interval_start"],
            how="left",
        )
        .withColumn(
            "average_weight_kg",
            F.last("weight", ignorenulls=True).over(
                Window.partitionBy("date", "org_id", "object_id")
                .orderBy("interval_start")
                .rowsBetween(Window.unboundedPreceding, 0)
            ),
        )
    )

    weight_final_df = (
        joined_df.withColumn("weight_time", F.lit(5 * 60 * 1000))
        .withColumn("weighted_weight", F.col("average_weight_kg") * (5 * 60))
        .select(
            "expanded.date",
            "org_id",
            "object_id",
            "interval_start",
            "average_weight_kg",
            "weight_time",
            "weighted_weight",
        )
        .filter(F.col("average_weight_kg").isNotNull())
    )

    # --- Grade ---
    grade_final_df = calculate_grade_metrics(start_date, end_date, spark)

    # --- Braking ---
    braking_events_df = (
        spark.table("kinesisstats_history.osDRegenBrakingMs")
        .filter(F.col("date").between(F.lit(start_date), F.lit(end_date)))
        .select(
            "date",
            "org_id",
            "object_id",
            "time",
            F.col("value.int_value").alias("duration_ms"),
            F.lit("WEAR_FREE").alias("brake_type"),
        )
        .unionByName(
            spark.table("kinesisstats_history.osDAggregateDurationRetarderBrakingMs")
            .filter(F.col("date").between(F.lit(start_date), F.lit(end_date)))
            .select(
                "date",
                "org_id",
                "object_id",
                "time",
                F.col("value.int_value").alias("duration_ms"),
                F.lit("WEAR_FREE").alias("brake_type"),
            )
        )
        .unionByName(
            spark.table("kinesisstats_history.osDAggregateDurationFrictionBrakingMs")
            .filter(F.col("date").between(F.lit(start_date), F.lit(end_date)))
            .select(
                "date",
                "org_id",
                "object_id",
                "time",
                F.col("value.int_value").alias("duration_ms"),
                F.lit("FRICTION").alias("brake_type"),
            )
        )
        .withColumn("event_time", (F.col("time") / 1000).cast("timestamp"))
    )
    braking_final_df = (
        apportion_time_delta_metric(
            braking_events_df, "duration_ms", grouping_columns=["brake_type"]
        )
        .groupBy("date", "org_id", "object_id", "interval_start")
        .agg(
            F.sum(
                F.when(
                    F.col("brake_type") == "WEAR_FREE", F.col("total_apportioned_ms")
                ).otherwise(0)
            ).alias("wear_free_duration_ms"),
            F.sum("total_apportioned_ms").alias("total_braking_duration_ms"),
        )
    )

    # --- Engine & Fuel ---
    engine_fuel_final_df = calculate_engine_fuel_metrics(start_date, end_date, spark)

    # --- Segment Distance ---
    segment_distance_final_df = calculate_segment_distance(start_date, end_date, spark)

    # --- Join all stats ---
    @dataclass
    class EcoDrivingStat:
        df: DataFrame
        alias: str

        def get_aliased_df(self) -> DataFrame:
            return self.df.alias(self.alias)

    def coalesce_across_stats(stat_dfs: List[EcoDrivingStat], column_name: str):
        return F.coalesce(*[f"{s.alias}.{column_name}" for s in stat_dfs])

    stat_dfs = [
        EcoDrivingStat(df=cruise_control_final_df, alias="cc"),
        EcoDrivingStat(df=coasting_final_df, alias="c"),
        EcoDrivingStat(df=weight_final_df, alias="w"),
        EcoDrivingStat(
            df=grade_final_df.withColumnRenamed("device_id", "object_id"), alias="g"
        ),
        EcoDrivingStat(df=braking_final_df, alias="b"),
        EcoDrivingStat(df=engine_fuel_final_df, alias="ef"),
        EcoDrivingStat(df=segment_distance_final_df, alias="sd"),
    ]

    combined_stats_df = reduce(
        lambda current_combined_stats_df, next_stat: current_combined_stats_df.join(
            next_stat.get_aliased_df(),
            ["date", "org_id", "object_id", "interval_start"],
            how="full",
        ),
        stat_dfs[1:],
        stat_dfs[0].get_aliased_df(),
    ).select(
        coalesce_across_stats(stat_dfs, "date").alias("date"),
        coalesce_across_stats(stat_dfs, "interval_start")
        .cast("timestamp")
        .alias("interval_start"),
        (
            coalesce_across_stats(stat_dfs, "interval_start")
            + F.expr("interval 5 minutes")
        )
        .cast("timestamp")
        .alias("interval_end"),
        coalesce_across_stats(stat_dfs, "org_id").alias("org_id"),
        coalesce_across_stats(stat_dfs, "object_id").alias("device_id"),
        F.coalesce(F.col("cc.total_cruise_control_ms"), F.lit(0))
        .cast("double")
        .alias("total_cruise_control_ms"),
        F.coalesce(F.col("c.total_coasting_ms"), F.lit(0))
        .cast("double")
        .alias("total_coasting_ms"),
        F.coalesce(F.col("w.weighted_weight"), F.lit(0))
        .cast("double")
        .alias("weighted_weight"),
        F.coalesce(F.col("w.weight_time"), F.lit(0))
        .cast("double")
        .alias("weight_time"),
        F.col("w.average_weight_kg").cast("double").alias("average_weight_kg"),
        F.coalesce(F.col("g.uphill_duration_ms"), F.lit(0))
        .cast("double")
        .alias("uphill_duration_ms"),
        F.coalesce(F.col("g.downhill_duration_ms"), F.lit(0))
        .cast("double")
        .alias("downhill_duration_ms"),
        F.coalesce(F.col("b.wear_free_duration_ms"), F.lit(0))
        .cast("double")
        .alias("wear_free_braking_duration_ms"),
        F.coalesce(F.col("b.total_braking_duration_ms"), F.lit(0))
        .cast("double")
        .alias("total_braking_duration_ms"),
        F.coalesce(F.col("ef.on_duration_ms"), F.lit(0))
        .cast("double")
        .alias("on_duration_ms"),
        F.coalesce(F.col("ef.idle_duration_ms"), F.lit(0))
        .cast("double")
        .alias("idle_duration_ms"),
        F.coalesce(F.col("ef.aux_during_idle_ms"), F.lit(0))
        .cast("double")
        .alias("aux_during_idle_ms"),
        F.coalesce(F.col("ef.fuel_consumed_ml"), F.lit(0))
        .cast("double")
        .alias("fuel_consumed_ml"),
        F.coalesce(F.col("ef.gaseous_fuel_consumed_grams"), F.lit(0))
        .cast("double")
        .alias("gaseous_fuel_consumed_grams"),
        F.coalesce(F.col("sd.distance_traveled_m"), F.lit(0))
        .cast("double")
        .alias("distance_traveled_m"),
    )

    # --- Find driver ID based on overlap with driver assignments ---

    driver_assignments_df = (
        spark.table("fuel_energy_efficiency_report.driver_assignments")
        .filter(F.col("date").between(F.lit(start_date), F.lit(end_date)))
        .select(
            "date",
            "org_id",
            "device_id",
            "driver_id",
            (F.col("start_time") / 1000).cast("timestamp").alias("driver_start_time"),
            (F.col("end_time") / 1000).cast("timestamp").alias("driver_end_time"),
        )
    )

    combined_stats_with_driver_df = (
        combined_stats_df.alias("a")
        .hint("range_join", 28800)
        .join(
            driver_assignments_df.alias("da"),
            (F.col("a.org_id") == F.col("da.org_id"))
            & (F.col("a.device_id") == F.col("da.device_id"))
            & (
                F.col("a.interval_start").between(
                    F.col("da.driver_start_time"),
                    F.col("da.driver_end_time") - F.expr("interval 1 millisecond"),
                )
            ),
            "left",
        )
        .select(
            "a.*",
            F.coalesce(F.col("da.driver_id"), F.lit(0))
            .cast("bigint")
            .alias("driver_id"),
        )
    )

    return combined_stats_with_driver_df
