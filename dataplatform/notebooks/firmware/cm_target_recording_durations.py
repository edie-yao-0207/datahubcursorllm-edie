# Databricks notebook source
# MAGIC %run ./helpers

# COMMAND ----------

start_date, end_date = get_start_end_date(10, 0)
prev_value_lookback_days = 5

# COMMAND ----------

dashcam_target_state_intervals_df = spark.sql(
    """
    select *
    from dataprep_firmware.cm_dashcam_target_state_intervals
    where date >= '{}'
      and date <= '{}'
    """.format(
        start_date, end_date
    )
)

# Filter intervals of target non-parking mode recording
dashcam_target_normal_recording_intervals_df = dashcam_target_state_intervals_df.filter(
    (F.size(F.col("recording_reason")) > 0)
    & ~((F.size(F.col("recording_reason")) == 1) & (F.col("recording_reason")[0] == 2))
)


# Filter intervals of target recording (including parking mode)
dashcam_target_recording_intervals_df = dashcam_target_state_intervals_df.filter(
    F.size(F.col("recording_reason")) > 0
)

# COMMAND ----------

cm_vg_intervals_df = spark.sql(
    """
  select *
  from dataprep_safety.cm_vg_intervals
  where date >= date_sub('{}', {})
    and date <= '{}'
""".format(
        start_date if start_date else F.date_sub(F.current_date(), 10),
        prev_value_lookback_days,
        end_date if end_date else F.current_date(),
    )
)

# Calculate target non-parking mode recording durations for intervals from cm_vg_intervals.
cm_target_recording_durations_df = (
    cm_vg_intervals_df.alias("a")
    .join(
        dashcam_target_normal_recording_intervals_df.alias("b"),
        (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.cm_device_id") == F.col("b.device_id"))
        & (
            (F.col("a.start_ms") < F.col("b.end_ms"))
            & (F.col("a.end_ms") > F.col("b.start_ms"))
        ),
        "left",
    )
    .groupBy(
        "a.org_id",
        "a.device_id",
        "a.cm_device_id",
        "a.product_id",
        "a.cm_product_id",
        "a.start_ms",
        "a.end_ms",
        "a.duration_ms",
        "a.date",
    )
    .agg(
        F.min(F.greatest(F.col("a.start_ms"), F.col("b.start_ms"))).alias(
            "interval_target_recording_start_ms"
        ),
        F.sum(
            F.when(
                (F.col("b.start_ms").isNotNull()) & (F.col("b.end_ms").isNotNull()),
                F.least(F.col("b.end_ms"), F.col("a.end_ms"))
                - F.greatest(F.col("b.start_ms"), F.col("a.start_ms")),
            ).otherwise(0)
        ).alias("target_recording_duration_ms"),
    )
    # If CM target recording interval intersects the first 90 seconds of the interval,
    # assume target recording starts since the beginning of the interval.
    .withColumn(
        "grace_target_recording_duration_ms",
        F.when(
            F.col("interval_target_recording_start_ms")
            < (F.col("start_ms") + 90 * 1000),
            F.col("target_recording_duration_ms")
            + F.col("interval_target_recording_start_ms")
            - F.col("start_ms"),
        ).otherwise(F.col("target_recording_duration_ms")),
    )
    .drop(
        "interval_target_recording_start_ms",
    )
)

# Calculate target recording (including parking mode) durations for intervals from cm_vg_intervals.
cm_target_recording_durations_df = (
    cm_target_recording_durations_df.alias("a")
    .join(
        dashcam_target_recording_intervals_df.alias("b"),
        (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.cm_device_id") == F.col("b.device_id"))
        & (
            (F.col("a.start_ms") < F.col("b.end_ms"))
            & (F.col("a.end_ms") > F.col("b.start_ms"))
        ),
        "left",
    )
    .groupBy(
        "a.org_id",
        "a.device_id",
        "a.cm_device_id",
        "a.product_id",
        "a.cm_product_id",
        "a.start_ms",
        "a.end_ms",
        "a.duration_ms",
        "a.date",
        "a.target_recording_duration_ms",
        "a.grace_target_recording_duration_ms",
    )
    .agg(
        F.min(F.greatest(F.col("a.start_ms"), F.col("b.start_ms"))).alias(
            "interval_target_recording_start_ms"
        ),
        F.sum(
            F.when(
                (F.col("b.start_ms").isNotNull()) & (F.col("b.end_ms").isNotNull()),
                F.least(F.col("b.end_ms"), F.col("a.end_ms"))
                - F.greatest(F.col("b.start_ms"), F.col("a.start_ms")),
            ).otherwise(0)
        ).alias("target_recording_with_parking_duration_ms"),
    )
    # If CM target recording interval intersects the first 90 seconds of the interval,
    # assume target recording starts since the beginning of the interval.
    .withColumn(
        "grace_target_recording_with_parking_duration_ms",
        F.when(
            F.col("interval_target_recording_start_ms")
            < (F.col("start_ms") + 90 * 1000),
            F.col("target_recording_with_parking_duration_ms")
            + F.col("interval_target_recording_start_ms")
            - F.col("start_ms"),
        ).otherwise(F.col("target_recording_with_parking_duration_ms")),
    )
    .drop(
        "interval_target_recording_start_ms",
    )
)

# COMMAND ----------

create_or_update_table(
    "dataprep_firmware.cm_target_recording_durations",
    cm_target_recording_durations_df,
    "date",
    ["date", "org_id", "device_id", "cm_device_id", "start_ms", "end_ms"],
)
