# Databricks notebook source
# MAGIC %run ./helpers

# COMMAND ----------

start_date, end_date = get_start_end_date(8, 1)

# COMMAND ----------

database_name = "dataprep_firmware"
table_name = "cm_uptime"
table_suffix = ""  # Table suffix is used during development to version tables

# COMMAND ----------

# Retrieve only full power intervals. See the proto DevicePowerState
# in the following file for power_state definitions:
# go/src/samsaradev.io/hubproto/logeventproto/log.proto
device_full_power_intervals_df = spark.sql(
    """--sql
    SELECT
        date
        , org_id
        , device_id
        , start_ms
        , end_ms
    FROM
        dataprep_firmware.device_full_power_intervals_v3
    WHERE
        date >= '{}'
        AND date <= '{}'
    --endsql
    """.format(
        start_date, end_date
    )
)

# Retrieve engine on intervals. See the proto EngineState
# in the following file for engine_state definitions:
# go/src/samsaradev.io/fleet/fuel/fuelhelpers/engine_state.go
device_engine_on_intervals_df = spark.sql(
    """--sql
    SELECT
        date
        , org_id
        , device_id
        , start_ms
        , end_ms
    FROM
        dataprep_firmware.device_engine_on_intervals
    WHERE
        date >= '{}'
        AND date <= '{}'
    --endsql
    """.format(
        start_date, end_date
    )
)

# Retrieve the intervals during which a dashcam was connected to the VG.
# These intervals will be intersected with CM full power intervals to get the numerator of cm_uptime.
# TODO: This is an alternative to measuring dashcam target state directly, which is currently on-hold
# due to an issue with some CM's not sending the correct signals.
cm_vg_connected_intervals_df = spark.sql(
    """--sql
    SELECT
        date
        , org_id
        , device_id
        , start_ms
        , end_ms
    FROM
        dataprep_firmware.cm_vg_connected_intervals_v3
    WHERE
        date >= '{}'
        AND date <= '{}'
    --endsql
    """.format(
        start_date, end_date
    )
)

# Retrieve the intervals during which a camera connector (Octo) was connected to the VG.
# These intervals will be intersected with Octo full power intervals to get the numerator of cm_uptime.
# TODO: This is an alternative to measuring Octo target state directly, which is currently on-hold
# due to an issue with some Octo's not sending the correct signals.
octo_vg_connected_intervals_df = spark.sql(
    """--sql
    SELECT
        date
        , org_id
        , device_id
        , start_ms
        , end_ms
    FROM
        dataprep_firmware.octo_vg_connected_intervals
    WHERE
        date >= '{}'
        AND date <= '{}'
    --endsql
    """.format(
        start_date, end_date
    )
)

# TODO: Use CM powered and connected intervals for now while Recording State Manager target state
# issues are being resolved.
# Pull all intervals for target_state; i.e., any time the Recording State Manager (RSM) was running.
# dashcam_target_state_intervals_df = spark.sql(
#     """
#     SELECT
#         date
#         , org_id
#         , device_id
#         , start_ms
#         , end_ms
#     FROM dataprep_firmware.cm_dashcam_target_state_intervals
#     WHERE
#         date >= '{}'
#         AND date <= '{}'
#     """.format(
#         start_date.strftime("%Y-%m-%d"),
#         end_date.strftime("%Y-%m-%d")
#     )
# )

# Only retrieve CMs with a firmware version that has Recording State Manager (RSM) enabled
valid_firmware_df = spark.sql(
    """--sql
    SELECT
        date
        , org_id
        , cm_device_id
        , vg_device_id
        , cm_product_id
    FROM
        dataprep_firmware.cm_uptime_eligible_devices
    WHERE
        date >= '{}'
        AND date <= '{}'
    --endsql
    """.format(
        start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
    )
)

# COMMAND ----------

# Capture a single consistent timestamp for the transformation,
# rather than evaluating current_timestamp() separately for each row,
# which could produce slightly different values across rows.
ts = spark.sql("SELECT current_timestamp()").collect()[0][0]

# Retrieve full power intervals for those VGs that are attached to active CMs.

# While a vehicle is inactive, the VG will power on every 6hrs for 3 minutes.
# This query will exclude power intervals that are less than 3 minutes long;
# the intent here is to exclude these brief power on durations during otherwise inactive periods.

# Note: A previous version of this table filtered out these short intervals because
# the signal osDDashcamTargetState would not switch off during these intervals.
# These intervals are being excluded here not because we are concerned about
# osDDashcamTargetState not switching off/on during these periods, but because
# when the VG-CM is in such a brief wake period, both will appear powered on and
# connected, even though the camera should not be recording.
valid_active_vg_full_power_intervals_df = (
    valid_firmware_df.alias("a")
    .join(
        device_full_power_intervals_df.alias("b"),
        (F.col("a.date") == F.col("b.date"))
        & (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.vg_device_id") == F.col("b.device_id")),
    )
    .select(
        "a.date",
        "a.org_id",
        "a.cm_device_id",
        "a.vg_device_id",
        "b.start_ms",
        "b.end_ms",
    )
    .withColumn(
        "datetime_logged", F.lit(ts)
    )  # Capture current timestamp for troubleshooting purposes
    .dropDuplicates()
).filter(F.col("end_ms") - F.col("start_ms") > F.lit(1000 * 60 * 3.5))

# Retrieve dashcam connected intervals for VGs that have a CM connected to them.
valid_active_cm_vg_connected_intervals_df = (
    valid_firmware_df.alias("a")
    .join(
        cm_vg_connected_intervals_df.alias("b"),
        (F.col("a.date") == F.col("b.date"))
        & (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.vg_device_id") == F.col("b.device_id")),
    )
    .select(
        "a.date",
        "a.org_id",
        "a.cm_device_id",
        "a.vg_device_id",
        "a.cm_product_id",
        "b.start_ms",
        "b.end_ms",
    )
    # Omit octo devices. Intervals for them will be sourced from osdmulticamconnected.
    .filter(
        (F.col("cm_product_id").isNotNull()) & (~F.col("cm_product_id").isin(126, 213))
    )
    .withColumn(
        "datetime_logged", F.lit(ts)
    )  # Capture current timestamp for troubleshooting purposes
    .dropDuplicates()
)

# Retrieve multicam connected intervals for VGs that have an Octo connected to them.
valid_active_octo_vg_connected_intervals_df = (
    valid_firmware_df.alias("a")
    .join(
        octo_vg_connected_intervals_df.alias("b"),
        (F.col("a.date") == F.col("b.date"))
        & (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.vg_device_id") == F.col("b.device_id")),
    )
    .select(
        "a.date",
        "a.org_id",
        "a.cm_device_id",
        "a.vg_device_id",
        "a.cm_product_id",
        "b.start_ms",
        "b.end_ms",
    )
    # Only select Octo devices. Intervals for CMs were sourced above (from osddashcamconnected).
    .filter((F.col("cm_product_id").isin(126, 213)))
    .withColumn(
        "datetime_logged", F.lit(ts)
    )  # Capture current timestamp for troubleshooting purposes
    .dropDuplicates()
)

# Retrieve full power intervals for the active CMs/Octos.
# We will be using this as the numerator in cm_uptime, as opposed to dashcam target state intervals, for now
# since issues with the osddashcamtargetstate kinesisstat were identified.
valid_active_cm_octo_full_power_intervals_df = (
    valid_firmware_df.alias("a")
    .join(
        device_full_power_intervals_df.alias("b"),
        (F.col("a.date") == F.col("b.date"))
        & (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.cm_device_id") == F.col("b.device_id")),
    )
    .select(
        "a.date",
        "a.org_id",
        "a.cm_device_id",
        "a.vg_device_id",
        "b.start_ms",
        "b.end_ms",
    )
    .withColumn(
        "datetime_logged", F.lit(ts)
    )  # Capture current timestamp for troubleshooting purposes
    .dropDuplicates()
)

# Retrieve engine on intervals for those VGs that are attached to active CMs.
# This is used to calculate cm_uptime_engine_on (uptime during engine on periods).
valid_active_vg_engine_on_intervals_df = (
    valid_firmware_df.alias("a")
    .join(
        device_engine_on_intervals_df.alias("b"),
        (F.col("a.date") == F.col("b.date"))
        & (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.vg_device_id") == F.col("b.device_id")),
    )
    .select(
        "a.date",
        "a.org_id",
        "a.cm_device_id",
        "a.vg_device_id",
        "b.start_ms",
        "b.end_ms",
    )
    .withColumn(
        "datetime_logged", F.lit(ts)
    )  # Capture current timestamp for troubleshooting purposes
    .dropDuplicates()
)

# COMMAND ----------

# Delete any existing rows for the specified date range before inserting updated data.
# This ensures that late-arriving or updated signal data doesn't result in overlapping or duplicated intervals.
# The deletion is scoped by `date` to allow safe, idempotent updates without full table recomputation.
delete_entries_by_date(
    f"{database_name}.valid_active_vg_full_power_intervals{table_suffix}",
    start_date,
    end_date,
)
delete_entries_by_date(
    f"{database_name}.valid_active_cm_vg_connected_intervals{table_suffix}",
    start_date,
    end_date,
)
delete_entries_by_date(
    f"{database_name}.valid_active_octo_vg_connected_intervals{table_suffix}",
    start_date,
    end_date,
)
delete_entries_by_date(
    f"{database_name}.valid_active_cm_octo_full_power_intervals{table_suffix}",
    start_date,
    end_date,
)
delete_entries_by_date(
    f"{database_name}.valid_active_vg_engine_on_intervals{table_suffix}",
    start_date,
    end_date,
)

# COMMAND ----------

create_or_update_table(
    f"{database_name}.valid_active_vg_full_power_intervals{table_suffix}",
    valid_active_vg_full_power_intervals_df,
    "date",
    ["date", "org_id", "cm_device_id", "vg_device_id", "start_ms"],
)

create_or_update_table(
    f"{database_name}.valid_active_cm_vg_connected_intervals{table_suffix}",
    valid_active_cm_vg_connected_intervals_df,
    "date",
    ["date", "org_id", "cm_device_id", "vg_device_id", "start_ms"],
)

create_or_update_table(
    f"{database_name}.valid_active_octo_vg_connected_intervals{table_suffix}",
    valid_active_octo_vg_connected_intervals_df,
    "date",
    ["date", "org_id", "cm_device_id", "vg_device_id", "start_ms"],
)

create_or_update_table(
    f"{database_name}.valid_active_cm_octo_full_power_intervals{table_suffix}",
    valid_active_cm_octo_full_power_intervals_df,
    "date",
    ["date", "org_id", "cm_device_id", "vg_device_id", "start_ms"],
)

create_or_update_table(
    f"{database_name}.valid_active_vg_engine_on_intervals{table_suffix}",
    valid_active_vg_engine_on_intervals_df,
    "date",
    ["date", "org_id", "cm_device_id", "vg_device_id", "start_ms"],
)

# COMMAND ----------

# Remove references to dataframes that have now been written to tables
# so that the references can be reused without risk of memory or reference leakage
del valid_active_vg_full_power_intervals_df
del valid_active_cm_vg_connected_intervals_df
del valid_active_octo_vg_connected_intervals_df
del valid_active_cm_octo_full_power_intervals_df
del valid_active_vg_engine_on_intervals_df

# COMMAND ----------

valid_active_vg_full_power_intervals_df = spark.sql(
    """--sql
    SELECT *
    FROM {}
    WHERE
        date >= '{}'
        AND date <= '{}'
    --endsql
    """.format(
        f"{database_name}.valid_active_vg_full_power_intervals{table_suffix}",
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    )
)

valid_active_cm_vg_connected_intervals_df = spark.sql(
    """--sql
    SELECT *
    FROM {}
    WHERE
        date >= '{}'
        AND date <= '{}'
    --endsql
    """.format(
        f"{database_name}.valid_active_cm_vg_connected_intervals{table_suffix}",
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    )
)

valid_active_octo_vg_connected_intervals_df = spark.sql(
    """--sql
    SELECT *
    FROM {}
    WHERE
        date >= '{}'
        AND date <= '{}'
    --endsql
    """.format(
        f"{database_name}.valid_active_octo_vg_connected_intervals{table_suffix}",
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    )
)

valid_active_cm_octo_full_power_intervals_df = spark.sql(
    """--sql
    SELECT *
    FROM {}
    WHERE
        date >= '{}'
        AND date <= '{}'
    --endsql
    """.format(
        f"{database_name}.valid_active_cm_octo_full_power_intervals{table_suffix}",
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    )
)

valid_active_vg_engine_on_intervals_df = spark.sql(
    """--sql
    SELECT *
    FROM {}
    WHERE
        date >= '{}'
        AND date <= '{}'
    --endsql
    """.format(
        f"{database_name}.valid_active_vg_engine_on_intervals{table_suffix}",
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    )
)

# COMMAND ----------

# VGs can receive a signal that a recording device is connected from osddashcamconnected or
# osdmulticamsconnected. Intervals for those devices were stored separately and are combined here.
valid_active_cm_octo_vg_connected_intervals_df = (
    valid_active_cm_vg_connected_intervals_df.union(
        valid_active_octo_vg_connected_intervals_df
    )
)

# Find intervals where CM's are at full-power and connected to the VG.
# ----
# valid_active_cm_octo_full_power_intervals_df will always only identify a single set of
# intervals for a CM. On the other hand, valid_active_cm_octo_vg_connected_intervals_df
# may identify multiple sets of intervals associated with a CM, with each set being the
# CM-VG connected intervals for a given VG that a CM was connected to on that day.
# Since we do not want to intersect the CM-VG connection intervals of a CM-VG pairing
# with the CM-VG connection intervals of another CM-VG pairing, use the function
# intersect_intervals_cm_vg, which joins to intervals on vg_device_id and cm_device_id.
valid_active_cm_octo_full_power_vg_connected_intervals_df = intersect_intervals_cm_vg(
    valid_active_cm_octo_full_power_intervals_df,
    valid_active_cm_octo_vg_connected_intervals_df,
)

# Capture a single consistent timestamp for the transformation,
# rather than evaluating current_timestamp() separately for each row,
# which could produce slightly different values across rows.
ts = spark.sql("SELECT current_timestamp()").collect()[0][0]

# Intersect CM full power intervals with VG full power intervals.
# This is to eliminate edge cases where CM full power intervals are erroneously greater
# than VG full power intervals.
valid_active_cm_octo_full_power_vg_connected_intervals_df = intersect_intervals_cm_vg(
    valid_active_vg_full_power_intervals_df,
    valid_active_cm_octo_full_power_vg_connected_intervals_df,
)


# For each CM, count the number of unique VGs it was connected to at full power per day.
# This will be brought forward into the final table to flag which CM's connected to
# multiple VG's on a given day.
device_counts_df = valid_active_cm_octo_full_power_vg_connected_intervals_df.groupBy(
    "date", "org_id", "cm_device_id"
).agg(F.countDistinct("vg_device_id").alias("count_vg_device_id"))

valid_active_cm_octo_full_power_vg_connected_intervals_with_count_df = (
    valid_active_cm_octo_full_power_vg_connected_intervals_df.join(
        device_counts_df, on=["date", "org_id", "cm_device_id"], how="left"
    ).withColumn("datetime_logged", F.lit(ts))
)  # Capture current timestamp for troubleshooting purposes

# Only select the target_state intervals (i.e., Recording State Manager uptime intervals)
# for active CMs where the associated VG was at full power.
# RSM may report intervals even when the VG is not at full power for a number of reasons:
# - Slight discrepencies when VG goes to low power and then RSM stops a few seconds after
# - RSM runs in moderate power at times
# recording_state_manager_full_power_intervals_df = intersect_intervals(
#     valid_active_vg_full_power_intervals_df,
#     dashcam_target_state_intervals_df,
# )

# Find intervals where CM's/Octo's are connected to the VG during engine on periods.
# This is used to calculate cm_uptime_engine_on (uptime during engine on periods).
valid_active_cm_octo_connected_vg_engine_on_intervals_df = intersect_intervals_cm_vg(
    valid_active_vg_engine_on_intervals_df,
    valid_active_cm_octo_vg_connected_intervals_df,
)

# For each CM, count the number of unique VGs it was connected to during engine on per day.
device_counts_engine_on_df = (
    valid_active_cm_octo_connected_vg_engine_on_intervals_df.groupBy(
        "date", "org_id", "cm_device_id"
    ).agg(F.countDistinct("vg_device_id").alias("count_vg_device_id_engine_on"))
)

valid_active_cm_octo_connected_vg_engine_on_intervals_with_count_df = (
    valid_active_cm_octo_connected_vg_engine_on_intervals_df.join(
        device_counts_engine_on_df, on=["date", "org_id", "cm_device_id"], how="left"
    ).withColumn("datetime_logged", F.lit(ts))
)  # Capture current timestamp for troubleshooting purposes

# COMMAND ----------

# Delete any existing rows for the specified date range before inserting updated data.
# This ensures that late-arriving or updated signal data doesn't result in overlapping or duplicated intervals.
# The deletion is scoped by `date` to allow safe, idempotent updates without full table recomputation.
delete_entries_by_date(
    f"{database_name}.valid_active_cm_octo_full_power_vg_connected_intervals{table_suffix}",
    start_date,
    end_date,
)
delete_entries_by_date(
    f"{database_name}.valid_active_cm_octo_connected_vg_engine_on_intervals{table_suffix}",
    start_date,
    end_date,
)

create_or_update_table(
    f"{database_name}.valid_active_cm_octo_full_power_vg_connected_intervals{table_suffix}",
    valid_active_cm_octo_full_power_vg_connected_intervals_with_count_df,
    "date",
    ["date", "org_id", "cm_device_id", "vg_device_id", "start_ms"],
)

create_or_update_table(
    f"{database_name}.valid_active_cm_octo_connected_vg_engine_on_intervals{table_suffix}",
    valid_active_cm_octo_connected_vg_engine_on_intervals_with_count_df,
    "date",
    ["date", "org_id", "cm_device_id", "vg_device_id", "start_ms"],
)

# Remove references to dataframes that have now been written to tables so that
# so that the references can be reused without risk of memory or reference leakage
del valid_active_cm_octo_full_power_vg_connected_intervals_with_count_df
del valid_active_cm_octo_connected_vg_engine_on_intervals_with_count_df

# COMMAND ----------

valid_active_cm_octo_full_power_vg_connected_intervals_df = spark.sql(
    """--sql
    SELECT *
    FROM {}
    WHERE
        date >= '{}'
        AND date <= '{}'
    --endsql
    """.format(
        f"{database_name}.valid_active_cm_octo_full_power_vg_connected_intervals{table_suffix}",
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    )
)

valid_active_cm_octo_connected_vg_engine_on_intervals_df = spark.sql(
    """--sql
    SELECT *
    FROM {}
    WHERE
        date >= '{}'
        AND date <= '{}'
    --endsql
    """.format(
        f"{database_name}.valid_active_cm_octo_connected_vg_engine_on_intervals{table_suffix}",
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    )
)

# COMMAND ----------

# Summing all the intervals should give us the total time the Recording State Manager (RSM) was active.
# recording_state_manager_full_power_intervals_agg_df = (
#     recording_state_manager_full_power_intervals_df.groupBy("date", "org_id", "device_id")
#     .agg(F.sum(F.col("end_ms") - F.col("start_ms")).alias("recording_state_manager_running_ms"))
# )

# Summing all the intervals should give us the total time the CM was connected to the VG
# and at full power.
# NOTE: For CM's that connected to multiple VG's on a given day, the sum of the durations
# will be <= 24*y, where y is the number of VG's the CM connected to on that day.
valid_active_cm_octo_full_power_vg_connected_intervals_agg_df = (
    valid_active_cm_octo_full_power_vg_connected_intervals_df.groupBy(
        "date", "org_id", "cm_device_id", "count_vg_device_id"
    )
    .agg(
        # Coalesce handles NULL from empty sum (when a CM exists but has no intervals in the group)
        F.coalesce(F.sum(F.col("end_ms") - F.col("start_ms")), F.lit(0)).alias(
            "cm_full_power_connected_ms"
        )
    )
    .withColumnRenamed(
        "cm_device_id", "device_id"
    )  # Rename since all future references will be to cm_device_id
)

# Summing all the intervals should give us the total time the VG was at full power.
valid_active_vg_full_power_intervals_agg_df = (
    valid_active_vg_full_power_intervals_df.groupBy("date", "org_id", "cm_device_id")
    .agg(F.sum(F.col("end_ms") - F.col("start_ms")).alias("vg_full_power_ms"))
    .withColumnRenamed(
        "cm_device_id", "device_id"
    )  # Rename since all future references will be to cm_device_id
)

# Summing all the intervals should give us the total time the CM was connected to the VG
# during engine on periods.
# NOTE: For CM's that connected to multiple VG's on a given day, the sum of the durations
# will be <= 24*y, where y is the number of VG's the CM connected to on that day.
valid_active_cm_octo_connected_vg_engine_on_intervals_agg_df = (
    valid_active_cm_octo_connected_vg_engine_on_intervals_df.groupBy(
        "date", "org_id", "cm_device_id", "count_vg_device_id_engine_on"
    )
    .agg(
        # Coalesce handles NULL from empty sum (when a CM exists but has no intervals in the group)
        F.coalesce(F.sum(F.col("end_ms") - F.col("start_ms")), F.lit(0)).alias(
            "cm_connected_engine_on_ms"
        )
    )
    .withColumnRenamed(
        "cm_device_id", "device_id"
    )  # Rename since all future references will be to cm_device_id
)

# Summing all the intervals should give us the total time the VG had engine on.
valid_active_vg_engine_on_intervals_agg_df = (
    valid_active_vg_engine_on_intervals_df.groupBy("date", "org_id", "cm_device_id")
    .agg(F.sum(F.col("end_ms") - F.col("start_ms")).alias("vg_engine_on_ms"))
    .withColumnRenamed(
        "cm_device_id", "device_id"
    )  # Rename since all future references will be to cm_device_id
)

# COMMAND ----------

# Capture a single consistent timestamp for the transformation,
# rather than evaluating current_timestamp() separately for each row,
# which could produce slightly different values across rows.
ts = spark.sql("SELECT current_timestamp()").collect()[0][0]

# First create the engine on uptime dataframe
cm_uptime_engine_on_df = (
    valid_active_vg_engine_on_intervals_agg_df.alias("a")
    .join(
        valid_active_cm_octo_connected_vg_engine_on_intervals_agg_df.alias("b"),
        ["date", "org_id", "device_id"],
        how="left",
    )
    .withColumn(
        "cm_uptime_engine_on",
        # When LEFT JOIN finds no CM connection data, coalesce converts NULL to 0,
        # resulting in 0/denominator = 0 (zero uptime, not unknown).
        F.round(
            F.coalesce(F.col("b.cm_connected_engine_on_ms"), F.lit(0))
            / F.col("a.vg_engine_on_ms"),
            8,
        ),
    )
    .select(
        "a.date",
        "a.org_id",
        "a.device_id",
        "a.vg_engine_on_ms",
        "b.cm_connected_engine_on_ms",
        "cm_uptime_engine_on",
        "b.count_vg_device_id_engine_on",
    )
)

# Then create the full power uptime dataframe and LEFT JOIN engine on metrics
cm_uptime_df = (
    valid_active_vg_full_power_intervals_agg_df.alias("a")
    .join(
        # recording_state_manager_full_power_intervals_agg_df.alias("b"),
        valid_active_cm_octo_full_power_vg_connected_intervals_agg_df.alias("b"),
        ["date", "org_id", "device_id"],
        how="left",
    )
    .withColumn(
        "cm_uptime",
        # When LEFT JOIN finds no CM connection data, coalesce converts NULL to 0,
        # resulting in 0/denominator = 0 (zero uptime, not unknown).
        F.round(
            F.coalesce(F.col("b.cm_full_power_connected_ms"), F.lit(0))
            / F.col("a.vg_full_power_ms"),
            8,
        ),
    )
    .select(
        "a.date",
        "a.org_id",
        "a.device_id",
        "a.vg_full_power_ms",
        "b.cm_full_power_connected_ms",
        "cm_uptime",
        "b.count_vg_device_id",
    )
    .join(
        cm_uptime_engine_on_df.alias("c"),
        ["date", "org_id", "device_id"],
        how="left",
    )
    .select(
        "date",
        "org_id",
        "device_id",
        "vg_full_power_ms",
        "cm_full_power_connected_ms",
        "cm_uptime",
        "count_vg_device_id",
        "c.vg_engine_on_ms",
        "c.cm_connected_engine_on_ms",
        "c.cm_uptime_engine_on",
        "c.count_vg_device_id_engine_on",
    )
    .withColumn(
        "datetime_logged", F.lit(ts)
    )  # Capture current timestamp for troubleshooting purposes
)

# COMMAND ----------

# Assert that no device exceeds 24*y hours of interval durations in a day,
# where y is the number of VG's the CM connected to on that day.
max_cm_full_power_connected_hours = cm_uptime_df.select(
    F.max(
        F.col("cm_full_power_connected_ms")
        / F.col("count_vg_device_id")
        / (1000 * 60 * 60)
    )
).collect()[0][0]
assert (
    max_cm_full_power_connected_hours <= 24.0
), f"Found device with {max_cm_full_power_connected_hours:.2f} hours of CM full power and connected time in a single day"

# NOTE: Don't include a DQ check for vg_full_power_ms since count_vg_device_id is not
# calculated separately for the vg_full_power_intervals_df.

# COMMAND ----------

# Ensure that no device has an impossible cm_uptime greater than 1.0
max_cm_uptime = cm_uptime_df.select(F.max("cm_uptime")).collect()[0][0]
assert max_cm_uptime <= 1.0, f"Found device with cm_uptime {max_cm_uptime:.8f} > 1.0"

# COMMAND ----------

# Assert that no device exceeds 24*y hours of interval durations in a day for engine on,
# where y is the number of VG's the CM connected to on that day.
max_cm_connected_engine_on_hours = cm_uptime_df.select(
    F.max(
        F.col("cm_connected_engine_on_ms")
        / F.col("count_vg_device_id_engine_on")
        / (1000 * 60 * 60)
    )
).collect()[0][0]
# Only assert if there are engine on records (max won't be None)
if max_cm_connected_engine_on_hours is not None:
    assert (
        max_cm_connected_engine_on_hours <= 24.0
    ), f"Found device with {max_cm_connected_engine_on_hours:.2f} hours of CM connected during engine on time in a single day"

# COMMAND ----------

# Ensure that no device has an impossible cm_uptime_engine_on greater than 1.0
max_cm_uptime_engine_on = cm_uptime_df.select(F.max("cm_uptime_engine_on")).collect()[
    0
][0]
# Only assert if there are engine on records (max won't be None)
if max_cm_uptime_engine_on is not None:
    assert (
        max_cm_uptime_engine_on <= 1.0
    ), f"Found device with cm_uptime_engine_on {max_cm_uptime_engine_on:.8f} > 1.0"

# COMMAND ----------

# Delete any existing rows for the specified date range before inserting updated data.
# This ensures that late-arriving or updated signal data doesn't result in overlapping or duplicated intervals.
# The deletion is scoped by `date` to allow safe, idempotent updates without full table recomputation.
delete_entries_by_date(
    f"{database_name}.{table_name}{table_suffix}", start_date, end_date
)

create_or_update_table(
    f"{database_name}.{table_name}{table_suffix}",
    cm_uptime_df,
    "date",
    ["date", "org_id", "device_id"],
)
