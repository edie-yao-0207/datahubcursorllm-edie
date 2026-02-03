# Databricks notebook source
# MAGIC %run ./helpers

# COMMAND ----------

start_date, end_date = get_start_end_date(8, 0)

# Query this many days before the start_date to get the previous value
# for state based object stats which don't change often
prev_value_lookback_days = 7

# COMMAND ----------

query_end_ms = to_ms(end_date) + day_ms() - 1

# COMMAND ----------

osdpowerstate_df = spark.sql(
    """
select a.*
from kinesisstats.osdpowerstate as a
join dataprep_firmware.data_ingestion_high_water_mark as hwm
where a.date >= date_sub('{}', {})
  and a.date <= '{}'
  and a.time <= hwm.time_ms
""".format(
        start_date, prev_value_lookback_days, end_date
    )
)

vg_cm_assoc_df = spark.sql(
    """
  select
    date,
    org_id,
    device_id,
    cm_device_id
  from dataprep_safety.cm_device_health_daily
  where date >= date_sub('{}', {})
    and date <= '{}'
  group by
    date,
    org_id,
    device_id,
    cm_device_id
  """.format(
        start_date, prev_value_lookback_days, end_date
    )
)

power_state_intervals_df = create_intervals(
    osdpowerstate_df, lambda x: x == 1, query_end_ms
)

power_on_df = (
    osdpowerstate_df.select(
        "date",
        "time",
        "org_id",
        F.col("object_id").alias("device_id"),
        F.col("value.int_value"),
        F.col("value.proto_value.power_state_info.change_reason"),
    )
    .withColumn(
        "prev_int_value",
        F.lag(F.col("int_value"), 1).over(
            Window.partitionBy("org_id", "device_id").orderBy("time")
        ),
    )
    .filter("(prev_int_value is null or prev_int_value != 1) and int_value = 1")
    .withColumn(
        "boot_type",
        F.when(
            (F.col("prev_int_value").isNull()) | (F.col("prev_int_value") == 2), "cold"
        ).otherwise("warm"),
    )
)

vg_power_on_df = (
    power_on_df.alias("a")
    .join(
        vg_cm_assoc_df.alias("b"),
        (F.col("a.date") == F.col("b.date"))
        & (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.device_id")),
    )
    .select("a.date", "a.org_id", "a.device_id", "a.time", "a.boot_type")
).filter("change_reason != 1")

cm_power_on_df = (
    power_on_df.alias("a")
    .join(
        vg_cm_assoc_df.alias("b"),
        (F.col("a.date") == F.col("b.date"))
        & (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.cm_device_id")),
    )
    .select(
        "a.date", "a.org_id", "b.device_id", "b.cm_device_id", "a.time", "a.boot_type"
    )
)

vg_and_cm_power_on_df = (
    vg_power_on_df.alias("a")
    .join(
        power_state_intervals_df.alias("b"),
        (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.device_id"))
        & (F.col("a.time") == F.col("b.start_ms")),
    )
    .join(
        cm_power_on_df.alias("c"),
        (F.col("a.org_id") == F.col("c.org_id"))
        & (F.col("a.device_id") == F.col("c.device_id"))
        & (F.col("c.time") >= F.col("a.time"))
        & (F.col("c.time") < F.col("b.end_ms")),
    )
    .select(
        "a.date",
        "a.org_id",
        "a.device_id",
        F.col("c.cm_device_id"),
        F.col("c.time").alias("cm_power_on_time"),
        F.col("c.boot_type").alias("cm_boot_type"),
        F.col("a.time").alias("vg_power_on_time"),
        F.col("a.boot_type").alias("vg_boot_type"),
    )
)

vg_and_cm_power_on_df.createOrReplaceTempView("vg_and_cm_power_on")

cm_power_on_latency_df = spark.sql(
    """
  select
    date,
    org_id,
    device_id,
    vg_boot_type,
    vg_power_on_time,
    min_by(cm_device_id, cm_power_on_time - vg_power_on_time) as cm_device_id,
    min_by(cm_boot_type, cm_power_on_time - vg_power_on_time) as cm_boot_type,
    min_by(cm_power_on_time, cm_power_on_time - vg_power_on_time) as cm_power_on_time,
    min(cm_power_on_time - vg_power_on_time) as power_on_duration_ms
  from vg_and_cm_power_on
  group by
    date,
    org_id,
    device_id,
    vg_boot_type,
    vg_power_on_time
"""
)

create_or_update_table(
    "dataprep_firmware.cm_power_on_latency",
    cm_power_on_latency_df,
    "date",
    ["date", "org_id", "device_id", "vg_power_on_time"],
)

# COMMAND ----------

osdpowerstate_df = spark.sql(
    """
select a.*
from kinesisstats.osdpowerstate as a
join dataprep_firmware.data_ingestion_high_water_mark as hwm
where a.date >= date_sub('{}', {})
  and a.date <= '{}'
  and a.time <= hwm.time_ms
""".format(
        start_date, prev_value_lookback_days, end_date
    )
)

detections_raw_df = spark.sql(
    """
  select
    a.date,
    a.org_id,
    a.object_id as device_id,
    a.time,
    explode(a.value.proto_value.voltage_ignition_detection.detection_offsets_ms) as offset_ms
  from kinesisstats.osdvoltageignitiondetections as a
  join dataprep_firmware.data_ingestion_high_water_mark as hwm
  where a.value.proto_value.voltage_ignition_detection.metadata.sample_period_ms in (100, 500)
    and a.date >= date_sub('{}', {})
    and a.date <= '{}'
    and a.time <= hwm.time_ms
""".format(
        start_date, end_date, prev_value_lookback_days
    )
)

vg_cm_assoc_df = spark.sql(
    """
  select
    date,
    org_id,
    device_id,
    cm_device_id
  from dataprep_safety.cm_device_health_daily
  where date >= date_sub('{}', {})
    and date <= '{}'
  group by
    date,
    org_id,
    device_id,
    cm_device_id
  """.format(
        start_date, prev_value_lookback_days, end_date
    )
)

# Get low and moderate power intervals but filter out intervals less than 1 minute.
# If the interval is too short we are basically trying to wakeup while we are going to sleep
# which delays the wake up sequence a lot.
low_and_moderate_power_state_intervals_df = create_intervals(
    osdpowerstate_df, lambda x: x == 3 or x == 6, query_end_ms
).filter(F.col("end_ms") - F.col("start_ms") > 60 * 1000)


detections_df = detections_raw_df.select(
    "date", "org_id", "device_id", (F.col("time") - F.col("offset_ms")).alias("time")
)

ignition_to_power_on_df = (
    detections_df.alias("a")
    .join(
        low_and_moderate_power_state_intervals_df.alias("b"),
        (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.device_id"))
        & (F.col("a.time") >= F.col("b.start_ms"))
        & (F.col("a.time") < F.col("b.end_ms")),
    )
    .groupBy("a.date", "a.org_id", "a.device_id", "a.time")
    .agg(F.min(F.col("b.end_ms") - F.col("a.time")).alias("latency_ms"))
)


vg_ignition_to_power_on_df = (
    ignition_to_power_on_df.alias("a")
    .join(
        vg_cm_assoc_df.alias("b"),
        (F.col("a.date") == F.col("b.date"))
        & (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.device_id")),
    )
    .select(
        "a.date", "a.org_id", "a.device_id", "b.cm_device_id", "a.time", "a.latency_ms"
    )
)


create_or_update_table(
    "dataprep_firmware.ignition_detection_to_vg_power_on",
    vg_ignition_to_power_on_df,
    "date",
    ["date", "org_id", "device_id", "cm_device_id", "time"],
)

# COMMAND ----------

runtime_since_ign_df = spark.sql(
    """
  select
    a.date,
    a.org_id,
    a.object_id as device_id,
    count(*) as count,
    cast((a.time - 1000 * a.value.proto_value.engine_gauge_event.engine_on_secs) / 1000 as integer) as time_bucket,
    min(a.time - 1000 * a.value.proto_value.engine_gauge_event.engine_on_secs) as time
  from kinesisstats.osdenginegauge as a
  join dataprep_firmware.data_ingestion_high_water_mark as hwm
  where a.date >= '{}'
    and a.date <= '{}'
    and a.time <= hwm.time_ms
  group by
    time_bucket,
    a.date,
    a.org_id,
    a.object_id
""".format(
        start_date, end_date
    )
)

detections_df = detections_raw_df.select(
    "date", "org_id", "device_id", (F.col("time") - F.col("offset_ms")).alias("time")
)

vehicle_ignition_to_detection_df = (
    runtime_since_ign_df.filter("count > 1")
    .alias("a")
    .join(
        detections_df.alias("b"),
        (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.device_id"))
        & (F.col("a.time") <= F.col("b.time")),
    )
    .groupBy("a.date", "a.org_id", "a.device_id", "a.time")
    .agg(F.min(F.col("b.time") - F.col("a.time")).alias("latency_ms"))
    .filter("latency_ms < 10000")
)

vehicle_ignition_to_detection_with_cm_df = (
    vehicle_ignition_to_detection_df.alias("a")
    .join(
        vg_cm_assoc_df.alias("b"),
        (F.col("a.date") == F.col("b.date"))
        & (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.device_id")),
    )
    .select(
        "a.date", "a.org_id", "b.device_id", "b.cm_device_id", "a.time", "a.latency_ms"
    )
)

create_or_update_table(
    "dataprep_firmware.vehicle_ignition_to_detection",
    vehicle_ignition_to_detection_with_cm_df,
    "date",
    ["date", "org_id", "device_id", "cm_device_id", "time"],
)

# COMMAND ----------

osdpowerstate_df = spark.sql(
    """
select a.*
from kinesisstats.osdpowerstate as a
join dataprep_firmware.data_ingestion_high_water_mark as hwm
where a.date >= date_sub('{}', {})
  and a.date <= '{}'
  and a.time <= hwm.time_ms
""".format(
        start_date, prev_value_lookback_days, end_date
    )
)

osdrecording_df = spark.sql(
    """
select
  a.date,
  a.time,
  a.org_id,
  a.object_id,
  named_struct("int_value", a.value.int_value,
    "proto_value", null,
    "is_databreak", a.value.is_databreak) as value
from kinesisstats.osddashcamstate as a
join dataprep_firmware.data_ingestion_high_water_mark as hwm
where a.date >= date_sub('{}', {})
  and a.date <= '{}'
  and a.time <= hwm.time_ms
""".format(
        start_date, prev_value_lookback_days, end_date
    )
)

full_power_intervals_df = create_intervals(
    osdpowerstate_df, lambda x: x == 1, query_end_ms
)

recording_intervals_df = create_intervals(
    osdrecording_df, lambda x: x == 1, query_end_ms
)

power_on_to_recording_df = (
    recording_intervals_df.alias("a")
    .join(
        full_power_intervals_df.alias("b"),
        (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.device_id"))
        & (F.col("a.start_ms") >= F.col("b.start_ms"))
        & (F.col("a.start_ms") < F.col("b.end_ms")),
    )
    .groupBy("a.org_id", F.col("a.device_id").alias("cm_device_id"), "b.start_ms")
    .agg(F.min(F.col("a.start_ms") - F.col("b.start_ms")).alias("latency_ms"))
    .withColumn(
        "date",
        F.to_date(F.from_unixtime((F.col("start_ms") / 1000).cast(T.IntegerType()))),
    )
)

create_or_update_table(
    "dataprep_firmware.cm_power_on_to_recording_df",
    power_on_to_recording_df,
    "date",
    ["date", "org_id", "cm_device_id", "start_ms"],
)
