# Databricks notebook source
# MAGIC %run ./helpers

# COMMAND ----------

start_date, end_date = get_start_end_date(8, 1)

# Query this many days before the start_date to get the previous value
# for state based object stats which don't change often
prev_value_lookback_days = 7

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)

# COMMAND ----------

query_end_ms = to_ms(end_date) + day_ms() - 1

# COMMAND ----------

device_builds_df = spark.sql(
    """
  select 
    date,
    org_id,
    device_id,
    latest_build_on_day,
    active_builds_on_day
  from dataprep.device_builds
  where date >= '{}'
    and date <= '{}'
""".format(
        start_date, end_date
    )
)

vg_devices_df = spark.sql(
    """
   select
    org_id,
    device_id,
    product_id
  from productsdb.gateways
  where product_id in (24, 35, 53, 89)
"""
)

cable_ids_df = spark.sql(
    """
  select 
    date,
    time,
    org_id,
    object_id as device_id,
    value.int_value
  from kinesisstats.osdobdcableid
  where value.int_value != 0 
    and value.int_value is not null 
    and date >= date_sub('{}', {})
    and date <= '{}'
""".format(
        start_date, prev_value_lookback_days, end_date
    )
)

voltage_cfg = spark.sql(
    """
   select
      date,
      time,
      org_id,
      object_id,
      named_struct("int_value",
      case
        when first(s3_proto_value.reported_device_config.device_config.voltage_processor_config.polling_interval_ms) in (100, 500) then 1
        else 0
      end,
      "proto_value", null,
      "is_databreak", false) as value
  from s3bigstats.osdreporteddeviceconfig_raw
  where date >= date_sub('{}', {})
    and date <= '{}'
  group by
      date,
      time,
      org_id,
      object_id
""".format(
        start_date, prev_value_lookback_days, end_date
    )
)

date_range_df = spark.createDataFrame(
    data=[(date_sub(start_date, prev_value_lookback_days), end_date)],
    schema=T.StructType(
        [
            T.StructField("start_date", T.DateType(), True),
            T.StructField("end_date", T.DateType(), True),
        ]
    ),
)
dates_df = (
    date_range_df.withColumn(
        "date", F.explode(F.expr("sequence(start_date, end_date, interval 1 day)"))
    )
    .drop("start_date")
    .drop("end_date")
)
dates_devices_df = vg_devices_df.hint("broadcast").crossJoin(dates_df)

cable_id_dates_df = dates_devices_df.join(
    cable_ids_df,
    (dates_devices_df.date == cable_ids_df.date)
    & (dates_devices_df.org_id == cable_ids_df.org_id)
    & (dates_devices_df.device_id == cable_ids_df.device_id),
    how="left",
).select(
    dates_devices_df.date,
    dates_devices_df.org_id,
    dates_devices_df.device_id,
    F.coalesce(
        cable_ids_df.time, (1000 * F.unix_timestamp(dates_devices_df.date))
    ).alias("time"),
    cable_ids_df.int_value,
)

windowSpec = Window.partitionBy("org_id", "device_id").orderBy("time")
cable_id_dates_df = cable_id_dates_df.withColumn(
    "cable_id", F.last(F.col("int_value"), True).over(windowSpec)
).drop("int_value")

vehicles_types_df = (
    cable_id_dates_df.drop("grouper")
    .drop("int_value")
    .withColumn(
        "vehicle_type",
        F.expr(
            "case when cable_id in (1, 2, 3, 5, 6, 7, 9, 10, 11, 12, 14, 15, 17) then 'heavy_duty' when cable_id in (4, 13, 16, 18, 19, 20, 21, 22, 23, 24) then 'light_duty' end"
        ),
    )
    .filter(F.col("vehicle_type") != "")
    .drop("cable_id")
    .drop("heavy_duty")
    .drop("light_duty")
    .groupBy("date", "org_id", "device_id")
    .agg(
        F.first("vehicle_type").alias("vehicle_type"),
        F.countDistinct("vehicle_type").alias("vehicle_types_on_day"),
    )
    .filter((F.col("vehicle_types_on_day") <= 1) & (F.col("date") >= start_date))
)

voltage_algo_v2_intervals_daily_df = create_intervals_daily(
    create_intervals(voltage_cfg, lambda x: x > 0, query_end_ms),
    start_date,
    end_date,
)
voltage_algo_v2_intervals_daily_complete_df = voltage_algo_v2_intervals_daily_df.filter(
    F.col("end_ms") - F.col("start_ms") == 86400e3 - 1
)

target_device_days_df = (
    device_builds_df.alias("a")
    .join(
        vg_devices_df.alias("b"),
        (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.device_id")),
    )
    .join(
        vehicles_types_df.alias("c"),
        (F.col("a.org_id") == F.col("c.org_id"))
        & (F.col("a.device_id") == F.col("c.device_id"))
        & (F.col("a.date") == F.col("c.date")),
    )
    .join(
        voltage_algo_v2_intervals_daily_complete_df.alias("d"),
        (F.col("a.org_id") == F.col("d.org_id"))
        & (F.col("a.device_id") == F.col("d.device_id"))
        & (F.col("a.date") == F.col("d.date")),
    )
    .filter(F.col("a.active_builds_on_day") <= 1)
    .select(
        "a.date",
        "a.org_id",
        "a.device_id",
        "b.product_id",
        "a.latest_build_on_day",
        "c.vehicle_type",
    )
)

osdenginestate_df = spark.sql(
    """
  select *
  from kinesisstats.osdenginestate
  where date >= date_sub('{}', {})
    and date <= '{}'
""".format(
        start_date, prev_value_lookback_days, end_date
    )
)

osdpowerstate_df = spark.sql(
    """
  select *
  from kinesisstats.osdpowerstate
  where date >= date_sub('{}', {})
    and date <= '{}'
""".format(
        start_date, prev_value_lookback_days, end_date
    )
)

detections_raw_df = spark.sql(
    """
  select 
    date,
    org_id,
    object_id as device_id,
    time,
    explode(value.proto_value.voltage_ignition_detection.detection_offsets_ms) as offset_ms
  from kinesisstats.osdvoltageignitiondetections
  where value.proto_value.voltage_ignition_detection.metadata.sample_period_ms in (100, 500)
    and date >= date_sub('{}', {})
    and date <= '{}'
""".format(
        start_date, prev_value_lookback_days, end_date
    )
)

detections_df = detections_raw_df.select(
    "date", "org_id", "device_id", (F.col("time") - F.col("offset_ms")).alias("time")
)


engine_on_intervals_df = create_intervals(
    osdenginestate_df, lambda x: x > 0, query_end_ms
)

labels_df = engine_on_intervals_df.select(
    "org_id",
    "device_id",
    F.to_date(F.from_unixtime((F.col("start_ms") / 1000).cast(T.IntegerType()))).alias(
        "date"
    ),
    F.col("start_ms").alias("time"),
)

daily_eng_on_intervals_df = create_intervals_daily(
    engine_on_intervals_df, start_date, end_date
)

labels_df = (
    target_device_days_df.alias("a")
    .join(
        labels_df.alias("b"),
        (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.device_id"))
        & (F.col("a.date") == F.col("b.date")),
        how="left",
    )
    .groupBy(
        "a.date",
        "a.org_id",
        "a.device_id",
        "a.product_id",
        "a.vehicle_type",
        "a.latest_build_on_day",
    )
    .agg(F.array_sort(F.collect_list("b.time")).alias("labels"))
)

detections_df = (
    target_device_days_df.alias("a")
    .join(
        detections_df.alias("b"),
        (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.device_id"))
        & (F.col("a.date") == F.col("b.date")),
        how="left",
    )
    .groupBy(
        "a.date",
        "a.org_id",
        "a.device_id",
        "a.product_id",
        "a.vehicle_type",
        "a.latest_build_on_day",
    )
    .agg(F.array_sort(F.collect_list("b.time")).alias("detections"))
)

allowed_fp_intervals_df = (
    target_device_days_df.alias("a")
    .join(
        daily_eng_on_intervals_df.alias("b"),
        (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.device_id"))
        & (F.col("a.date") == F.col("b.date")),
        how="left",
    )
    .groupBy(
        "a.date",
        "a.org_id",
        "a.device_id",
        "a.product_id",
        "a.vehicle_type",
        "a.latest_build_on_day",
    )
    .agg(
        F.collect_list(F.struct(F.col("start_ms"), F.col("end_ms"))).alias(
            "allowed_fp_intervals"
        )
    )
)

labels_detections_df = (
    labels_df.alias("a")
    .join(
        detections_df.alias("b"),
        (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.device_id"))
        & (F.col("a.date") == F.col("b.date")),
    )
    .join(
        allowed_fp_intervals_df.alias("c"),
        (F.col("a.org_id") == F.col("c.org_id"))
        & (F.col("a.device_id") == F.col("c.device_id"))
        & (F.col("a.date") == F.col("c.date")),
    )
    .select(
        "a.date",
        "a.org_id",
        "a.device_id",
        "a.product_id",
        "a.vehicle_type",
        "a.latest_build_on_day",
        "a.labels",
        "b.detections",
        "c.allowed_fp_intervals",
    )
)
import numpy as np


def detectionScores(detections, labels, allowed_fp_intervals):
    detection_label_max_time_diff = 60000

    tp = 0
    fp = 0
    fn = 0
    tn = 0
    fp_timestamps = []
    fn_timestamps = []
    min_diffs = []

    detections_arr = np.array([])
    if detections is not None:
        detections_arr = np.array([d for d in detections if d is not None])

    labels_arr = np.array([])
    if labels is not None:
        labels_arr = np.array([l for l in labels if l is not None])

    for d in detections_arr:
        min_diff = None
        if len(labels_arr) > 0:
            label_detection_time_diff = np.abs(labels_arr - d)
            min_diff = label_detection_time_diff.min()
            min_diffs.append(int(min_diff))

        if min_diff is not None and min_diff <= detection_label_max_time_diff:
            tp += 1
            continue

        if allowed_fp_intervals is not None:
            eng_on = False
            for interval in allowed_fp_intervals:
                if (
                    interval is None
                    or interval.start_ms is None
                    or interval.start_ms is None
                ):
                    continue

                if interval.start_ms <= d <= interval.end_ms:
                    eng_on = True
                    break
            if eng_on:
                continue

        fp += 1
        fp_timestamps.append(int(d))

    for l in labels_arr:
        min_diff = None
        if len(detections_arr) > 0:
            label_detection_time_diff = np.abs(detections_arr - l)
            min_diff = label_detection_time_diff.min()

        if min_diff is None or min_diff > detection_label_max_time_diff:
            fn += 1
            fn_timestamps.append(int(l))

    return int(tp), int(fp), int(fn), fp_timestamps, fn_timestamps


detectionScoresUdf = F.udf(
    detectionScores,
    T.StructType(
        [
            T.StructField("tp", T.LongType()),
            T.StructField("fp", T.LongType()),
            T.StructField("fn", T.LongType()),
            T.StructField("fp_timestamps", T.ArrayType(T.LongType(), False)),
            T.StructField("fn_timestamps", T.ArrayType(T.LongType(), True)),
        ]
    ),
)

# Compute precision and recall numbers for each device.
deviceScoresDf = (
    labels_detections_df.withColumn(
        "scores",
        detectionScoresUdf(
            F.col("detections"), F.col("labels"), F.col("allowed_fp_intervals")
        ),
    )
    .select(
        "org_id",
        "device_id",
        "product_id",
        "vehicle_type",
        "latest_build_on_day",
        "date",
        "detections",
        "labels",
        "scores.*",
    )
    .withColumn("precision", F.expr("tp / (tp + fp)"))
    .withColumn("recall", F.expr("tp / (tp + fn)"))
)

create_or_update_table(
    "dataprep_firmware.voltage_ignition_detection_algorithm",
    deviceScoresDf,
    "date",
    ["date", "org_id", "device_id"],
)

# COMMAND ----------
