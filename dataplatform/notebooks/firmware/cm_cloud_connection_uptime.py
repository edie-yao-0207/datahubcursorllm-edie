# Databricks notebook source
# MAGIC %run ./helpers

# COMMAND ----------

start_date, end_date = get_start_end_date(8, 0)
prev_value_lookback_days = 7

# COMMAND ----------

query_end_ms = to_ms(end_date) + day_ms() - 1

# COMMAND ----------

osddashcamconnected_df = spark.sql(
    """
   select
    date,
    time,
    org_id,
    object_id,
    named_struct("int_value", value.int_value,
    "proto_value", named_struct(
        "dashcam_connection_state", named_struct(
            "camera_id", value.proto_value.dashcam_connection_state.camera_id
        )
    ),
    "is_databreak", value.is_databreak) as value
  from kinesisstats.osddashcamconnected as a
  join dataprep_firmware.data_ingestion_high_water_mark as b
  where date >= date_sub('{}', {})
    and date <= '{}'
    and a.time <= b.time_ms
""".format(
        start_date, prev_value_lookback_days, end_date
    )
)

osdmulticamconnected_df = spark.sql(
    """
   select
    a.date,
    a.time,
    a.org_id,
    a.object_id,
    named_struct("int_value", a.value.int_value,
    "proto_value", named_struct(
        "dashcam_connection_state", named_struct(
            "camera_id", b.id
        )
    ),
    "is_databreak", value.is_databreak) as value
  from kinesisstats.osdmulticamconnected as a
  join clouddb.gateways as b
    on upper(a.value.proto_value.multicam_connection_state.multicam_serial) = b.serial
  join dataprep_firmware.data_ingestion_high_water_mark as c
  where a.date >= date_sub('{}', {})
    and a.date <= '{}'
    and a.time <= c.time_ms
""".format(
        start_date, prev_value_lookback_days, end_date
    )
)

osdpowerstate_df = spark.sql(
    """
   select *
   from kinesisstats.osdpowerstate as a
   join dataprep_firmware.data_ingestion_high_water_mark as b
   where date >= date_sub('{}', {})
    and date <= '{}'
    and a.time <= b.time_ms
""".format(
        start_date, prev_value_lookback_days, end_date
    )
)

heartbeat_df = spark.sql(
    """
   select *
   from kinesisstats.osdhubserverdeviceheartbeat as a
   join dataprep_firmware.data_ingestion_high_water_mark as b
   where date >= date_sub('{}', {})
    and date <= '{}'
    and a.time <= b.time_ms
""".format(
        start_date, prev_value_lookback_days, end_date
    )
)

gw_device_history_df = spark.sql(
    """
  select
    gateway_id,
    device_id,
    timestamp
  from clouddb.gateway_device_history
  union
  select
    id as gateway_id,
    device_id,
    associated_at as timestamp
  from clouddb.gateways
"""
)

orgs_df = spark.sql(
    """
  select *
  from clouddb.organizations
"""
)

dashcam_state_df = osddashcamconnected_df.unionAll(osdmulticamconnected_df)

dashcam_connected_to_vg_intervals_df = create_intervals(
    dashcam_state_df, lambda x: x > 0, query_end_ms
)

vg_full_power_intervals_df = create_intervals(
    osdpowerstate_df, lambda x: x == 1, query_end_ms
)

heartbeat_df = heartbeat_df.select(
    "org_id",
    F.col("object_id").alias("device_id"),
    F.col(
        "value.proto_value.hub_server_device_heartbeat.connection.started_at_ms"
    ).alias("start_ms"),
    F.col("value.proto_value.hub_server_device_heartbeat.last_heartbeat_at_ms").alias(
        "end_ms"
    ),
)

cloud_connection_intervals_overlap_df = (
    heartbeat_df.groupBy("org_id", "device_id", "start_ms")
    .agg(F.max("end_ms").alias("overlap_end_ms"))
    .select("org_id", "device_id", "start_ms", "overlap_end_ms")
)

windowSpec = Window.partitionBy("org_id", "device_id").orderBy("start_ms")
cloud_connection_intervals_lead_df = cloud_connection_intervals_overlap_df.withColumn(
    "next_start_ms", F.lead("start_ms", 1).over(windowSpec)
)
cloud_connection_intervals_df = cloud_connection_intervals_lead_df.withColumn(
    "end_ms", F.least("overlap_end_ms", "next_start_ms")
).select("org_id", "device_id", "start_ms", "end_ms")

dashcam_connected_to_vg_and_vg_connected_to_cloud_and_vg_full_power_intervals_df = (
    intersect_intervals(
        intersect_intervals(
            dashcam_connected_to_vg_intervals_df, vg_full_power_intervals_df
        ),
        cloud_connection_intervals_df,
    )
)

windowSpec = Window.partitionBy("gateway_id").orderBy("timestamp")
gw_device_history_lead_df = (
    gw_device_history_df.withColumn(
        "next_time", F.lead(1000 * F.unix_timestamp("timestamp"), 1).over(windowSpec)
    )
    .withColumn("time", 1000 * F.unix_timestamp("timestamp"))
    .select("gateway_id", "device_id", "time", "next_time")
)

dashcam_connected_to_vg_and_vg_connected_to_cloud_and_vg_full_power_intervals_by_camera_id_df = (
    dashcam_connected_to_vg_and_vg_connected_to_cloud_and_vg_full_power_intervals_df.alias(
        "a"
    )
    .join(
        gw_device_history_lead_df.alias("b"),
        (
            F.col("a.proto_value.dashcam_connection_state.camera_id")
            == F.col("b.gateway_id")
        )
        & (F.col("a.start_ms") >= F.col("b.time"))
        & (
            (F.col("b.next_time").isNull())
            | (F.col("a.start_ms") < F.col("b.next_time"))
        ),
    )
    .select("a.org_id", "b.device_id", "a.start_ms", "a.end_ms")
)

dashcam_connected_to_vg_and_vg_connected_to_cloud_and_vg_full_power_and_dashcam_connected_to_cloud_intervals_df = intersect_intervals(
    dashcam_connected_to_vg_and_vg_connected_to_cloud_and_vg_full_power_intervals_by_camera_id_df,
    cloud_connection_intervals_df,
)


dashcam_connected_to_vg_and_vg_connected_to_cloud_and_vg_full_power_intervals_daily_df = create_intervals_daily(
    dashcam_connected_to_vg_and_vg_connected_to_cloud_and_vg_full_power_intervals_by_camera_id_df,
    start_date,
    end_date,
)

dashcam_connected_to_vg_and_vg_connected_to_cloud_and_vg_full_power_and_dashcam_connected_to_cloud_intervals_daily_df = create_intervals_daily(
    dashcam_connected_to_vg_and_vg_connected_to_cloud_and_vg_full_power_and_dashcam_connected_to_cloud_intervals_df,
    start_date,
    end_date,
)

denominator_df = dashcam_connected_to_vg_and_vg_connected_to_cloud_and_vg_full_power_intervals_daily_df.groupBy(
    "date", "org_id", "device_id"
).agg(
    F.sum(F.col("end_ms") - F.col("start_ms")).alias("total_ms")
)

numerator_df = dashcam_connected_to_vg_and_vg_connected_to_cloud_and_vg_full_power_and_dashcam_connected_to_cloud_intervals_daily_df.groupBy(
    "date", "org_id", "device_id"
).agg(
    F.sum(F.col("end_ms") - F.col("start_ms")).alias("total_ms")
)

cm_device_daily_cloud_connection_df = (
    denominator_df.join(orgs_df, denominator_df.org_id == orgs_df.id)
    .join(
        numerator_df,
        (denominator_df.date == numerator_df.date)
        & (denominator_df.org_id == numerator_df.org_id)
        & (denominator_df.device_id == numerator_df.device_id),
        how="left",
    )
    .filter((denominator_df.org_id != 1) & (orgs_df.internal_type == 0))
    .select(
        denominator_df["*"],
        F.coalesce(numerator_df.total_ms).alias("connection_total_ms"),
    )
)

# COMMAND ----------

create_or_update_table(
    "dataprep_firmware.cm_device_daily_cloud_connection",
    cm_device_daily_cloud_connection_df,
    "date",
    ["date", "org_id", "device_id"],
)

# COMMAND ----------
