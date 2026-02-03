# Databricks notebook source
# MAGIC %pip install skyfield

# COMMAND ----------

# MAGIC %run ./helpers

# COMMAND ----------

start_date, end_date = get_start_end_date(8, 1)

# Query this many days before the start_date to get the previous value
# for state based object stats which don't change often
prev_value_lookback_days = 7

# COMMAND ----------

query_end_ms = to_ms(end_date) + day_ms() - 1

# COMMAND ----------

from pytz import timezone
from skyfield import almanac
from skyfield.api import N, W, wgs84, load_file, load
import boto3

region = boto3.session.Session().region_name
s3_bucket = "samsara-databricks-playground"
if region == "eu-west-1":
    s3_bucket = "samsara-eu-databricks-playground"
elif region == "ca-central-1":
    s3_bucket = "samsara-ca-databricks-playground"

# Get the phase of day (dawn, dusk, day or night) based on the location, date
# and time of the object stat. This uses the skyfield library and the de421.bsp file
# which holds the ephemeris data until 2050.
# Dusk and dawn are based on "civil" dusk and dawn which means the sun is 6 deg below the horizon.
def loc_to_phase_of_day(lat, lon, date_string, event_time):
    # if None in (lat, lon):
    #     return "unknown_location"

    # zone = timezone("UTC")
    # d = zone.localize(string_to_date(date_string))
    # midnight = d.replace(hour=0, minute=0, second=0, microsecond=0)
    # next_midnight = midnight + datetime.timedelta(days=1)

    # ts = load.timescale()
    # t0 = ts.from_datetime(midnight)
    # t1 = ts.from_datetime(next_midnight)

    # eph = load_file("/dbfs/mnt/" + s3_bucket + "/elidykaar/de421.bsp")
    # bluffton = wgs84.latlon(float(lat), float(lon))
    # f = almanac.dark_twilight_day(eph, bluffton)
    # times, events = almanac.find_discrete(t0, t1, f)

    # civil_twilight_str = "Civil twilight"
    # day_str = "Day"

    # order = ["dawn_ms", "sunrise_ms", "sunset_ms", "dusk_ms"]
    # phases_in_day = {"start_ms": to_ms(midnight)}
    # previous_e = f(t0).item()
    # for t, e in zip(times, events):
    #     ms = to_ms(t.astimezone(zone))
    #     if previous_e < e:
    #         phase = almanac.TWILIGHTS[e]
    #         if phase == civil_twilight_str:
    #             phases_in_day["dawn_ms"] = ms
    #         elif phase == day_str:
    #             phases_in_day["sunrise_ms"] = ms
    #     else:
    #         phase = almanac.TWILIGHTS[previous_e]
    #         if phase == day_str:
    #             phases_in_day["sunset_ms"] = ms
    #         elif phase == civil_twilight_str:
    #             phases_in_day["dusk_ms"] = ms
    #     previous_e = e

    # closest_before_ms = day_ms()
    # closest_before_phase = ""
    # closest_after_ms = day_ms()
    # closest_after_phase = ""
    # for k in phases_in_day:
    #     if (
    #         event_time >= phases_in_day[k]
    #         and event_time - phases_in_day[k] < closest_before_ms
    #     ):
    #         closest_before_ms = event_time - phases_in_day[k]
    #         closest_before_phase = k

    #     if (
    #         event_time < phases_in_day[k]
    #         and phases_in_day[k] - event_time < closest_after_ms
    #     ):
    #         closest_after_ms = phases_in_day[k] - event_time
    #         closest_after_phase = k

    # if closest_before_phase == "start_ms":
    #     if closest_after_phase != "end_ms":
    #         closest_before_phase = order[
    #             order.index(closest_after_phase) - 1 % len(order)
    #         ]
    #     else:
    #         # this can happen near the poles since there might not be a phase change
    #         # anytime during a 24 hour window
    #         closest_before_phase = "no_phase_change"

    # if closest_before_phase == "dawn_ms":
    #     return "dawn_dusk"
    # elif closest_before_phase == "sunrise_ms":
    #     return "day"
    # elif closest_before_phase == "sunset_ms":
    #     return "dawn_dusk"
    # elif closest_before_phase == "dusk_ms":
    #     return "night"
    # else:
    #     return "unknown_phase"

    # Stop computing the time of day because it was too slow and costing too much money and
    # just return "all" to represent all times of day.
    return "all"


# Compute the phase of day for stat_df by looking at the most recent trip start or end location
def add_day_phase_column(stat_df):
    vg_locations_df = spark.sql(
        """
    (select
      date,
      org_id,
      device_id,
      proto.start.time,
      proto.start.longitude,
      proto.start.latitude
    from trips2db_shards.trips
    where date >= date_sub('{}', {})
      and date <= '{}'
      and version = 101
    ) union
    (select
      date,
      org_id,
      device_id,
      proto.end.time,
      proto.end.longitude,
      proto.end.latitude
    from trips2db_shards.trips
    where date >= date_sub('{}', {})
      and date <= '{}'
      and version = 101
    )
    """.format(
            start_date,
            prev_value_lookback_days,
            end_date,
            start_date,
            prev_value_lookback_days,
            end_date,
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
    """.format(
            start_date, prev_value_lookback_days, end_date
        )
    )

    cm_locations_df = vg_locations_df.join(
        vg_cm_assoc_df,
        (vg_locations_df.date == vg_cm_assoc_df.date)
        & (vg_locations_df.org_id == vg_cm_assoc_df.org_id)
        & (vg_locations_df.device_id == vg_cm_assoc_df.device_id),
    ).select(
        vg_locations_df.date,
        vg_locations_df.org_id,
        vg_cm_assoc_df.cm_device_id.alias("device_id"),
        vg_locations_df.time,
        vg_locations_df.latitude,
        vg_locations_df.longitude,
    )

    # create temp views so we can use the min_by func which is only available in sql
    cm_locations_df.createOrReplaceTempView("cm_locations")
    stat_df.createOrReplaceTempView("stat")

    stat_with_location_df = spark.sql(
        """
    select
      a.date,
      a.time,
      a.org_id,
      a.device_id,
      min_by(b.latitude, abs(a.time - b.time)) as latitude,
      min_by(b.longitude, abs(a.time - b.time)) as longitude,
      a.value
    from stat as a
    left join cm_locations as b
      on a.device_id = b.device_id
    group by
      a.date,
      a.org_id,
      a.device_id,
      a.time,
      a.value
  """
    )

    loc_to_phase_of_day_udf = F.udf(loc_to_phase_of_day, T.StringType())
    stat_with_location_day_phase_df = stat_with_location_df.withColumn(
        "day_phase",
        loc_to_phase_of_day_udf(
            F.col("latitude"), F.col("longitude"), F.col("date"), F.col("time")
        ),
    )

    return stat_with_location_day_phase_df


def days_enabled_for_full_day(stat_df):
    stat_intervals_daily_df = create_intervals_daily(
        create_intervals(stat_df, lambda x: x > 0, query_end_ms),
        start_date,
        end_date,
    )
    stat_full_days_df = stat_intervals_daily_df.filter(
        F.col("end_ms") - F.col("start_ms") == day_ms() - 1
    ).select("date", "org_id", "device_id")
    return stat_full_days_df


# COMMAND ----------

# Create table dataprep_firmware.qr_code_debug

qr_code_debug_df = spark.sql(
    """
 select
  date,
  time,
  org_id,
  object_id as device_id,
  value
from kinesisstats.osdqrcodescandebug
where date >= '{}'
  and date <= '{}'
""".format(
        start_date, end_date
    )
)

qr_code_with_location_day_phase_df = add_day_phase_column(qr_code_debug_df)
create_or_update_table(
    "dataprep_firmware.qr_code_debug",
    qr_code_with_location_day_phase_df,
    "date",
    ["date", "org_id", "device_id", "day_phase", "time"],
)

# COMMAND ----------

# Create table dataprep_firmware.device_daily_qr_code_enabled

qr_code_enabled_cfg_df = spark.sql(
    """
   select
      date,
      time,
      org_id,
      object_id,
      named_struct("int_value",
      case
        when first(s3_proto_value.reported_device_config.device_config.q_r_code_reader.enabled) = 1 then 1
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

create_or_update_table(
    "dataprep_firmware.device_daily_qr_code_enabled",
    days_enabled_for_full_day(qr_code_enabled_cfg_df),
    "date",
    ["date", "org_id", "device_id"],
)

# COMMAND ----------

# Create table dataprep_firmware.device_daily_sign_in_alerting_enabled

nfc_card_reader_attached_df = spark.sql(
    """
 select
    a.date,
    a.time,
    a.org_id,
    b.cm_device_id as object_id,
    named_struct("int_value",
    case
      when exists(a.value.proto_value.attached_usb_devices.usb_id, x -> x == 4294901813) is null then 0
      else 1
    end,
    "proto_value", null,
    "is_databreak", a.value.is_databreak) as value
  from kinesisstats.osdattachedusbdevices as a
  join dataprep_safety.cm_device_health_daily as b
    on a.object_id = b.device_id
    and a.org_id = b.org_id
    and a.date = b.date
  where a.date >= date_sub('{}', {})
    and a.date <= '{}'
""".format(
        start_date, prev_value_lookback_days, end_date
    )
)
nfc_card_reader_attached_full_days_df = days_enabled_for_full_day(
    nfc_card_reader_attached_df
)

old_fw_sign_in_enabled_df = spark.sql(
    """
 select
    date,
    time,
    org_id,
    object_id,
    named_struct("int_value",
    case
      when first(s3_proto_value.reported_device_config.device_config.driver_sign_in_alerts.enabled) = 1 then 1
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
old_fw_sign_in_enabled_full_days_df = days_enabled_for_full_day(
    old_fw_sign_in_enabled_df
)

nfc_sign_in_enabled_df = spark.sql(
    """
 select
    date,
    time,
    org_id,
    object_id,
    named_struct("int_value",
    case
      when first(s3_proto_value.reported_device_config.device_config.driver_sign_in_alerts.enabled_for_n_f_c_card) = 1 then 1
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
nfc_sign_in_enabled_full_days_df = days_enabled_for_full_day(nfc_sign_in_enabled_df)

qr_code_sign_in_enabled_df = spark.sql(
    """
 select
    date,
    time,
    org_id,
    object_id,
    named_struct("int_value",
    case
      when first(s3_proto_value.reported_device_config.device_config.driver_sign_in_alerts.enabled_for_q_r_code) = 1 then 1
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
qr_code_sign_in_enabled_full_days_df = days_enabled_for_full_day(
    qr_code_sign_in_enabled_df
)

cm32_df = spark.sql(
    """
  select
    date,
    org_id,
    cm_device_id as device_id
  from dataprep_safety.cm_device_health_daily
  where date >= '{}'
    and date <= '{}'
    and cm_product_id in (43, 155)
""".format(
        start_date, end_date
    )
)

nfc_sign_in_df = (
    nfc_card_reader_attached_full_days_df.alias("a")
    .join(
        nfc_sign_in_enabled_full_days_df.alias("b"),
        (F.col("a.date") == F.col("b.date"))
        & (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.device_id")),
    )
    .select("a.date", "a.org_id", "a.device_id")
)


qr_code_sign_in_df = (
    cm32_df.alias("a")
    .join(
        qr_code_sign_in_enabled_full_days_df.alias("b"),
        (F.col("a.date") == F.col("b.date"))
        & (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.device_id")),
    )
    .select("a.date", "a.org_id", "a.device_id")
)

sign_in_xxx_alerts_enabled_df = (
    old_fw_sign_in_enabled_full_days_df.unionAll(nfc_sign_in_df)
    .distinct()
    .unionAll(qr_code_sign_in_df)
    .distinct()
)
create_or_update_table(
    "dataprep_firmware.device_daily_sign_in_alerting_enabled",
    sign_in_xxx_alerts_enabled_df,
    "date",
    ["date", "org_id", "device_id"],
)

# COMMAND ----------

# Create table dataprep_firmware.qr_code_scan_uptime

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

osdqrcodescanningstate_df = spark.sql(
    """
select
  date,
  time,
  org_id,
  object_id,
  named_struct("int_value",
  case
    when value.proto_value.q_r_code_scanning_state.state = 2 then 1
    else 0
  end,
  "proto_value", null,
  "is_databreak", value.is_databreak) as value
from kinesisstats.osdqrcodescanningstate
where date >= date_sub('{}', {})
  and date <= '{}'
""".format(
        start_date, prev_value_lookback_days, end_date
    )
)

power_state_intervals_df = create_intervals(
    osdpowerstate_df, lambda x: x == 1, query_end_ms
)
scanning_state_intervals_df = create_intervals(
    osdqrcodescanningstate_df, lambda x: x == 1, query_end_ms
)

scanning_while_powered_intervals = intersect_intervals(
    scanning_state_intervals_df, power_state_intervals_df
)

daily_power_state_intervals_df = create_intervals_daily(
    power_state_intervals_df, start_date, end_date
)
daily_scanning_while_powered_intervals_df = create_intervals_daily(
    scanning_while_powered_intervals, start_date, end_date
)

daily_power_state_intervals_agg_df = daily_power_state_intervals_df.groupBy(
    "date", "org_id", "device_id"
).agg(F.sum(F.col("end_ms") - F.col("start_ms")).alias("total_ms"))
daily_scanning_while_powered_intervals_agg_df = (
    daily_scanning_while_powered_intervals_df.groupBy(
        "date", "org_id", "device_id"
    ).agg(F.sum(F.col("end_ms") - F.col("start_ms")).alias("total_ms"))
)

device_daily_qr_code_enabled_df = spark.sql(
    "select * from dataprep_firmware.device_daily_qr_code_enabled"
)

qr_code_scan_df = (
    daily_power_state_intervals_agg_df.alias("a")
    .join(
        device_daily_qr_code_enabled_df.alias("b"),
        (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.device_id"))
        & (F.col("a.date") == F.col("b.date")),
    )
    .join(
        daily_scanning_while_powered_intervals_agg_df.alias("c"),
        (F.col("a.org_id") == F.col("c.org_id"))
        & (F.col("a.device_id") == F.col("c.device_id"))
        & (F.col("a.date") == F.col("c.date")),
        how="left",
    )
    .select(
        "a.date",
        "a.org_id",
        "a.device_id",
        F.col("a.total_ms").alias("powered_ms"),
        F.coalesce(F.col("c.total_ms"), F.lit(0)).alias("scan_ms"),
    )
)

create_or_update_table(
    "dataprep_firmware.qr_code_scan_uptime",
    qr_code_scan_df,
    "date",
    ["date", "org_id", "device_id"],
)

# COMMAND ----------

# Create table dataprep_firmware.qr_code_scan_success_count

successful_scans_df = spark.sql(
    """
  select
      date,
      time,
      org_id,
      object_id as device_id,
      value
  from kinesisstats.osdqrcodescan
  where date >= '{}'
    and date <= '{}'
""".format(
        start_date, end_date
    )
)

successful_scans_with_location_day_phase_df = (
    add_day_phase_column(successful_scans_df)
    .groupBy("date", "day_phase", "org_id", "device_id")
    .agg(F.count("*").alias("count"))
)
create_or_update_table(
    "dataprep_firmware.qr_code_scan_success_count",
    successful_scans_with_location_day_phase_df,
    "date",
    ["date", "org_id", "device_id", "day_phase"],
)

# COMMAND ----------

# Create table dataprep_firmware.qr_code_trip_count

trip_starts_df = spark.sql(
    """
  select
    b.date,
    a.org_id,
    b.cm_device_id as device_id,
    a.proto.start.time,
    a.proto.start.longitude,
    a.proto.start.latitude
  from trips2db_shards.trips as a
  join dataprep_safety.cm_device_health_daily as b
    on a.device_id = b.device_id
    and a.org_id = b.org_id
    and a.date = b.date
  join dataprep_firmware.device_daily_qr_code_enabled as c
    on b.cm_device_id = c.device_id
    and a.org_id = c.org_id
    and a.date = c.date
  where a.date >= '{}'
    and a.date <= '{}'
    and a.version = 101
  """.format(
        start_date, end_date
    )
)

loc_to_phase_of_day_udf = F.udf(loc_to_phase_of_day, T.StringType())
trip_starts_day_phase_df = (
    trip_starts_df.withColumn(
        "day_phase",
        loc_to_phase_of_day_udf(
            F.col("latitude"), F.col("longitude"), F.col("date"), F.col("time")
        ),
    )
    .groupBy("date", "day_phase", "org_id", "device_id")
    .agg(F.count("*").alias("count"))
)

create_or_update_table(
    "dataprep_firmware.qr_code_trip_count",
    trip_starts_day_phase_df,
    "date",
    ["date", "org_id", "device_id", "day_phase"],
)

# COMMAND ----------

# Create table dataprep_firmware.sign_in_alerting_trip_count

trips_df = spark.sql(
    """
  select
    b.date,
    a.org_id,
    b.cm_device_id as device_id,
    a.proto.start.time
  from trips2db_shards.trips as a
  join dataprep_safety.cm_device_health_daily as b
    on a.device_id = b.device_id
    and a.org_id = b.org_id
    and a.date = b.date
  join dataprep_firmware.device_daily_sign_in_alerting_enabled as c
    on b.cm_device_id = c.device_id
    and a.org_id = c.org_id
    and a.date = c.date
  where a.date >= '{}'
    and a.date <= '{}'
    and a.version = 101
  """.format(
        start_date, end_date
    )
)

trips_agg_df = trips_df.groupBy("date", "org_id", "device_id").agg(
    F.count("*").alias("count")
)
create_or_update_table(
    "dataprep_firmware.sign_in_alerting_trip_count",
    trips_agg_df,
    "date",
    ["date", "org_id", "device_id"],
)

# COMMAND ----------

# Create table dataprep_firmware.qr_code_scan_alert_count

scan_alerts_df = spark.sql(
    """
	select
		date,
        time,
		org_id,
		object_id as device_id,
        value
	from kinesisstats.osdcm3xaudioalertinfo
    where date >= '{}'
      and date <= '{}'
      and value.proto_value.cm3x_audio_alert_info.event_type = 33
""".format(
        start_date, end_date
    )
)

scan_alerts_with_location_day_phase_df = (
    add_day_phase_column(scan_alerts_df)
    .groupBy("date", "day_phase", "org_id", "device_id")
    .agg(F.count("*").alias("count"))
)
create_or_update_table(
    "dataprep_firmware.qr_code_scan_alert_count",
    scan_alerts_with_location_day_phase_df,
    "date",
    ["date", "org_id", "device_id", "day_phase"],
)

# COMMAND ----------

# Create table dataprep_firmware.qr_code_scan_latency

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

osdqrcodescanningstate_df = spark.sql(
    """
select
  date,
  time,
  org_id,
  object_id,
  named_struct("int_value",
  case
    when value.proto_value.q_r_code_scanning_state.state = 2 then 1
    else 0
  end,
  "proto_value", null,
  "is_databreak", value.is_databreak) as value
from kinesisstats.osdqrcodescanningstate
where date >= date_sub('{}', {})
  and date <= '{}'
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
  """.format(
        start_date, prev_value_lookback_days, end_date
    )
)

qr_code_enabled_cfg_df = spark.sql(
    """
   select
      date,
      time,
      org_id,
      object_id,
      named_struct("int_value",
      case
        when first(s3_proto_value.reported_device_config.device_config.q_r_code_reader.enabled) = 1 then 1
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

enabled_intervals_df = create_intervals(
    qr_code_enabled_cfg_df, lambda x: x == 1, query_end_ms
)
power_state_intervals_df = create_intervals(
    osdpowerstate_df, lambda x: x == 1, query_end_ms
)
power_state_or_enabled_intervals_df = power_state_intervals_df.union(
    enabled_intervals_df
)


scanning_state_intervals_df = create_intervals(
    osdqrcodescanningstate_df, lambda x: x == 1, query_end_ms
).withColumn(
    "date", F.to_date(F.from_unixtime((F.col("start_ms") / 1000).cast(T.IntegerType())))
)


latency_df = (
    scanning_state_intervals_df.alias("a")
    .join(
        vg_cm_assoc_df.alias("b"),
        (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.cm_device_id"))
        & (F.col("a.date") == F.col("b.date")),
    )
    .join(
        power_state_or_enabled_intervals_df.alias("c"),
        (F.col("a.org_id") == F.col("c.org_id"))
        & (F.col("b.device_id") == F.col("c.device_id"))
        & (F.col("a.start_ms") >= F.col("c.start_ms"))
        & (F.col("a.start_ms") < F.col("c.end_ms")),
    )
    .groupBy("a.org_id", "a.device_id", "c.start_ms")
    .agg(F.min(F.col("a.start_ms") - F.col("c.start_ms")).alias("latency_ms"))
    .withColumn(
        "date",
        F.to_date(F.from_unixtime((F.col("c.start_ms") / 1000).cast(T.IntegerType()))),
    )
)

create_or_update_table(
    "dataprep_firmware.qr_code_scan_latency",
    latency_df,
    "date",
    ["date", "org_id", "device_id", "start_ms"],
)

# COMMAND ----------

# Create table dataprep_firmware.qr_code_ir_led_disabled

ir_led_df = spark.sql(
    """
	select
		a.date,
        a.time,
		a.org_id,
		a.object_id as device_id,
        a.value
	from kinesisstats.osddriverirledenabled as a
    join dataprep_firmware.device_daily_qr_code_enabled as b
      on a.date = b.date
      and a.org_id = b.org_id
      and a.object_id = b.device_id
    where a.date >= '{}'
      and a.date <= '{}'
""".format(
        start_date, end_date
    )
)

ir_led_df = ir_led_df.withColumn(
    "prev_value",
    F.lag(F.col("value"), 1).over(
        Window.partitionBy("org_id", "device_id").orderBy("time")
    ),
)
ir_led_disabled_df = ir_led_df.filter(
    (F.col("prev_value.proto_value.driver_i_r_led_state.enabled") == True)
    & (F.col("value.proto_value.driver_i_r_led_state.enabled").isNull())
    & (F.col("value.is_databreak") == False)
    & (F.col("value.is_end") == False)
    & (F.col("value.proto_value.driver_i_r_led_state.source") == 2)
)

ir_led_disabled_with_day_phase_df = (
    add_day_phase_column(ir_led_disabled_df)
    .groupBy("date", "day_phase", "org_id", "device_id")
    .agg(F.count("*").alias("count"))
)
create_or_update_table(
    "dataprep_firmware.qr_code_ir_led_disabled",
    ir_led_disabled_with_day_phase_df,
    "date",
    ["date", "org_id", "device_id", "day_phase"],
)

# COMMAND ----------

# Create table dataprep_firmware.qr_code_ir_led_disabled_on_trip

ir_led_on_trip_df = spark.sql(
    """
	select
		a.date,
        a.time,
		a.org_id,
		a.object_id as device_id,
        a.value
	from kinesisstats.osddriverirledenabled as a
    join dataprep_safety.cm_device_health_daily as b
      on a.org_id = b.org_id
      and a.object_id = b.cm_device_id
      and a.date = b.date
    join trips2db_shards.trips as c
      on a.org_id = c.org_id
      and b.device_id = c.device_id
      and a.time >= proto.start.time
      and a.time < proto.end.time
    join dataprep_firmware.device_daily_qr_code_enabled as d
      on a.date = d.date
      and a.org_id = d.org_id
      and a.object_id = d.device_id
    where a.date >= '{}'
      and a.date <= '{}'
      and c.version = 101
""".format(
        start_date, end_date
    )
)

ir_led_on_trip_df = ir_led_on_trip_df.withColumn(
    "prev_value",
    F.lag(F.col("value"), 1).over(
        Window.partitionBy("org_id", "device_id").orderBy("time")
    ),
)
ir_led_disabled_on_trip_df = ir_led_on_trip_df.filter(
    (F.col("prev_value.proto_value.driver_i_r_led_state.enabled") == True)
    & (F.col("value.proto_value.driver_i_r_led_state.enabled").isNull())
    & (F.col("value.is_databreak") == False)
    & (F.col("value.is_end") == False)
    & (F.col("value.proto_value.driver_i_r_led_state.source") == 2)
)

ir_led_disabled_on_trip_with_day_phase_df = (
    add_day_phase_column(ir_led_disabled_on_trip_df)
    .groupBy("date", "day_phase", "org_id", "device_id")
    .agg(F.count("*").alias("count"))
)
create_or_update_table(
    "dataprep_firmware.qr_code_ir_led_disabled_on_trip",
    ir_led_disabled_on_trip_with_day_phase_df,
    "date",
    ["date", "org_id", "device_id", "day_phase"],
)

# COMMAND ----------

# create table dataprep_firmware.qr_code_sign_in_reminder_alert_latency

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

sign_in_alert_df = spark.sql(
    """
  select
    a.date,
    a.time,
    a.org_id,
    a.object_id as device_id,
    first(b.device_id) as vg_device_id -- remove duplicate rows for the same CM
  from kinesisstats.osdcm3xaudioalertinfo as a
  join dataprep_safety.cm_device_health_daily as b
      on a.object_id = b.cm_device_id
      and a.org_id = b.org_id
      and a.date = b.date
  where a.date >= '{}'
    and a.date <= '{}'
    and a.value.proto_value.cm3x_audio_alert_info.event_type = 34
  group by
    a.date,
    a.time,
    a.org_id,
    a.object_id
""".format(
        start_date, end_date
    )
)

trip_starts_df = spark.sql(
    """
  select
    b.date,
    a.org_id,
    b.cm_device_id as device_id,
    a.proto.start.time
  from trips2db_shards.trips as a
  join dataprep_safety.cm_device_health_daily as b
    on a.device_id = b.device_id
    and a.org_id = b.org_id
    and a.date = b.date
  where a.date >= '{}'
    and a.date <= '{}'
    and a.version = 101
  """.format(
        start_date, end_date
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
  """.format(
        start_date, prev_value_lookback_days, end_date
    )
)

# Get power state intervals with brief wake filtered out.
power_state_intervals_df = create_intervals(
    osdpowerstate_df, lambda x: x == 1, query_end_ms
).filter("value.proto_value.power_state_info.change_reason != 1")


latency_df = (
    power_state_intervals_df.alias("a")
    .join(
        sign_in_alert_df.alias("b"),
        (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.vg_device_id"))
        & (F.col("b.time") >= F.col("a.start_ms"))
        & (F.col("b.time") <= F.col("a.end_ms")),
    )
    .groupBy("a.org_id", "b.device_id", "a.start_ms")
    .agg(F.min(F.col("b.time") - F.col("a.start_ms")).alias("latency_ms"))
    .withColumn("sign_in_alert_ms", F.col("start_ms") + F.col("latency_ms"))
)

latency_filtered_df = (
    latency_df.alias("a")
    .join(
        trip_starts_df.alias("b"),
        (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.device_id"))
        & (F.col("a.start_ms") <= F.col("b.time"))
        & (F.col("a.sign_in_alert_ms") > F.col("b.time")),
        how="left",
    )
    .filter("b.time is null")
    .select(
        F.to_date(
            F.from_unixtime((F.col("a.sign_in_alert_ms") / 1000).cast(T.IntegerType()))
        ).alias("date"),
        "a.org_id",
        "a.device_id",
        "a.sign_in_alert_ms",
        "a.latency_ms",
    )
)

create_or_update_table(
    "dataprep_firmware.sign_in_reminder_alert_latency",
    latency_filtered_df,
    "date",
    ["date", "org_id", "device_id", "sign_in_alert_ms"],
)

# COMMAND ----------

# Create table dataprep_firmware.sign_in_reminder_alert_count

alert_df = (
    spark.sql(
        """
	select
		date,
        time,
		org_id,
		object_id as device_id
	from kinesisstats.osdcm3xaudioalertinfo
    where date >= '{}'
      and date <= '{}'
      and value.proto_value.cm3x_audio_alert_info.event_type = 34
""".format(
            start_date, end_date
        )
    )
    .groupBy("date", "org_id", "device_id")
    .agg(F.count("*").alias("count"))
)

create_or_update_table(
    "dataprep_firmware.sign_in_reminder_alert_count",
    alert_df,
    "date",
    ["date", "org_id", "device_id"],
)

# COMMAND ----------

# Create table dataprep_firmware.qr_code_sign_in_success_alert_count

alert_success_df = (
    spark.sql(
        """
	select
		date,
        time,
		org_id,
		object_id as device_id
	from kinesisstats.osdcm3xaudioalertinfo
    where date >= '{}'
      and date <= '{}'
      and value.proto_value.cm3x_audio_alert_info.event_type = 35
""".format(
            start_date, end_date
        )
    )
    .groupBy("date", "org_id", "device_id")
    .agg(F.count("*").alias("count"))
)

create_or_update_table(
    "dataprep_firmware.sign_in_success_alert_count",
    alert_success_df,
    "date",
    ["date", "org_id", "device_id"],
)

# COMMAND ----------

# create table dataprep_firmware.sign_in_no_reminder_alert_count and dataprep_firmware.sign_in_power_states_with_trip

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

sign_in_alert_df = spark.sql(
    """
  select
    date,
    time,
    org_id,
    object_id as device_id
  from kinesisstats.osdcm3xaudioalertinfo
  where date >= '{}'
    and date <= '{}'
    and value.proto_value.cm3x_audio_alert_info.event_type = 34
""".format(
        start_date, end_date
    )
)

trip_starts_df = spark.sql(
    """
  select
    date,
    org_id,
    device_id as vg_device_id,
    proto.start.time
  from trips2db_shards.trips
  where date >= '{}'
    and date <= '{}'
    and version = 101
  """.format(
        start_date, end_date
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
  """.format(
        start_date, prev_value_lookback_days, end_date
    )
)

power_state_intervals_df = create_intervals(
    osdpowerstate_df, lambda x: x == 1, query_end_ms
).withColumn(
    "date", F.to_date(F.from_unixtime((F.col("start_ms") / 1000).cast(T.IntegerType())))
)

enabled_df = spark.sql(
    "select * from dataprep_firmware.device_daily_sign_in_alerting_enabled"
)

trip_after_power_state_df = (
    power_state_intervals_df.alias("a")
    .join(
        vg_cm_assoc_df.alias("b"),
        (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.device_id"))
        & (F.col("a.date") == F.col("b.date")),
    )
    .join(
        enabled_df.alias("c"),
        (F.col("a.org_id") == F.col("c.org_id"))
        & (F.col("b.cm_device_id") == F.col("c.device_id"))
        & (F.col("a.date") == F.col("c.date")),
    )
    .join(
        trip_starts_df.alias("d"),
        (F.col("a.org_id") == F.col("d.org_id"))
        & (F.col("b.device_id") == F.col("d.vg_device_id"))
        & (F.col("d.time") >= F.col("a.start_ms"))
        & (F.col("d.time") < F.col("a.end_ms")),
    )
    .groupBy(
        "a.org_id", F.col("b.cm_device_id").alias("device_id"), "a.start_ms", "a.date"
    )
    .agg(F.min(F.col("d.time") - F.col("a.start_ms")).alias("latency_ms"))
)

trip_after_power_state_agg_df = trip_after_power_state_df.groupBy(
    "date", "org_id", "device_id"
).agg(F.count("*").alias("count"))
create_or_update_table(
    "dataprep_firmware.sign_in_power_states_with_trip",
    trip_after_power_state_agg_df,
    "date",
    ["date", "org_id", "device_id"],
)


no_sign_in_reminders_df = (
    trip_after_power_state_df.alias("a")
    .join(
        sign_in_alert_df.alias("b"),
        (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.device_id"))
        & (F.col("a.start_ms") <= F.col("b.time"))
        & (F.col("a.start_ms") + F.col("a.latency_ms") > F.col("b.time")),
        how="left",
    )
    .filter("b.time is null")
    .select("a.date", "a.org_id", "a.device_id", "a.start_ms")
    .groupBy("date", "org_id", "device_id")
    .agg(F.count("*").alias("count"))
)

create_or_update_table(
    "dataprep_firmware.sign_in_no_reminder_alert_count",
    no_sign_in_reminders_df,
    "date",
    ["date", "org_id", "device_id"],
)

# COMMAND ----------

# Create table dataprep_firmware.sign_in_uptime

sign_in_alert_df = spark.sql(
    """
  select *
  from kinesisstats.osddriversigninstate
  where date >= date_sub('{}', {})
    and date <= '{}'
""".format(
        start_date, prev_value_lookback_days, end_date
    )
)

trip_intervals_df = spark.sql(
    """
  select
    date,
    org_id,
    device_id,
    proto.start.time as start_ms,
    proto.end.time as end_ms
  from trips2db_shards.trips
  where date >= date_sub('{}', {})
    and date <= '{}'
    and version = 101
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
  """.format(
        start_date, prev_value_lookback_days, end_date
    )
)

enabled_df = spark.sql(
    "select * from dataprep_firmware.device_daily_sign_in_alerting_enabled"
)

relevent_trip_intervals_df = (
    trip_intervals_df.alias("a")
    .join(
        vg_cm_assoc_df.alias("b"),
        (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.device_id"))
        & (F.col("a.date") == F.col("b.date")),
    )
    .join(
        enabled_df.alias("c"),
        (F.col("a.org_id") == F.col("c.org_id"))
        & (F.col("b.cm_device_id") == F.col("c.device_id"))
        & (F.col("a.date") == F.col("b.date")),
    )
    .select(
        "a.org_id", F.col("b.cm_device_id").alias("device_id"), "a.start_ms", "a.end_ms"
    )
)


trip_daily_agg_df = (
    create_intervals_daily(relevent_trip_intervals_df, start_date, end_date)
    .groupBy("date", "org_id", "device_id")
    .agg(F.sum(F.col("end_ms") - F.col("start_ms")).alias("trip_ms"))
)

sign_in_daily_agg_df = (
    create_intervals_daily(
        intersect_intervals(
            create_intervals(sign_in_alert_df, lambda x: x == 1, query_end_ms),
            relevent_trip_intervals_df,
        ),
        start_date,
        end_date,
    )
    .groupBy("date", "org_id", "device_id")
    .agg(F.sum(F.col("end_ms") - F.col("start_ms")).alias("signed_in_ms"))
)

joined_df = (
    trip_daily_agg_df.alias("a")
    .join(
        sign_in_daily_agg_df.alias("b"),
        (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.device_id"))
        & (F.col("a.date") == F.col("b.date")),
        how="left",
    )
    .select("a.date", "a.org_id", "a.device_id", "a.trip_ms", "b.signed_in_ms")
)


create_or_update_table(
    "dataprep_firmware.sign_in_uptime",
    joined_df,
    "date",
    ["date", "org_id", "device_id", "trip_ms"],
)

# COMMAND ----------

# Create table dataprep_firmware.qr_code_scan_motions

qr_code_debug_df = spark.sql(
    """
 select
  date,
  time,
  org_id,
  object_id as device_id,
  value
from kinesisstats.osdqrcodescandebug
where date >= '{}'
  and date <= '{}'
""".format(
        start_date, end_date
    )
)


def scan_motion_intervals(events):
    if events is None or len(events) == 0:
        return None

    last_time = None
    interval_started = False
    intervals = []
    for i, e in enumerate(events):
        if not interval_started:
            interval_started = True
            interval_start = e.time
            last_time = None

        if interval_started:
            if e.scan_status >= 3 and last_time is None:
                intervals.append((interval_start, e.time, True))
                interval_started = False
            elif e.scan_status >= 3 and e.time - last_time <= 60000:
                interval_started = False
                intervals.append((interval_start, e.time, True))
            elif (
                last_time is not None
                and e.time - last_time > 60000
                and i == len(events) - 1
            ):
                intervals.append((interval_start, last_time, False))
                intervals.append((e.time, e.time, None))
            elif last_time is not None and e.time - last_time > 60000:
                intervals.append((interval_start, last_time, False))
                interval_start = e.time
                last_time = None
            elif i == len(events) - 1:
                intervals.append((interval_start, e.time, None))

        last_time = e.time

    return intervals


scan_motion_intervals_udf = F.udf(
    scan_motion_intervals,
    T.ArrayType(
        T.StructType(
            [
                T.StructField("start_ms", T.LongType()),
                T.StructField("end_ms", T.LongType()),
                T.StructField("success", T.BooleanType()),
            ]
        )
    ),
)

scans_grouped_df = qr_code_debug_df.groupBy("org_id", "device_id").agg(
    F.array_sort(
        F.collect_list(
            F.struct(
                F.col("time"),
                F.col("value.proto_value.q_r_code_scan_debug.scan_status"),
            )
        )
    ).alias("events")
)

motion_intervals_df = (
    scans_grouped_df.withColumn("motions", scan_motion_intervals_udf(F.col("events")))
    .withColumn("motion", F.explode("motions"))
    .select(
        F.to_date(
            F.from_unixtime((F.col("motion.start_ms") / 1000).cast(T.IntegerType()))
        )
        .cast(T.StringType())
        .alias("date"),
        "org_id",
        "device_id",
        F.col("motion.start_ms").alias("time"),
        F.struct(
            (F.col("motion.end_ms") - F.col("motion.start_ms")).alias("duration_ms"),
            "motion.success",
        ).alias("value"),
    )
)

motion_intervals_with_location_day_phase_df = add_day_phase_column(
    motion_intervals_df
).select(
    "date",
    "time",
    "org_id",
    "device_id",
    "value.duration_ms",
    "value.success",
    "day_phase",
)
create_or_update_table(
    "dataprep_firmware.qr_code_scan_motions",
    motion_intervals_with_location_day_phase_df,
    "date",
    ["date", "time", "org_id", "device_id", "day_phase"],
)

# COMMAND ----------

# Create table dataprep_firmware.qr_code_scan_attempt_latency

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

scan_attempt_df = spark.sql(
    """
select
  date,
  time,
  org_id,
  object_id as device_id
from kinesisstats.osdqrcodescandebug
where date >= '{}'
  and date <= '{}'
""".format(
        start_date, end_date
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
  """.format(
        start_date, prev_value_lookback_days, end_date
    )
)

qr_code_enabled_cfg_df = spark.sql(
    """
   select
      date,
      time,
      org_id,
      object_id,
      named_struct("int_value",
      case
        when first(s3_proto_value.reported_device_config.device_config.q_r_code_reader.enabled) = 1 then 1
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

enabled_intervals_df = create_intervals(
    qr_code_enabled_cfg_df, lambda x: x == 1, query_end_ms
)
power_state_intervals_df = create_intervals(
    osdpowerstate_df, lambda x: x == 1, query_end_ms
)
power_state_or_enabled_intervals_df = power_state_intervals_df.union(
    enabled_intervals_df
)

power_on_df = (
    osdpowerstate_df.select(
        "date",
        "time",
        "org_id",
        F.col("object_id").alias("device_id"),
        F.col("value.int_value"),
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

latency_df = (
    scan_attempt_df.alias("a")
    .join(
        vg_cm_assoc_df.alias("b"),
        (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.cm_device_id"))
        & (F.col("a.date") == F.col("b.date")),
    )
    .join(
        power_state_or_enabled_intervals_df.alias("c"),
        (F.col("a.org_id") == F.col("c.org_id"))
        & (F.col("b.device_id") == F.col("c.device_id"))
        & (F.col("a.time") >= F.col("c.start_ms"))
        & (F.col("a.time") < F.col("c.end_ms")),
    )
    .groupBy("a.org_id", "a.device_id", "c.start_ms")
    .agg(F.min(F.col("a.time") - F.col("c.start_ms")).alias("latency_ms"))
    .withColumn(
        "date",
        F.to_date(
            F.from_unixtime((F.col("c.start_ms") / 1000).cast(T.IntegerType()))
        ).cast(T.StringType()),
    )
    .select(
        "date", "org_id", "device_id", F.col("start_ms").alias("time"), "latency_ms"
    )
    .withColumn("value", F.struct("latency_ms"))
    .drop("latency_ms")
)

latency_with_location_day_phase_df = add_day_phase_column(latency_df).select(
    "date", "time", "org_id", "device_id", "value.latency_ms", "day_phase"
)

create_or_update_table(
    "dataprep_firmware.qr_code_scan_attempt_latency",
    latency_with_location_day_phase_df,
    "date",
    ["date", "time", "org_id", "device_id", "day_phase"],
)

# COMMAND ----------

# Create table dataprep_firmware.qr_code_scan_success_latency

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

scan_success_df = spark.sql(
    """
select
  date,
  time,
  org_id,
  object_id as device_id
from kinesisstats.osdqrcodescan
where date >= '{}'
  and date <= '{}'
""".format(
        start_date, end_date
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
  """.format(
        start_date, prev_value_lookback_days, end_date
    )
)

qr_code_enabled_cfg_df = spark.sql(
    """
   select
      date,
      time,
      org_id,
      object_id,
      named_struct("int_value",
      case
        when first(s3_proto_value.reported_device_config.device_config.q_r_code_reader.enabled) = 1 then 1
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

enabled_intervals_df = create_intervals(
    qr_code_enabled_cfg_df, lambda x: x == 1, query_end_ms
)
power_state_intervals_df = create_intervals(
    osdpowerstate_df, lambda x: x == 1, query_end_ms
)
power_state_or_enabled_intervals_df = power_state_intervals_df.union(
    enabled_intervals_df
)

latency_df = (
    scan_success_df.alias("a")
    .join(
        vg_cm_assoc_df.alias("b"),
        (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.cm_device_id"))
        & (F.col("a.date") == F.col("b.date")),
    )
    .join(
        power_state_intervals_df.alias("c"),
        (F.col("a.org_id") == F.col("c.org_id"))
        & (F.col("b.device_id") == F.col("c.device_id"))
        & (F.col("a.time") >= F.col("c.start_ms"))
        & (F.col("a.time") < F.col("c.end_ms")),
    )
    .groupBy("a.org_id", "a.device_id", "c.start_ms")
    .agg(F.min(F.col("a.time") - F.col("c.start_ms")).alias("latency_ms"))
    .withColumn(
        "date",
        F.to_date(
            F.from_unixtime((F.col("c.start_ms") / 1000).cast(T.IntegerType()))
        ).cast(T.StringType()),
    )
    .select(
        "date", "org_id", "device_id", F.col("start_ms").alias("time"), "latency_ms"
    )
    .withColumn("value", F.struct("latency_ms"))
    .drop("latency_ms")
)

latency_with_location_day_phase_df = add_day_phase_column(latency_df).select(
    "date", "time", "org_id", "device_id", "value.latency_ms", "day_phase"
)

create_or_update_table(
    "dataprep_firmware.qr_code_scan_success_latency",
    latency_with_location_day_phase_df,
    "date",
    ["date", "time", "org_id", "device_id", "day_phase"],
)

# COMMAND ----------

# Create table dataprep_firmware.qr_code_ir_led_disabled_duration

ir_led_df = spark.sql(
    """
	select
		a.date,
        a.time,
		a.org_id,
		a.object_id,
        named_struct("int_value",
          case
           when value.proto_value.driver_i_r_led_state.enabled is null
                     and value.is_end = false
                     and value.is_databreak = false
                     and value.proto_value.driver_i_r_led_state.source = 2 then 1
           else 0
         end,
         "proto_value", null,
         "is_databreak", false
       ) as value
	from kinesisstats.osddriverirledenabled as a
    join dataprep_firmware.device_daily_qr_code_enabled as b
      on a.date = b.date
      and a.org_id = b.org_id
      and a.object_id = b.device_id
    where a.date >= date_sub('{}', {})
      and a.date <= '{}'
""".format(
        start_date, prev_value_lookback_days, end_date
    )
)

ir_led_disabled_intervals_df = create_intervals(
    ir_led_df, lambda x: x == 1, query_end_ms
).withColumn(
    "date", F.to_date(F.from_unixtime((F.col("start_ms") / 1000).cast(T.IntegerType())))
)

ir_led_disabled_daily_intervals_df = (
    create_intervals_daily(ir_led_disabled_intervals_df, start_date, end_date)
    .withColumn("time", F.col("start_ms"))
    .withColumn("value", F.struct(F.col("end_ms")))
    .withColumn("date", F.col("date").cast(T.StringType()))
)

ir_led_disabled_duration_with_day_phase_df = (
    add_day_phase_column(ir_led_disabled_daily_intervals_df)
    .groupBy("date", "day_phase", "org_id", "device_id")
    .agg(
        F.count("*").alias("count"),
        F.sum(F.col("value.end_ms") - F.col("time")).alias("duration_ms"),
    )
)
create_or_update_table(
    "dataprep_firmware.qr_code_ir_led_disabled_duration",
    ir_led_disabled_duration_with_day_phase_df,
    "date",
    ["date", "org_id", "device_id", "day_phase"],
)

# COMMAND ----------

# Create table dataprep_firmware.qr_code_ir_led_disabled_on_trip_duration

ir_led_df = spark.sql(
    """
	select
		a.date,
        a.time,
		a.org_id,
		a.object_id,
        named_struct("int_value",
          case
           when value.proto_value.driver_i_r_led_state.enabled is null
                     and value.is_end = false
                     and value.is_databreak = false
                     and value.proto_value.driver_i_r_led_state.source = 2 then 1
           else 0
         end,
         "proto_value", null,
         "is_databreak", false
       ) as value
	from kinesisstats.osddriverirledenabled as a
    join dataprep_firmware.device_daily_qr_code_enabled as b
      on a.date = b.date
      and a.org_id = b.org_id
      and a.object_id = b.device_id
    where a.date >= date_sub('{}', {})
      and a.date <= '{}'
""".format(
        start_date, prev_value_lookback_days, end_date
    )
)

trip_intervals_df = spark.sql(
    """
  select
    date,
    org_id,
    device_id,
    proto.start.time as start_ms,
    proto.end.time as end_ms
  from trips2db_shards.trips
  where date >= date_sub('{}', {})
    and date <= '{}'
    and version = 101
  """.format(
        start_date, prev_value_lookback_days, end_date
    )
)

ir_led_disabled_intervals_df = create_intervals(
    ir_led_df, lambda x: x == 1, query_end_ms
)

ir_led_disabled_during_trip_intervals_df = intersect_intervals(
    ir_led_disabled_intervals_df, trip_intervals_df
)

ir_led_disabled_daily_intervals_df = (
    create_intervals_daily(
        ir_led_disabled_during_trip_intervals_df, start_date, end_date
    )
    .withColumn("time", F.col("start_ms"))
    .withColumn("value", F.struct(F.col("end_ms")))
    .withColumn("date", F.col("date").cast(T.StringType()))
)

ir_led_disabled_duration_with_day_phase_df = (
    add_day_phase_column(ir_led_disabled_daily_intervals_df)
    .groupBy("date", "day_phase", "org_id", "device_id")
    .agg(F.sum(F.col("value.end_ms") - F.col("time")).alias("duration_ms"))
)
create_or_update_table(
    "dataprep_firmware.qr_code_ir_led_disabled_on_trip_duration",
    ir_led_disabled_duration_with_day_phase_df,
    "date",
    ["date", "org_id", "device_id", "day_phase"],
)
