# Databricks notebook source
from datetime import date as dt
from datetime import timedelta
from typing import Tuple

import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# COMMAND ----------


def get_hev1_crashes(spark: SparkSession, date: str) -> SparkDataFrame:
    # Select all hev1 crashes surfaced in safetydb without write path filters before safety inbox
    # backend/go/src/samsaradev.io/safety/safetyproto/behavior_labels.proto
    # https://samsara-rd.slack.com/archives/C02QU7TL1DJ/p1641339720005500
    # additional labels are use to detect SYSTEM crash labels. These are distinct from event types specified in the osdaccelerometer.harsh_accel_type
    hev1_crashes_safetydb_query = f"""select
      org_id,
      device_id,
      date,
      event_ms,
      detail_proto.event_id as event_id,
      "crash" as backend_event_type,
      case
        when detail_proto.accel_type = 1 then "harsh_accel"
        when detail_proto.accel_type = 2 then "harsh_brake"
        when detail_proto.accel_type = 3 then "harsh_turn"
        when detail_proto.accel_type in (5, 28) then "crash" -- included 28 (rollovers) as they are treated as crashes
        else "other"
      end as fw_event_type,
      case
        when trip_start_ms > 0 then True
        else False
      end as on_trip
    from
      safetydb_shards.safety_events
    where
      (
        -- hev1 running
        detail_proto.ingestion_tag = 0
        -- identifiers of hev1 events classified as hev1 crash in the backend
        and (
          additional_labels.additional_labels [0].label_type is not null
          or additional_labels.additional_labels [1].label_type is not null
          or additional_labels.additional_labels [2].label_type is not null
        )
        and (
          additional_labels.additional_labels [0].label_source = 1
          or additional_labels.additional_labels [1].label_source = 1
          or additional_labels.additional_labels [2].label_source = 1
        )
        and (
          additional_labels.additional_labels [0].label_type = 19
          or additional_labels.additional_labels [1].label_type = 19
          or additional_labels.additional_labels [2].label_type = 19
        )
      )
      and date = "{date}" """

    hev1_crashes_safetydb = spark.sql(hev1_crashes_safetydb_query)
    return hev1_crashes_safetydb


def get_hev2_crashes(spark: SparkSession, date: str) -> SparkDataFrame:
    # Select all hev2 crashes from safetydb without write path filters before safety inbox
    hev2_crashes_safetydb_query = f"""select
      org_id,
      device_id,
      date,
      event_ms,
      detail_proto.event_id as event_id,
      case
        when detail_proto.accel_type = 1 then "harsh_accel"
        when detail_proto.accel_type = 2 then "harsh_brake"
        when detail_proto.accel_type = 3 then "harsh_turn"
        when detail_proto.accel_type in (5, 28) then "crash" -- included 28 (rollovers) as they are treated as crashes
        else "other"
      end as harsh_event_type,
      case
        when trip_start_ms > 0 then True
        else False
      end as on_trip
    from
      safetydb_shards.safety_events
    where
      detail_proto.ingestion_tag = 1
      and detail_proto.accel_type in (5, 28)
      and date = "{date}" """

    hev2_crashes_safetydb = spark.sql(hev2_crashes_safetydb_query)

    # Add column identifying events as hev2 crash
    hev2_crashes_safetydb = hev2_crashes_safetydb.withColumn(
        "is_hev2_crash", F.lit(True)
    )
    return hev2_crashes_safetydb


def get_all_hev2_events(spark: SparkSession, date: str) -> SparkDataFrame:
    # Get all hev2 events from osdaccelerometer. These are all events on the ingestion path that have hev2 oriented data
    all_hev2_events_query = f"""select
        org_id,
        object_id as device_id,
        date,
        time,
        value.proto_value.accelerometer_event.event_id as event_id,
        case
          when value.proto_value.accelerometer_event.harsh_accel_type = 1 then "harsh_accel"
          when value.proto_value.accelerometer_event.harsh_accel_type = 2 then "harsh_brake"
          when value.proto_value.accelerometer_event.harsh_accel_type = 3 then "harsh_turn"
          when value.proto_value.accelerometer_event.harsh_accel_type in (5, 28) then "crash" -- included 28 (rollovers) as they are treated as crashes
          else "other"
        end as event_type
      from
        kinesisstats.osdaccelerometer
      where
        value.proto_value.accelerometer_event.ingestion_tag = 1
        and date = "{date}" """

    all_hev2_events = spark.sql(all_hev2_events_query)
    return all_hev2_events


def assign_is_crash_to_events(
    spark: SparkSession, events: SparkDataFrame, crashes: SparkDataFrame
) -> Tuple[SparkDataFrame, SparkDataFrame]:
    # Join crashes onto all events and label all events without entries in crashes as non-crashes
    events = (
        events.join(crashes, crashes.event_id == events.event_id, "left")
        .select(
            events.org_id,
            events.device_id,
            events.date,
            events.time,
            events.event_id,
            events.event_type,
            crashes.is_hev2_crash,
        )
        .fillna(False, "is_hev2_crash")
    )

    # Join the opposite way to verify that there aren't hev2 crashes without osdaccelerometer events
    crashes_without_events = events.join(
        crashes, crashes.event_id == events.event_id, "right"
    ).where(events.event_id.isNull())

    return events, crashes_without_events


def drop_hev2_crashes(
    spark: SparkSession, hev2_events: SparkDataFrame
) -> SparkDataFrame:
    # Drop all hev2 crashes from table. Only want hev1 crashes that aren't already in the hev2 crash bucket
    hev2_events = hev2_events.filter(F.col("is_hev2_crash") == False)

    return hev2_events


def match_hev1_crashes_to_hev2_events(
    spark: SparkSession, hev1: SparkDataFrame, hev2: SparkDataFrame
) -> SparkDataFrame:

    # Choose 10 second delay for time between events to account for differences in hev1 and hev2 identification of the
    # beginning of the event
    num_ms_between_events = 10000

    # Join HEv1 and HEv2 events keeping only events within a certain delay
    join_condition = [
        hev1.event_ms.between(
            hev2.time - num_ms_between_events, hev2.time + num_ms_between_events
        )
        & (hev1.device_id == hev2.device_id)
        & (hev1.org_id == hev2.org_id)
    ]  # need to join on device id and org id

    hev1_crashes_with_hev2_events = hev2.join(hev1, join_condition, "inner").select(
        hev1.org_id.alias("org_id"),
        hev1.device_id.alias("device_id"),
        hev1.date.alias("date"),
        hev1.event_ms.alias("hev1_time"),
        hev2.time.alias("hev2_time"),
        hev1.event_id.alias("hev1_event_id"),
        hev2.event_id.alias("hev2_event_id"),
        hev1.backend_event_type.alias("hev1_backend_event_type"),
        hev1.fw_event_type.alias("hev1_fw_event_type"),
        hev2.event_type.alias("hev2_event_type"),
        hev2.is_hev2_crash.alias("is_hev2_crash"),
    )
    return hev1_crashes_with_hev2_events


def assign_has_video_to_events(
    spark: SparkSession, events_df: SparkDataFrame
) -> SparkDataFrame:

    # Join HEv1 and HEv2 events to osduploadedfileset to check for an mp4 file to ID events that should have a video
    # for labelers to watch
    f1 = spark.table("kinesisstats.osduploadedfileset")
    events_df = events_df.join(
        f1,
        [
            (events_df.hev1_event_id == f1.value.proto_value.uploaded_file_set.event_id)
            & (events_df.date == f1.date)
        ],
        "left",
    ).select(
        events_df.org_id,
        events_df.device_id,
        events_df.date,
        events_df.hev1_time,
        events_df.hev2_time,
        events_df.hev1_event_id,
        events_df.hev2_event_id,
        events_df.hev1_backend_event_type,
        events_df.hev1_fw_event_type,
        events_df.hev2_event_type,
        events_df.is_hev2_crash,
        f1.value.proto_value.uploaded_file_set.s3urls.alias("hev1_s3urls_array"),
    )

    f2 = spark.table("kinesisstats.osduploadedfileset")
    events_df = events_df.join(
        f2,
        [
            (events_df.hev2_event_id == f2.value.proto_value.uploaded_file_set.event_id)
            & (events_df.date == f2.date)
        ],
        "left",
    ).select(
        events_df.org_id,
        events_df.device_id,
        events_df.date,
        events_df.hev1_time,
        events_df.hev2_time,
        events_df.hev1_event_id,
        events_df.hev2_event_id,
        events_df.hev1_backend_event_type,
        events_df.hev1_fw_event_type,
        events_df.hev2_event_type,
        events_df.is_hev2_crash,
        events_df.hev1_s3urls_array,
        f2.value.proto_value.uploaded_file_set.s3urls.alias("hev2_s3urls_array"),
    )
    events_df = events_df.withColumn(
        "hev1_s3urls_array", F.concat_ws(",", F.col("hev1_s3urls_array"))
    )
    events_df = events_df.withColumn(
        "hev2_s3urls_array", F.concat_ws(",", F.col("hev2_s3urls_array"))
    )

    events_df = events_df.withColumn(  # type:ignore
        "has_hev1_video_asset",  # type:ignore
        F.when(  # type:ignore
            F.col("hev1_s3urls_array").contains(F.lit(".mp4")), True  # type:ignore
        ).otherwise(  # type:ignore
            False  # type:ignore
        ),  # type:ignore
    )  # type:ignore

    events_df = events_df.withColumn(  # type:ignore
        "has_hev2_video_asset",  # type:ignore
        F.when(  # type:ignore
            F.col("hev2_s3urls_array").contains(F.lit(".mp4")), True  # type:ignore
        ).otherwise(  # type:ignore
            False  # type:ignore
        ),  # type:ignore
    )  # type:ignore

    # Cleaner way of doing this -- removed because backend version of python 3.0.3 and exists needs 3.1 or later
    #
    # has_video_extension = lambda x: x.contains(".mp4")
    # events_df = events_df.withColumn(
    #     "has_hev1_video_asset",
    #     F.when(
    #         F.exists(F.col("hev1_s3urls_array"), has_video_extension), True
    #     ).otherwise(False),
    # )
    # events_df = events_df.withColumn(
    #     "has_hev2_video_asset",
    #     F.when(
    #         F.exists(F.col("hev2_s3urls_array"), has_video_extension), True
    #     ).otherwise(False),
    # )

    return events_df


def process(spark: SparkSession, date: str):
    hev1_crashes_safetydb = get_hev1_crashes(spark, date)
    hev2_crashes_safetydb = get_hev2_crashes(spark, date)

    hev2_events = get_all_hev2_events(spark, date)
    hev2_events, hev2_crashes_without_events = assign_is_crash_to_events(
        spark, hev2_events, hev2_crashes_safetydb
    )
    hev2_events = drop_hev2_crashes(spark, hev2_events)

    events = match_hev1_crashes_to_hev2_events(
        spark, hev1_crashes_safetydb, hev2_events
    )
    events = assign_has_video_to_events(spark, events)

    hev1_join_condition = [
        (events.hev1_time == hev1_crashes_safetydb.event_ms)
        & (events.device_id == hev1_crashes_safetydb.device_id)
        & (events.org_id == hev1_crashes_safetydb.org_id)
    ]
    events = events.join(hev1_crashes_safetydb, hev1_join_condition, "left").select(
        events["*"], hev1_crashes_safetydb["on_trip"].alias("hev1_on_trip")
    )

    hev2_join_condition = [
        (events.hev2_time == hev2_crashes_safetydb.event_ms)
        & (events.device_id == hev2_crashes_safetydb.device_id)
        & (events.org_id == hev2_crashes_safetydb.org_id)
    ]
    events = events.join(hev2_crashes_safetydb, hev2_join_condition, "left").select(
        events["*"], hev2_crashes_safetydb["on_trip"].alias("hev2_on_trip")
    )
    events = events.withColumn("date", events.date.cast("date"))

    events.createOrReplaceTempView("events_table")

    table_name = "datascience.hev1_crashes_with_hev2_events"

    events.write.format("delta").mode("overwrite").partitionBy("date").option(
        "replaceWhere", f"date = '{date}'"
    ).saveAsTable(table_name)

    if hev2_crashes_without_events is not None:
        no_event_crash_count = hev2_crashes_without_events.count()
    else:
        no_event_crash_count = 0

    print(
        f"hev2 crashes without a corresponding osdaccelerometer event: {no_event_crash_count}"
    )
    print(f"{date} added to table")


# COMMAND ----------

end_date = dt.today() - timedelta(days=1)  # inclusive
start_date = end_date - timedelta(days=7)  # inclusive

dates = pd.date_range(start_date, end_date, freq="d")
for d in dates:
    process(spark, d.strftime("%Y-%m-%d"))
