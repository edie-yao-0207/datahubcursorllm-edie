from datetime import datetime, timedelta

import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def get_crash_tagged_video_requests_with_latents(spark: SparkSession, date: str):
    ctvr = get_crash_tagged_video_requests(spark, date)
    ctvr = match_crash_tagged_video_requests_to_latents(spark, ctvr, date)
    ctvr = match_crash_tagged_video_requests_to_detected_crashes(spark, ctvr, date)
    return ctvr


def get_crash_tagged_video_requests(spark: SparkSession, date: str) -> SparkDataFrame:
    safety_events = spark.table("safetydb_shards.safety_events")
    safety_events = safety_events.where(
        (safety_events.detail_proto.accel_type == 5)
        & (safety_events.trigger_reason == 1)
        & (safety_events.date == date)
    )

    historical_video_requests = spark.table("clouddb.historical_video_requests")
    joined_df = safety_events.join(
        historical_video_requests,
        on=(
            (safety_events.device_id == historical_video_requests.device_id)
            & (safety_events.event_ms == historical_video_requests.start_ms)
        ),
    ).select(
        safety_events.date,
        safety_events.org_id,
        safety_events.device_id,
        safety_events.detail_proto.event_id.alias("event_id"),
        safety_events.event_ms.alias("time"),
        historical_video_requests.asset_ms,
        (historical_video_requests.end_ms - historical_video_requests.start_ms).alias(
            "video_length_ms"
        ),
        historical_video_requests.start_ms.alias("video_start_ms"),
        historical_video_requests.end_ms.alias("video_end_ms"),
    )

    return joined_df


def match_crash_tagged_video_requests_to_latents(
    spark: SparkSession, crash_tagged_video_requests: SparkDataFrame, date: str
) -> SparkDataFrame:
    latents = spark.table("datascience.backend_crash_model_latents")
    osd_accel_df = spark.table("kinesisstats.osdaccelerometer")
    osd_accel_df = osd_accel_df.select(
        osd_accel_df.date,
        osd_accel_df.org_id,
        osd_accel_df.value.proto_value.accelerometer_event.event_id.alias("event_id"),
        osd_accel_df.time,
        osd_accel_df.object_id.alias("device_id"),
    )
    latents = latents.join(osd_accel_df, on=["date", "org_id", "event_id"]).select(
        latents.date,
        osd_accel_df.time,
        latents.org_id,
        osd_accel_df.device_id,
        latents.event_id,
        latents.latent_model_id,
        latents.latent,
    )
    latents_filtered = latents.where(latents.date == date)
    crash_tagged_video_requests_filtered = crash_tagged_video_requests.where(
        crash_tagged_video_requests.date == date
    )
    crash_tagged_video_requests_with_latents = (
        crash_tagged_video_requests_filtered.join(
            latents_filtered,
            on=(
                (crash_tagged_video_requests_filtered.org_id == latents_filtered.org_id)
                & (crash_tagged_video_requests_filtered.date == latents_filtered.date)
                & (
                    crash_tagged_video_requests_filtered.device_id
                    == latents_filtered.device_id
                )
                & (
                    latents_filtered.time.between(
                        crash_tagged_video_requests_filtered.video_start_ms,
                        crash_tagged_video_requests_filtered.video_end_ms,
                    )
                )
            ),
        ).select(
            crash_tagged_video_requests_filtered.date,
            crash_tagged_video_requests_filtered.org_id,
            crash_tagged_video_requests_filtered.device_id,
            latents.time,
            latents.event_id,
            crash_tagged_video_requests_filtered.event_id.alias("video_event_id"),
            crash_tagged_video_requests_filtered.asset_ms,
            crash_tagged_video_requests_filtered.video_length_ms,
            crash_tagged_video_requests_filtered.video_start_ms,
            crash_tagged_video_requests_filtered.video_end_ms,
            latents.latent_model_id,
            latents.latent,
        )
    )
    return crash_tagged_video_requests_with_latents


def match_crash_tagged_video_requests_to_detected_crashes(
    spark: SparkSession, crash_tagged_video_requests: SparkDataFrame, date: str
) -> SparkDataFrame:
    safety_event_crashes = spark.table("safetydb_shards.safety_events")
    safety_event_crashes = safety_event_crashes.where(
        (safety_event_crashes.detail_proto.accel_type == 5)
        & (safety_event_crashes.trigger_reason != 1)
        & (safety_event_crashes.date == date)
    )
    safety_event_crashes = safety_event_crashes.select(
        safety_event_crashes.date,
        safety_event_crashes.event_ms,
        safety_event_crashes.org_id,
        safety_event_crashes.device_id,
        F.lit(True).alias("has_se_crash"),
    )
    crash_tagged_video_requests = crash_tagged_video_requests.join(
        safety_event_crashes,
        on=(
            (crash_tagged_video_requests.date == safety_event_crashes.date)
            & (crash_tagged_video_requests.device_id == safety_event_crashes.device_id)
            & (crash_tagged_video_requests.org_id == safety_event_crashes.org_id)
            & safety_event_crashes.event_ms.between(
                crash_tagged_video_requests.video_start_ms,
                crash_tagged_video_requests.video_end_ms,
            )
        ),
        how="left",
    ).select(
        crash_tagged_video_requests.date,
        crash_tagged_video_requests.org_id,
        crash_tagged_video_requests.device_id,
        crash_tagged_video_requests.time,
        crash_tagged_video_requests.event_id,
        crash_tagged_video_requests.event_id.alias("video_event_id"),
        crash_tagged_video_requests.asset_ms,
        crash_tagged_video_requests.video_length_ms,
        crash_tagged_video_requests.video_start_ms,
        crash_tagged_video_requests.video_end_ms,
        crash_tagged_video_requests.latent_model_id,
        crash_tagged_video_requests.latent,
        F.coalesce(safety_event_crashes.has_se_crash, F.lit(False)).alias(
            "has_se_crash"
        ),
    )

    crash_tagged_video_requests = crash_tagged_video_requests.groupBy(
        crash_tagged_video_requests.date,
        crash_tagged_video_requests.org_id,
        crash_tagged_video_requests.device_id,
        crash_tagged_video_requests.time,
        crash_tagged_video_requests.event_id,
        crash_tagged_video_requests.event_id.alias("video_event_id"),
        crash_tagged_video_requests.asset_ms,
        crash_tagged_video_requests.video_length_ms,
        crash_tagged_video_requests.video_start_ms,
        crash_tagged_video_requests.video_end_ms,
        crash_tagged_video_requests.latent_model_id,
    ).agg(
        F.first(crash_tagged_video_requests.latent).alias("latent"),
        F.max(crash_tagged_video_requests.has_se_crash).alias("has_se_crash"),
    )
    return crash_tagged_video_requests


# Allow one week for requests to come in
START_DATE = dbutils.widgets.get("start_date") or str(
    (datetime.utcnow() - timedelta(days=7)).date()
)
END_DATE = dbutils.widgets.get("end_date") or str(
    (datetime.utcnow() - timedelta(days=7)).date()
)
date_list = [d.strftime("%Y-%m_%d") for d in pd.date_range(START_DATE, END_DATE)]
for date in date_list:
    ctvr_with_latents = get_crash_tagged_video_requests_with_latents(
        spark=spark,
        date=date,
    )
    ctvr_with_latents.write.partitionBy("date").format("delta").mode(
        "overwrite"
    ).option("replaceWhere", f"date = '{date}'").saveAsTable(
        "datascience.crash_tagged_video_requests_with_latents"
    )
