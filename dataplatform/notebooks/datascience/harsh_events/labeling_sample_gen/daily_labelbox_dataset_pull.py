# Databricks notebook source
# MAGIC %pip install labelbox

# COMMAND ----------

# MAGIC %pip install labelbox[data] --upgrade

# COMMAND ----------

from functools import reduce
from collections import namedtuple
import labelbox

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from labelbox import Client

from pyspark.sql.types import StringType
from pathlib import Path


@udf(StringType())
def get_external_id(path: str) -> str:
    if path is None:
        return None
    return Path(path).name


DATASET_NAMES = [
    "fleet-hev1-crash-with-hev2-events-videos",
    "fleet-customer-crash-videos",
    "fleet-backend-crash-videos",
    "fleet-crash-videos",
]


TABLE = "datascience.uploaded_labelbox_datasets"

DatasetRow = namedtuple(
    "DatasetRow",
    [
        "external_id",
        "dataset_name",
        "created_time",
        "created_date",
    ],
)

DatasetSchema = StructType(
    [
        StructField("external_id", StringType()),
        StructField("dataset_name", StringType()),
        StructField("created_time", TimestampType()),
        StructField("created_date", StringType()),
    ]
)


lb_client = Client(dbutils.secrets.get(scope="labelbox", key="demokey"))
datasets = []
rows = []
for name in DATASET_NAMES:
    dataset = list(lb_client.get_datasets(where=(labelbox.Dataset.name == name)))[0]
    for row in list(dataset.export_data_rows()):
        rows.append(
            DatasetRow(
                row.external_id, name, row.created_at, str(row.created_at.date())
            )
        )

df = spark.createDataFrame(rows, DatasetSchema)


# Getting Stitched Videos
to_union = []
hev1_not_hev2_videos = spark.table(
    "dojo.daily_stitched_hev1_crash_with_hev2_events_videos_for_labeling"
)
hev1_not_hev2_videos = hev1_not_hev2_videos.select(
    F.col("hev2_event_id").alias("event_id"),
    get_external_id("stitched_url").alias("external_id"),
    F.col("job_names"),
)
to_union.append(hev1_not_hev2_videos)
customer_crash_videos = spark.table(
    "dojo.daily_stitched_customer_crash_videos_for_labeling"
)
customer_crash_videos = customer_crash_videos.select(
    F.explode("osd_accel_event_ids").alias("event_id"),
    get_external_id("stitched_url").alias("external_id"),
    F.col("job_names"),
)
to_union.append(customer_crash_videos)
hev2_crash_videos = spark.table("dojo.daily_stitched_hev2_crash_videos_for_labeling")
hev2_crash_videos = hev2_crash_videos.select(
    F.col("event_id"),
    get_external_id("stitched_url").alias("external_id"),
    F.col("job_names"),
)

to_union.append(hev2_crash_videos)
sampled_crash_videos_dojo = spark.table("dojo.daily_stitched_videos_for_labeling")

sampled_crash_videos_dojo = sampled_crash_videos_dojo.select(
    F.explode(F.col("event_ids")).alias("event_id"),
    get_external_id("stitched_url").alias("external_id"),
)

sampled_crash_videos_ds = spark.table("datascience.daily_stitched_videos_for_labeling")

sampled_crash_videos_ds = sampled_crash_videos_ds.select(
    F.explode(F.col("event_ids")).alias("event_id"),
    get_external_id("stitched_url").alias("external_id"),
)

sampled_crash_videos = sampled_crash_videos_ds.unionAll(sampled_crash_videos_dojo)
event_id_map = spark.table("datascience.daily_video_requests")
event_id_map = event_id_map.withColumn("event_id", F.explode("event_ids")).distinct()
event_id_map = event_id_map.select(
    "date",
    "org_id",
    "device_id",
    "event_id",
    "job_names",
    F.lit(None).alias("time"),
)
sampled_crash_videos = event_id_map.join(sampled_crash_videos, on=["event_id"]).select(
    "event_id",
    "external_id",
    "job_names",
)
to_union.append(sampled_crash_videos)
stitched_videos = reduce(DataFrame.unionAll, to_union)

to_write = df.join(
    stitched_videos, on=(df.external_id == stitched_videos.external_id), how="outer"
).select(
    df.external_id,
    df.dataset_name,
    df.created_date,
    df.created_time,
    stitched_videos.job_names,
    stitched_videos.event_id,
)
to_write.write.partitionBy("created_date").mode("overwrite").saveAsTable(TABLE)
