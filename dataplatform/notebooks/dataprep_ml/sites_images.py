# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------

import itertools
import os

from PIL import Image
import boto3
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import types as T


def s3url_to_bucket_and_key(s3url):
    """parses "s3://bucket/key" to ("bucket", "key")"""
    if s3url.startswith("s3://"):
        s3url = s3url[len("s3://") :]
    slash_idx = s3url.index("/")  # This will raise a ValueError if not found
    return s3url[:slash_idx], s3url[slash_idx + 1 :]


def s3_list_objects(s3_uri, delimiter=""):
    s3 = get_s3_client("samsara-workforce-data-read")
    s3_bucket, s3_key = s3url_to_bucket_and_key(s3_uri)
    paginator = s3.get_paginator("list_objects_v2")
    objs = []
    for page in paginator.paginate(
        Bucket=s3_bucket, Prefix=s3_key, Delimiter=delimiter
    ):
        objs.append(page)
    return objs


def s3_get_object(s3_uri):
    s3 = get_s3_client("samsara-workforce-data-read")
    s3_bucket, s3_key = s3url_to_bucket_and_key(s3_uri)
    try:
        obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    except:
        obj = {}
    return obj


def get_image_shape(s3_uri):
    obj = s3_get_object(s3_uri)
    try:
        im = Image.open(obj.get("Body"))
        im_shape = im.size
    except:
        im_shape = (None, None)
    return im_shape


SOURCE_BUCKET = "samsara-workforce-data"
DBRICKS_DATABASE = "dataprep_ml"
DBRICKS_TABLE = "sites_images"

s3 = get_s3_client("samsara-workforce-data-read")
s3_objs = s3_list_objects(f"s3://{SOURCE_BUCKET}/raw_frames/", delimiter="/")
s3_prefixes = itertools.chain.from_iterable([key["CommonPrefixes"] for key in s3_objs])
rdd = spark.sparkContext.parallelize(s3_prefixes, numSlices=400)
rdd = rdd.flatMap(lambda x: [f"s3://{SOURCE_BUCKET}/{x['Prefix']}"])
rdd = rdd.flatMap(lambda x: s3_list_objects(x, delimiter="/"))
rdd = rdd.flatMap(
    lambda x: [f"s3://{SOURCE_BUCKET}/{y['Prefix']}" for y in x["CommonPrefixes"]]
)
rdd = rdd.flatMap(lambda x: s3_list_objects(x))
rdd = rdd.flatMap(lambda x: x.get("Contents"))
rdd = rdd.map(
    lambda x: {
        "key": x["Key"],
        "image_url": os.path.join("s3://", SOURCE_BUCKET, x["Key"]),
        "size_bytes": x["Size"],
        "org_stream_timestamp": x["Key"].split(".")[-2].split("/")[-3:],
    }
)
rdd = rdd.map(lambda x: dict(x, **{"shape": get_image_shape(x["image_url"])}))
rdd = rdd.map(lambda x: dict(x, **{"width": x["shape"][0], "height": x["shape"][1]}))
rdd = rdd.map(lambda x: Row(**x))
rdd = rdd.persist()
rdd.count()
df = spark.createDataFrame(rdd, samplingRatio=0.99)
df = df.select(
    "*",
    F.col("org_stream_timestamp").getItem(0).astype("long").alias("org_id"),
    F.col("org_stream_timestamp").getItem(1).astype("long").alias("stream_id"),
    (F.col("org_stream_timestamp").getItem(2).astype("long") * 1000).alias("time"),
    F.to_date(
        F.from_unixtime(F.col("org_stream_timestamp").getItem(2).astype("long"))
    ).alias("date"),
)

df_table = df.select(
    "image_url", "date", "time", "org_id", "stream_id", "size_bytes", "width", "height"
)
df_table.write.format("delta").mode("overwrite").saveAsTable(
    f"{DBRICKS_DATABASE}.{DBRICKS_TABLE}",
    partitionBy=["date", "org_id", "stream_id"],
)
