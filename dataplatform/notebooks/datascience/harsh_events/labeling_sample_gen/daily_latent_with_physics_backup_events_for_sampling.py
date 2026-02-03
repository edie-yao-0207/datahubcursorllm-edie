from datetime import datetime, timedelta

import numpy as np
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, row_number
from pyspark.sql.types import ArrayType, FloatType, IntegerType, StringType
from pyspark.sql.window import Window

PRODUCTION_MODEL_NAME = "latent_with_physics"

START_DATE = dbutils.widgets.get("start_date") or str(
    (datetime.utcnow() - timedelta(days=1)).date()
)
END_DATE = dbutils.widgets.get("end_date") or str(
    (datetime.utcnow() - timedelta(days=1)).date()
)

# Used to test over 14 days
# START_DATE = str(
#     (datetime.utcnow() - timedelta(days=14)).date()
# )
# END_DATE = str(
#     (datetime.utcnow() - timedelta(days=1)).date()
# )

INFERENCE_RESULTS_TABLE = "datastreams.crash_detector_inference_results"
ALL_OUTPUT = "datascience.backend_backup_events"
SAMPLING_OUTPUT = "datascience.backend_backup_events_items_to_request"

raw_inf = table(INFERENCE_RESULTS_TABLE)

cluster_sample_rates_sdf = spark.createDataFrame(
    [
        ("HEAVY_HEAVY_BACKUP", 0.002882),
        ("HEAVY_HEAVY_POTENTIAL_BACKUP", 0.006338),
        ("HEAVY_LIGHT_BACKUP", 1.000000),
        ("HEAVY_LIGHT_POTENTIAL_BACKUP", 1.000000),
        ("HEAVY_PASSENGER_BACKUP", 1.000000),
        ("HEAVY_PASSENGER_POTENTIAL_BACKUP", 1.000000),
        ("HEAVY_UNKNOWN_BACKUP", 0.021541),
        ("HEAVY_UNKNOWN_POTENTIAL_BACKUP", 0.027043),
        ("LIGHT_HEAVY_BACKUP", 0.107143),
        ("LIGHT_HEAVY_POTENTIAL_BACKUP", 0.139319),
        ("LIGHT_LIGHT_BACKUP", 1.00000),
        ("LIGHT_LIGHT_POTENTIAL_BACKUP", 1.00000),
        ("LIGHT_PASSENGER_BACKUP", 1.000000),
        ("LIGHT_PASSENGER_POTENTIAL_BACKUP", 1.000000),
        ("LIGHT_UNKNOWN_BACKUP", 0.432692),
        ("LIGHT_UNKNOWN_POTENTIAL_BACKUP", 0.401786),
        ("PASSENGER_HEAVY_BACKUP", 0.089463),
        ("PASSENGER_HEAVY_POTENTIAL_BACKUP", 0.088757),
        ("PASSENGER_LIGHT_BACKUP", 1.000000),
        ("PASSENGER_LIGHT_POTENTIAL_BACKUP", 1.000000),
        ("PASSENGER_PASSENGER_BACKUP", 1.000000),
        ("PASSENGER_PASSENGER_POTENTIAL_BACKUP", 1.000000),
        ("PASSENGER_UNKNOWN_BACKUP", 0.095745),
        ("PASSENGER_UNKNOWN_POTENTIAL_BACKUP", 0.065502),
    ],
    ["backup_group", "backup_group_sample_rate"],
)

raw_inf = raw_inf.where(raw_inf.date.between(START_DATE, END_DATE))
raw_inf = raw_inf.where(F.col("model_version") == F.lit(PRODUCTION_MODEL_NAME))


fname = "/mnt/samsara-databricks-workspace/datascience/manually_labeled_edge_cases/Manual Vehicle Type Lookup 04 28 2022.csv"
gvwr = spark.read.csv(fname, header=True)
gvwr = (
    gvwr.withColumnRenamed("formatted_make", "make")
    .withColumnRenamed("formatted_model", "model")
    .withColumnRenamed("manual_vehicle_type", "vehicle_type")
    .drop("num_devices", "num_recovered")
)

gvwr = gvwr.select(
    F.lower(gvwr.make).alias("make"),
    F.lower(gvwr.model).alias("model"),
    F.lower(gvwr.vehicle_type).alias("gvwr_vehicle_type"),
)

gvwr_cloud = spark.table("clouddb.vehicle_make_model_weights")
gvwr_cloud = gvwr_cloud.select(
    F.lower("make").alias("make"),
    F.lower("model").alias("model"),
    F.when(gvwr_cloud.max_weight_lbs <= 0, "unknown")
    .when(gvwr_cloud.max_weight_lbs < 6_000, "passenger")
    .when(gvwr_cloud.max_weight_lbs < 26000, "light")
    .when(gvwr_cloud.max_weight_lbs >= 26000, "heavy")
    .otherwise(None)
    .alias("gvwr_vehicle_type"),
)
gvwr = gvwr.union(gvwr_cloud)

vehicles = spark.table("productsdb.devices")
vehicles = vehicles.select(
    vehicles.id.alias("device_id"),
    F.lower(vehicles.make).alias("make"),
    F.lower(vehicles.model).alias("model"),
)
gvwr = gvwr.join(
    vehicles,
    on=["make", "model"],
    how="left",
)

raw_inf = raw_inf.join(gvwr, on=["device_id"], how="left")
raw_inf = raw_inf.withColumn("gvwr_vehicle_type", F.upper(F.col("gvwr_vehicle_type")))
raw_inf = raw_inf.fillna("UNKNOWN", subset="gvwr_vehicle_type")

raw_inf = raw_inf.withColumn("p_crash", F.col("probability_array").getItem(1))
raw_inf = raw_inf.withColumn("p_backup", F.col("probability_array").getItem(2))
raw_inf = raw_inf.withColumn("p_noevent", F.col("probability_array").getItem(0))

raw_inf = raw_inf.select(
    "date",
    "org_id",
    "device_id",
    "event_id",
    "harsh_event_type",
    "model_version",
    "timestamp",
    F.col("event_time_ms").alias("time"),
    F.col("vehicle_type").alias("customer_vehicle_type"),
    "gvwr_vehicle_type",
    "p_crash",
    "p_backup",
    "p_noevent",
)
raw_inf = raw_inf.withColumn(
    "customer_vehicle_type",
    F.element_at(F.split(F.col("customer_vehicle_type"), "_"), -1),
)


CRASH_THRESHOLD = 0.25
raw_inf = raw_inf.withColumn(
    "is_crash", F.when(F.col("p_crash") >= CRASH_THRESHOLD, True).otherwise(False)
)

BACKUP_THRESHOLD = 0.58
POTENTIAL_BACKUP_THRESHOLD = 0.30
raw_inf = raw_inf.withColumn(
    "backup_name",
    F.when(~(F.col("is_crash")) & (F.col("p_backup") >= BACKUP_THRESHOLD), "BACKUP")
    .when(
        ~(F.col("is_crash"))
        & (F.col("p_backup") < BACKUP_THRESHOLD)
        & (F.col("p_backup") >= POTENTIAL_BACKUP_THRESHOLD),
        "POTENTIAL_BACKUP",
    )
    .otherwise("NON_EVENT"),
)


raw_inf = raw_inf.withColumn(
    "backup_group",
    F.concat_ws(
        "_",
        F.col("customer_vehicle_type"),
        F.col("gvwr_vehicle_type"),
        F.col("backup_name"),
    ),
)


raw_inf = raw_inf.withColumn("rand", F.rand())
raw_inf = raw_inf.join(cluster_sample_rates_sdf, on=["backup_group"])

device_event_sampling_discount = raw_inf.groupBy("date", "device_id").agg(
    (1 / F.count(F.lit(1))).alias("sampling_discount")
)

raw_inf = raw_inf.join(device_event_sampling_discount, on=["date", "device_id"]).select(
    raw_inf.date,
    raw_inf.org_id,
    raw_inf.device_id,
    raw_inf.event_id,
    raw_inf.harsh_event_type,
    raw_inf.model_version,
    raw_inf.timestamp,
    raw_inf.time,
    raw_inf.customer_vehicle_type,
    raw_inf.gvwr_vehicle_type,
    raw_inf.p_crash,
    raw_inf.p_backup,
    raw_inf.p_noevent,
    raw_inf.is_crash,
    raw_inf.backup_name,
    raw_inf.backup_group,
    raw_inf.rand,
    (
        raw_inf.backup_group_sample_rate
        * device_event_sampling_discount.sampling_discount
    ).alias("sample_rate"),
    raw_inf.backup_group_sample_rate,
    device_event_sampling_discount.sampling_discount,
)

raw_inf.write.partitionBy("date").mode("overwrite").option(
    "mergeSchema", "true"
).option("replaceWhere", f"date BETWEEN '{START_DATE}' AND '{END_DATE}'").saveAsTable(
    ALL_OUTPUT
)

samples = spark.table(ALL_OUTPUT)
samples = (
    samples.where(samples.date.between(START_DATE, END_DATE))
    .where(samples.rand <= samples.sample_rate)
    .withColumn("start_ms", samples.time - 5_000)
    .withColumn("end_ms", samples.time + 5_000)
)
samples.write.partitionBy("date").mode("overwrite").option(
    "mergeSchema", "true"
).option("replaceWhere", f"date BETWEEN '{START_DATE}' AND '{END_DATE}'").saveAsTable(
    SAMPLING_OUTPUT
)
