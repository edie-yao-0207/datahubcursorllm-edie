from datetime import datetime, timedelta

import mlflow
import pyspark.ml.functions as MLF
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, FloatType

START_DATE = dbutils.widgets.get("start_date") or str(
    (datetime.utcnow() - timedelta(days=1)).date()
)

END_DATE = dbutils.widgets.get("end_date") or str(
    (datetime.utcnow() - timedelta(days=1)).date()
)

LATENT_TABLE = "datascience.backend_crash_model_latents"
ALL_OUTPUT = "datascience.backend_k_means_cluster_for_sampling"
SAMPLING_OUTPUT = "datascience.backend_k_means_cluster_for_sampling_items_to_request"


LOGGED_KNN_MODEL = "runs:/b4ca257dcf9d476eac2c924175196126/knn"

# Load Model
knn_model = mlflow.pyfunc.load_model(LOGGED_KNN_MODEL)._model_impl.spark_model

# target 12.5 samples per cluster per day
# Sampling rate deterined by using sampled trace data
#  from 2022-05-01 to 2022-07-27 and targeting an
#  expected quantity from each cluster of 2.5
#  notebook: https://samsara-dev-us-west-2.cloud.databricks.com/?o=5924096274798303#notebook/727431361323981/command/727431361482622
cluster_sample_rates_sdf = spark.createDataFrame(
    [
        (0, 70 * 0.000430497925311204),
        (1, 70 * 0.0000555853201178677),
        (2, 70 * 0.000879237288135594),
        (3, 70 * 0.00446236559139786),
        (4, 70 * 0.0000323763457637697),
        (5, 70 * 0.0143103448275862),
        (6, 70 * 0.0000976240884497767),
        (7, 70 * 0.00049112426035503),
        (8, 70 * 0.000181064572425829),
        (9, 70 * 0.0000192370092244936),
        (10, 70 * 0.0000255746595180872),
        (11, 70 * 0.00132165605095542),
        (12, 70 * 0.0000381503952932525),
        (13, 70 * 0.000420466058763932),
        (14, 70 * 0.000305371596762326),
        (15, 70 * 0.000688225538971809),
        (16, 70 * 0.0148214285714286),
        (17, 70 * 0.0000476737507179782),
        (18, 70 * 0.000190454336851767),
        (19, 70 * 0.0125757575757576),
        (20, 70 * 0.00237142857142857),
        (21, 70 * 0.00364035087719299),
        (22, 70 * 0.000420466058763932),
        (23, 70 * 0.00143103448275862),
        (24, 70 * 0.00039827255278311),
        (25, 70 * 0.00466292134831461),
        (26, 70 * 0.000596264367816093),
        (27, 70 * 0.00135179153094463),
        (28, 70 * 0.00110666666666667),
        (29, 70 * 0.0000160510539547477),
        (30, 70 * 0.00244117647058824),
        (31, 70 * 0.000780075187969926),
        (32, 70 * 0.000407262021589795),
        (33, 70 * 0.000619402985074628),
        (34, 70 * 0.000454048140043764),
        (35, 70 * 0.000882978723404257),
        (36, 70 * 0.00033685064935065),
        (37, 70 * 0.000475917431192661),
        (38, 70 * 0.0045108695652174),
        (39, 70 * 0.00399038461538462),
    ],
    ["cluster", "cluster_sample_rate"],
)

dataset = spark.table(LATENT_TABLE)
dataset = dataset.where(dataset.date.between(START_DATE, END_DATE))
dataset = dataset.withColumn("features", MLF.array_to_vector(dataset.latent))
dataset = knn_model.transform(dataset)
dataset = dataset.withColumnRenamed("prediction", "cluster")
osd_accel_df = spark.table("kinesisstats.osdaccelerometer")
osd_accel_df = osd_accel_df.where(
    osd_accel_df.value.proto_value.accelerometer_event.ingestion_tag == 1
)
osd_accel_df = osd_accel_df.where(
    F.expr(
        "value.proto_value.accelerometer_event.harsh_accel_type IN (5, 28) OR AGGREGATE(value.proto_value.accelerometer_event.imu_harsh_event.secondary_events, FALSE, (acc, x) -> acc OR (x.type IN (5, 28)))"
    )
)
osd_accel_df = osd_accel_df.select(
    osd_accel_df.date,
    osd_accel_df.org_id,
    osd_accel_df.value.proto_value.accelerometer_event.event_id.alias("event_id"),
    osd_accel_df.time,
    osd_accel_df.object_id.alias("device_id"),
)
dataset = dataset.join(osd_accel_df, on=["date", "org_id", "event_id"])

dataset = dataset.select(
    "date",
    "org_id",
    "device_id",
    "time",
    "event_id",
    "latent_model_id",
    "latent",
    "cluster",
)
dataset = dataset.withColumn("rand", F.rand())
dataset = dataset.join(cluster_sample_rates_sdf, on=["cluster"])

device_event_sampling_discount = dataset.groupBy("date", "device_id").agg(
    (1 / F.count(F.lit(1))).alias("sampling_discount")
)
dataset = dataset.join(device_event_sampling_discount, on=["date", "device_id"]).select(
    dataset.cluster,
    dataset.date,
    dataset.org_id,
    dataset.device_id,
    dataset.time,
    dataset.event_id,
    dataset.latent_model_id,
    dataset.latent,
    dataset.rand,
    (
        dataset.cluster_sample_rate * device_event_sampling_discount.sampling_discount
    ).alias("sample_rate"),
    dataset.cluster_sample_rate,
    device_event_sampling_discount.sampling_discount,
)
dataset.write.partitionBy("date").mode("overwrite").option(
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
