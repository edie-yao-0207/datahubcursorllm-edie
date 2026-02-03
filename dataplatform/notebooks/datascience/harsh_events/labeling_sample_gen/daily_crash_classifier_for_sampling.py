from datetime import datetime, timedelta

import mlflow
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, FloatType

START_DATE = dbutils.widgets.get("start_date") or str(
    (datetime.utcnow() - timedelta(days=1)).date()
)

END_DATE = dbutils.widgets.get("end_date") or str(
    (datetime.utcnow() - timedelta(days=1)).date()
)

LATENT_TABLE = "datascience.backend_crash_model_latents"
ALL_OUTPUT = "datascience.backend_crash_classifier_predictions_for_sampling"
SAMPLING_OUTPUT = (
    "datascience.backend_crash_classifier_predictions_for_sampling_items_to_request"
)
LOGGED_CRASH_MODEL = "runs:/998fca1de3a6450a8114bbc2dfc0d00b/best_model"

# Load model and broadcast
loaded_crash_model = mlflow.pyfunc.load_model(LOGGED_CRASH_MODEL)._model_impl
_global_crash_model = sc.broadcast(loaded_crash_model)


def predict_crash(arr):
    global _global_crash_model
    model = _global_crash_model.value
    return list(map(float, model.predict_proba([arr])[0].tolist()))


predict_crash_udf = F.udf(predict_crash, ArrayType(FloatType()))

# Target about 50 samples per decile per day
# Sampling rate deterined by using sampled trace data
#  from 2022-05-01 to 2022-07-27 and targeting an
#  expected quantity from each decile of 10
#  notebook: https://samsara-dev-us-west-2.cloud.databricks.com/?o=5924096274798303#notebook/727431361323981/command/727431361482622
decile_sample_rates_sdf = spark.createDataFrame(
    [
        (0, 5 * 0.0000175549915397631),
        (1, 5 * 0.0000734773371104817),
        (2, 5 * 0.000322017458777886),
        (3, 5 * 0.000737777777777779),
        (4, 5 * 0.00126428027418127),
        (5, 5 * 0.00190366972477065),
        (6, 5 * 0.00253048780487805),
        (7, 5 * 0.00372197309417041),
        (8, 5 * 0.00603636363636365),
        (9, 5 * 0.830000000000001),
    ],
    ["crash_decile", "sample_rate"],
)

dataset = spark.table(LATENT_TABLE)
dataset = dataset.where(dataset.date.between(START_DATE, END_DATE))
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

dataset = dataset.repartition(1000)
dataset = dataset.withColumn("crash_predictions", predict_crash_udf(dataset.latent))
dataset = dataset.withColumn(
    "crash_decile", (dataset.crash_predictions[1] * 10).cast("int")
)

dataset = dataset.select(
    "date",
    "org_id",
    "device_id",
    "time",
    "event_id",
    "latent_model_id",
    "latent",
    "crash_predictions",
    "crash_decile",
)
dataset = dataset.withColumn("rand", F.rand())
dataset = dataset.join(decile_sample_rates_sdf, on=["crash_decile"])

dataset.write.partitionBy("date").mode("overwrite").option(
    "replaceWhere", f"date BETWEEN '{START_DATE}' AND '{END_DATE}'"
).saveAsTable(ALL_OUTPUT)

samples = spark.table(ALL_OUTPUT)
samples = (
    samples.where(samples.date.between(START_DATE, END_DATE))
    .where(samples.rand <= samples.sample_rate)
    .withColumn("start_ms", samples.time - 5_000)
    .withColumn("end_ms", samples.time + 5_000)
)
samples.write.partitionBy("date").mode("overwrite").option(
    "replaceWhere", f"date BETWEEN '{START_DATE}' AND '{END_DATE}'"
).saveAsTable(SAMPLING_OUTPUT)
