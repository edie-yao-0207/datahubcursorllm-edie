from datetime import datetime, timedelta
import os
import random

import mlflow
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, FloatType
import tqdm

import torch

SOURCE_TABLE = "datascience.backend_crash_model_traces"
WRITE_TABLE = "datascience.backend_crash_model_latents"

LATENT_MODEL_ID = "d4cd25c867584829b0216d7f660e6a37"

START_DATE = dbutils.widgets.get("start_date") or str(
    (datetime.utcnow() - timedelta(days=1)).date()
)
END_DATE = dbutils.widgets.get("end_date") or str(
    (datetime.utcnow() - timedelta(days=1)).date()
)
NUM_BATCHES = int(dbutils.widgets.get("num_batches")) or 5

run_dates = [d.strftime("%Y-%m-%d") for d in pd.date_range(START_DATE, END_DATE)]

logged_model = f"runs:/{LATENT_MODEL_ID}/best_model"

# Load model from MLFlow
loaded_model = mlflow.pyfunc.load_model(logged_model)._model_impl.pytorch_model

# Extract layers due to pickling issues
loaded_model = loaded_model.online_representation.eval().double()
conv_layers = loaded_model.conv_layers
transformer_layer = loaded_model.transformer_layer


# Broadcast model to use in UDF
_conv_layers = sc.broadcast(conv_layers)
_transformer_layer = sc.broadcast(transformer_layer)


def gen_latent(trace_array):
    if trace_array is None:
        return None
    trace = np.expand_dims(
        np.stack(trace_array, axis=0), axis=0
    )  # 7, 9000 -> 1, 7, 9000
    if trace.shape != (1, 7, 9000) or not np.issubdtype(trace.dtype, np.number):
        return None
    trace = np.nan_to_num(trace)
    with torch.no_grad():
        data = torch.tensor(trace).double()
        latent = run_model(data)
        latent = latent.detach()
        if torch.cuda.is_available():
            latent = latent.cpu()
        try:
            latent = latent.flatten()
        except:
            raise ValueError(str(latent.dtype))

    return list(map(float, latent.tolist()))


def run_model(data):
    global _conv_layers
    global _transformer_layer
    conv = _conv_layers.value
    transformer = _transformer_layer.value

    with torch.no_grad():
        if torch.cuda.is_available():
            data = data.cuda()
            conv = conv.cuda()
            transformer = transformer.cuda()
        latent = conv(data)
        latent = torch.swapaxes(latent, 1, 2)
        latent = transformer(latent)
    return latent


gen_latent_udf = F.udf(gen_latent, ArrayType(FloatType()))


df = spark.table(SOURCE_TABLE)

org_ids = [row["org_id"] for row in df.select("org_id").distinct().collect()]
random.shuffle(org_ids)
n = int(len(org_ids) / NUM_BATCHES) + 1
org_id_batches = [org_ids[i : i + n] for i in range(0, len(org_ids), n)]

df = df.withColumn("latent", gen_latent_udf(df.trace_array))
df = df.withColumn("latent_model_id", F.lit(LATENT_MODEL_ID))
df = df.select(
    "date",
    "org_id",
    "event_id",
    "latent_model_id",
    "latent",
)
df = df.where(~df.latent[0].isNull())

for date in run_dates:
    for i, org_ids in enumerate(org_id_batches):
        print(f"Date: {date} batch: {i+1} of {NUM_BATCHES}")
        start = datetime.now()
        print(f"Starting at {start}...")
        df.where((df.date == date) & df.org_id.isin(org_ids)).write.partitionBy(
            "date"
        ).mode("overwrite").option(
            "replaceWhere",
            f"date = '{date}' AND org_id IN ({','.join(map(str, org_ids))})",
        ).saveAsTable(
            WRITE_TABLE
        )
        print(f"completed at {datetime.now()}...took {datetime.now() - start}")
