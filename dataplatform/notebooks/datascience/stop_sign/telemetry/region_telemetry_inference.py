# Databricks notebook source

# COMMAND ----------

# MAGIC %run "/backend/datascience/stop_sign/utils"

# COMMAND ----------

import glob
import os

from PIL import Image
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.functions import PandasUDFType, col, from_json, pandas_udf, udf
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from sklearn import metrics

import matplotlib.pyplot as plt
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset
from torchvision import datasets, transforms, utils
import torchvision.models as models

# COMMAND ----------

torch.cuda.empty_cache()

# COMMAND ----------

IMAGENET_MEANS = [0.485, 0.456, 0.406]
IMAGENET_STDDEV = [0.229, 0.224, 0.225]
img_transform = transforms.Compose(
    [
        transforms.Resize((224, 224)),
        transforms.ToTensor(),
        transforms.Normalize(mean=IMAGENET_MEANS, std=IMAGENET_STDDEV),
    ]
)

# COMMAND ----------

# Enable Arrow support.
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "1000000")

# COMMAND ----------


class TelemetryDataset(Dataset):
    def __init__(self, image_paths, transform=None):
        self.image_paths = image_paths
        self.transform = transform

    def __getitem__(self, index):
        x = Image.open(self.image_paths[index]).convert("RGB")
        if self.transform is not None:
            x = self.transform(x)

        return x, self.image_paths[index]

    def __len__(self):
        return len(self.image_paths)


# COMMAND ----------

USE_CUDA = torch.cuda.is_available()

# COMMAND ----------

PATH = "/dbfs/mnt/samsara-databricks-playground/stop_sign_detection/stop_sign_combined_dataset_pytorch_model.pth"
model = models.vgg19(pretrained=True)
model.load_state_dict(torch.load(PATH))
model.eval()
if USE_CUDA:
    model = model.cuda()

# COMMAND ----------


def run_inference(image_paths):
    intersection_results = []
    hist_data = TelemetryDataset(image_paths, transform=img_transform)
    # 4096, 32
    testloader = torch.utils.data.DataLoader(hist_data, batch_size=64, num_workers=32)
    for i, data in enumerate(testloader, 0):
        inputs, paths = data
        if USE_CUDA:
            inputs = inputs.cuda()
        # forward pass
        outputs = model(inputs)

        _, predicted = torch.max(outputs, 1)
        # get probabilities
        sm = torch.nn.Softmax()
        probabilities = sm(outputs)
        data_probs = [(1 - x[0]) for x in probabilities.detach().cpu().numpy()]
        intersection_results = intersection_results + data_probs
    return pd.Series(intersection_results)


@pandas_udf(FloatType(), PandasUDFType.SCALAR_ITER)
def run_inference_iter(image_paths_iter):
    for image_paths in image_paths_iter:
        yield run_inference(image_paths)


# COMMAND ----------


class RegionTelemetryInference:
    def __init__(self, city_name, hist_limit=100):
        self.city_name = city_name
        self.hist_limit = hist_limit

    def run_and_save(self, mode="overwrite"):
        if mode == "ignore":
            return
        data_sdf = spark.table(f"stopsigns.{self.city_name}_telemetry_data")
        h3_res_3_index_list = (
            data_sdf.select("hex_id_res_3")
            .dropDuplicates()
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        print(h3_res_3_index_list)
        h3_table_name = f"stopsigns.{self.city_name}_telem_inference_index_tracker"
        finished_h3 = []
        if table_exists(h3_table_name):
            finished_h3 = (
                spark.table(h3_table_name)
                .select("h3_index")
                .rdd.flatMap(lambda x: x)
                .collect()
            )
        write_h3_table(h3_table_name, finished_h3)
        for h3_res_3_index in h3_res_3_index_list:
            if h3_res_3_index in finished_h3:
                continue
            try:
                image_df = spark.table(
                    f"stopsigns.{self.city_name}_histogram_paths_{self.hist_limit}_{h3_res_3_index}"
                )
            except:  # dataframe doesn't exist
                continue
            repart_size = int(image_df.count() / 10000)
            if repart_size == 0:
                repart_size = 1
            print(f"Repartitioning")
            image_df = image_df.repartition(repart_size)
            print(f"Starting inference for {h3_res_3_index}")
            image_df = image_df.withColumn(
                "probability_of_stop_sign", run_inference_iter(F.col("path"))
            )
            image_df.write.mode("overwrite").option(
                "overwriteSchema", "true"
            ).saveAsTable(
                f"stopsigns.{self.city_name}_inference_{self.hist_limit}_{h3_res_3_index}"
            )
            finished_h3 = finished_h3 + [h3_res_3_index]
            write_h3_table(h3_table_name, finished_h3)
            print("Done")
        drop_h3_table(h3_table_name)


# COMMAND ----------
