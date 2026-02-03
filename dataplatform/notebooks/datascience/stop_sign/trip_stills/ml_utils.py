# Databricks notebook source

# COMMAND ----------

# MAGIC %run "backend/datascience/stop_sign/utils"

# COMMAND ----------

import contextlib
import io
import json
import math
from multiprocessing import cpu_count
from multiprocessing.dummy import Pool
import os
import re
import sys
import types
from typing import Iterator

from PIL import Image
import numpy as np
import pandas as pd
from pyspark.sql import Row
from pyspark.sql.functions import PandasUDFType, col, from_json, pandas_udf, udf
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
import tensorflow as tf

# DBTITLE 1,Image Helpers
import contextlib2
import matplotlib.pyplot as plt
from skimage.io import imread, imshow

# COMMAND ----------


MNETv1_INPUT_TENSOR = "image_tensor:0"
MNET_OUTPUT_TENSORS = [
    "num_detections",
    "detection_boxes",
    "detection_scores",
    "detection_classes",
]
PRETRAINED_MOBILENET_COCO_CKPT_S3URL = "s3://samsara-datasets/samsara_hand_actions/ssd_mobilenet_v1_coco_2018_01_28/frozen_inference_graph.pb"


@contextlib.contextmanager
def options(options):
    old_opts = tf.config.optimizer.get_experimental_options()
    tf.config.optimizer.set_experimental_options(options)
    try:
        yield
    finally:
        tf.config.optimizer.set_experimental_options(old_opts)


_bc_model = None


def _broadcast_model(ckpt_s3url: str):
    tf.reset_default_graph()
    with tf.Session() as sess:
        detection_graph = tf.Graph()
        with detection_graph.as_default():
            od_graph_def = tf.GraphDef()
            with tf.gfile.GFile(map_s3_path_to_dbfs(ckpt_s3url), "rb") as fid:
                serialized_graph = fid.read()
                od_graph_def.ParseFromString(serialized_graph)
                tf.import_graph_def(od_graph_def, name="")
            global _bc_model
            _bc_model = sc.broadcast(od_graph_def)
            return _bc_model


# COMMAND ----------

# DBTITLE 1,Tensorflow <-> PySpark


def run_object_detection_model_batch(
    s3urls_batch, sess, tensor_dict, image_tensor, output_tensors
):
    """
    UDF that runs a model exported from the object detection API
    expects broadcast_model to have been called, with the handle
    to the broadcasted model stored in a variable named `bc_model`
    in the executing notebook's state.
    :param image_batch: A spark column, e.g. `col("s3url")`
    """
    batch_size = 32
    image_input = tf.placeholder(dtype=tf.string, shape=(None,))
    dataset = (
        tf.data.Dataset.from_tensor_slices(image_input)
        .prefetch(10000)
        .map(
            lambda imgpath: tf.image.resize(
                tf.io.decode_image(
                    tf.io.read_file(imgpath), channels=3, expand_animations=False
                ),
                (1080, 1920),
            ),
            num_parallel_calls=1200,
        )
        .batch(batch_size)
    )
    iterator = dataset.make_initializable_iterator()
    image_batch = iterator.get_next()

    result = []
    sess.run(iterator.initializer, feed_dict={image_input: s3urls_batch})
    optimize_options = {
        "constant_folding": True,
        "arithmetic": True,
        "layout": True,
        "remapper": True,
        "memory": True,
        "dependency": True,
        "pruning": True,
        "function": True,
        "shape": True,
        "autoparallel": True,
        "loop": True,
        "scoped_allocator": True,
        "pin_to_host": True,
        "auto_mixed_precision": True,
        "debug_stripper": True,
    }
    with options(optimize_options):
        try:
            while True:
                this_batch = sess.run(image_batch)
                preds = sess.run(tensor_dict, feed_dict={image_tensor: this_batch})
                for i in range(this_batch.shape[0]):
                    # Move to json because ndarray doesn't pickle
                    res = {}
                    if "num_detections" in output_tensors:
                        n = int(preds["num_detections"][i])
                        res["num_detections"] = n
                        res.update(
                            {
                                k: preds[k][i][:n].tolist()
                                for k in output_tensors
                                if k != "num_detections"
                            }
                        )
                    else:
                        res = {k: preds[k][i].tolist() for k in output_tensors}
                    result.append(json.dumps(res))
        except tf.errors.OutOfRangeError as e:
            pass

    return pd.Series(result)


@pandas_udf(StringType(), PandasUDFType.SCALAR_ITER)
def pandas_udf_iterator_run_object_detection_model_batch(
    s3urls_batch_iter: Iterator[pd.Series],
) -> Iterator[pd.Series]:
    input_tensor = MNETv1_INPUT_TENSOR
    output_tensors = MNET_OUTPUT_TENSORS
    tf.reset_default_graph()

    with tf.Session() as sess:
        sess.run(tf.global_variables_initializer())
        global _bc_model
        graph_def = _bc_model.value
        tf.graph_util.import_graph_def(graph_def, name="")
        ops = tf.get_default_graph().get_operations()
        all_tensor_names = {output.name for op in ops for output in op.outputs}
        image_tensor = tf.get_default_graph().get_tensor_by_name(input_tensor)
        keys = output_tensors
        tensor_dict = {}
        for key in keys:
            tensor_name = key + ":0"
            if tensor_name in all_tensor_names:
                tensor_dict[key] = tf.get_default_graph().get_tensor_by_name(
                    tensor_name
                )

        for s3urls_batch in s3urls_batch_iter:
            with Pool(cpu_count() * 32) as p:
                exists = list(p.imap(s3url_exists, s3urls_batch))
            kept_indices = [i for i in range(len(s3urls_batch)) if exists[i]]
            s3urls_filtered_batch = [
                s3url for i, s3url in enumerate(s3urls_batch) if i in kept_indices
            ]
            results = list(
                run_object_detection_model_batch(
                    s3urls_filtered_batch,
                    sess,
                    tensor_dict,
                    image_tensor,
                    output_tensors,
                )
            )
            all_res = []
            for i in range(len(s3urls_batch)):
                if i in kept_indices:
                    all_res.append(results[0])
                    if len(results) > 1:
                        results = results[1:]
                else:
                    all_res.append(None)
            yield pd.Series(all_res)


def run_object_detection_model(df, model: str = PRETRAINED_MOBILENET_COCO_CKPT_S3URL):
    """
    Runs an object-detection API model (frozen_inference_graph.pb) on a list of s3urls
    :param s3urls: list of s3urls in the form s3://bucket/.../key
    :param model: s3 path to the model s3://bucket/.../frozen_inference_graph.pb

    With a mobilenet-based network on a warm-(ish) cluster, we see throughput of:
      num_images    |     elapsed time
      --------------+-----------------
      3             |      24.30s
      128           |      31.55s
      1024          |      40.25s
      8192          |     106.83s
    So, expect roughly ~70FPS with a moderately-warm cluster (i3.xlarge instances).

    Returns a dataframe with structure
      s3url:string
      object_detection:struct
        num_detections:float
        detection_boxes:array
          element:array
            element:float
        detection_scores:array
          element:float
        detection_classes:array
          element:float
    """
    # Batch
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "16000")
    # see if we need to do this mapping
    _broadcast_model(model)
    repart_size = int(df.count() / 10000)
    if repart_size == 0:
        repart_size = 1
    df = df.repartition(repart_size)
    print("Finished Repartitioning, Running Inference...")
    df = df.withColumn(
        "det_json",
        pandas_udf_iterator_run_object_detection_model_batch(col("dbfs_path")),
    )
    det_schema = StructType(
        [
            StructField("num_detections", IntegerType()),
            StructField("detection_boxes", ArrayType(ArrayType(FloatType()))),
            StructField("detection_scores", ArrayType(FloatType())),
            StructField("detection_classes", ArrayType(FloatType())),
        ]
    )
    df = df.withColumn("object_detection", from_json(df["det_json"], det_schema))
    return df.select("dbfs_path", "object_detection", "date")
