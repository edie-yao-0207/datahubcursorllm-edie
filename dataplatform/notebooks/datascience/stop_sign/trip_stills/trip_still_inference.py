# Databricks notebook source

# COMMAND ----------

# MAGIC %run "backend/datascience/stop_sign/trip_stills/ml_utils"

# COMMAND ----------

# MAGIC %run "/backend/datascience/stop_sign/utils"

# COMMAND ----------

from multiprocessing import cpu_count
from multiprocessing.dummy import Pool

import pyspark.sql.functions as F
import tensorflow as tf

# COMMAND ----------


MODEL = "rfcn_resnet101_coco_2018_01_28"
PREFIX = "s3://samsara-databricks-playground/stop_sign_detection/models"

MODEL_S3URL = f"{PREFIX}/{MODEL}/frozen_inference_graph.pb"

# COMMAND ----------


class TripStillInference:
    def __init__(self, city_name):
        self.city_name = city_name

    def run_inference(self, mode="overwrite", min_date_for_update="2017-01-01"):
        if mode == "ignore":
            return
        trip_stills_intersections = spark.table(
            f"stopsigns.trip_stills_intersections_{self.city_name}"
        )
        h3_res_3_index_list = (
            trip_stills_intersections.select("hex_id_res_3")
            .dropDuplicates()
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        print(f"h3 indices: {h3_res_3_index_list}")
        h3_table_name = f"stopsigns.{self.city_name}_trip_still_inference_index_tracker"
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
            print(f"Running inference for {h3_res_3_index}")
            data_sdf = spark.table(
                f"stopsigns.trip_stills_intersections_paths_{self.city_name}_h3_res_3_{h3_res_3_index}"
            )
            # filter date
            data_sdf_date_filtered = data_sdf.filter(
                F.col("date") >= min_date_for_update
            )
            detections = run_object_detection_model(
                data_sdf_date_filtered, model=MODEL_S3URL
            )
            write_table_with_date_partition(
                detections,
                f"stopsigns.trip_stills_inference_{self.city_name}_h3_res_3_{h3_res_3_index}",
                min_date_for_update=min_date_for_update,
            )
            finished_h3 = finished_h3 + [h3_res_3_index]
            write_h3_table(h3_table_name, finished_h3)
            print("Done")
        drop_h3_table(h3_table_name)

    def _write_table(self, write_sdf, table_name):
        write_sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            table_name
        )

    def label_inference(self):
        detections_sdf = spark.table(
            f"stopsigns.{self.city_name}_detected_intersections"
        )
        detections_sdf = detections_sdf.groupBy("way_id", "node_id", "bearing_dir").agg(
            F.collect_list(
                F.struct(F.col("stop_sign_confidence"), F.col("stop_sign_bbox"))
            ).alias("stop_sign_preds")
        )
        stop_signs_sdf_with_osm = detections_sdf.join(
            spark.table(f"stopsigns.{self.city_name}_stop_signs_osm"),
            ["way_id", "node_id", "bearing_dir"],
        )
        non_stop_signs_osm = detections_sdf.join(
            spark.table(f"stopsigns.{self.city_name}_non_stop_signs_osm"),
            ["way_id", "node_id", "bearing_dir"],
        )
        cols_to_select = [
            "way_id",
            "node_id",
            "bearing_dir",
            "bearing",
            "reverse_bearing",
            "latitude",
            "longitude",
            "stop_sign_preds",
        ]
        combined_df = stop_signs_sdf_with_osm.select(cols_to_select).unionByName(
            non_stop_signs_osm.select(cols_to_select)
        )
        write_table_no_partition(
            combined_df, f"stopsigns.{self.city_name}_trip_stills_inference_labeled"
        )
