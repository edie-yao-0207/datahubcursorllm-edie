# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %run "backend/datascience/stop_sign/utils"

# COMMAND ----------

stop_sign_class = 13


@udf("integer")
def get_stop_sign_index(detections):
    return detections.index(13)


@udf("double")
def get_stop_sign_conf(confidences, stop_sign_index):
    return confidences[stop_sign_index]


@udf("array<double>")
def get_stop_sign_bbox(bboxes, stop_sign_index):
    return bboxes[stop_sign_index]


# COMMAND ----------


class TripStillsInferenceProcessor:
    def __init__(self, city_name):
        self.city_name = city_name

    def process_inference(self, mode="overwrite", min_date_for_update="2017-01-01"):
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
        h3_table_name = (
            f"stopsigns.{self.city_name}_trip_still_inference_processor_index_tracker"
        )
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
            print(f"Processing inference for {h3_res_3_index}")
            stop_sign_detections_raw = spark.table(
                f"stopsigns.trip_stills_inference_{self.city_name}_h3_res_3_{h3_res_3_index}"
            ).dropna(subset=["object_detection"])
            # Filter by date
            stop_sign_detections_date_filtered = stop_sign_detections_raw.filter(
                F.col("date") >= min_date_for_update
            )
            stop_sign_detections_exploded = stop_sign_detections_date_filtered.withColumn(
                "detections_exploded",
                F.explode(
                    stop_sign_detections_date_filtered.object_detection.detection_classes
                ),
            )
            stop_sign_detections_exploded = stop_sign_detections_exploded.filter(
                F.col("detections_exploded") == 13
            )
            stop_sign_detections_grouped = (
                stop_sign_detections_exploded.groupBy("dbfs_path", "object_detection")
                .agg(F.count("dbfs_path"))
                .drop("count(dbfs_path)")
            )
            stop_sign_detections = stop_sign_detections_grouped.withColumn(
                "stop_sign_index",
                get_stop_sign_index(
                    stop_sign_detections_grouped.object_detection.detection_classes
                ),
            )
            stop_sign_detections = stop_sign_detections.withColumn(
                "stop_sign_confidence",
                get_stop_sign_conf(
                    stop_sign_detections.object_detection.detection_scores,
                    F.col("stop_sign_index"),
                ),
            )
            stop_sign_detections = stop_sign_detections.withColumn(
                "stop_sign_bbox",
                get_stop_sign_bbox(
                    stop_sign_detections.object_detection.detection_boxes,
                    F.col("stop_sign_index"),
                ),
            )
            stop_sign_detections = stop_sign_detections.withColumn(
                "length", F.length(F.col("dbfs_path"))
            )
            stop_sign_detections = stop_sign_detections.withColumn(
                "s3url",
                F.concat(
                    F.lit("s3://"),
                    F.col("dbfs_path").substr(F.lit(11), F.col("length") - 10),
                ),
            )
            stop_sign_intersections = stop_sign_detections.join(
                trip_stills_intersections, "s3url"
            )
            stop_sign_intersections = stop_sign_intersections.withColumn(
                "length", F.length("s3url")
            ).withColumn(
                "<img>",
                F.concat(
                    F.lit("https://s3.internal.samsara.com/s3/"),
                    F.col("s3url").substr(F.lit(5), F.col("length") - F.lit(4)),
                ),
            )
            write_table_with_date_partition(
                stop_sign_intersections,
                f"stopsigns.{self.city_name}_trip_stills_detected_stop_signs_h3_res_3_{h3_res_3_index}",
                min_date_for_update,
            )
            finished_h3 = finished_h3 + [h3_res_3_index]
            write_h3_table(h3_table_name, finished_h3)
            print("Done")
        drop_h3_table(h3_table_name)
