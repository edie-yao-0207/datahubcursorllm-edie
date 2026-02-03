# Databricks notebook source
# MAGIC %run "backend/datascience/stop_sign/utils"

# COMMAND ----------

# MAGIC %run "backend/datascience/stop_sign/model/model_config"

# COMMAND ----------

# MAGIC %run "backend/datascience/stop_sign/heuristic_filters/close_stop_sign_identifier"

# COMMAND ----------

# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------

import boto3
import pyspark.sql.functions as F

# COMMAND ----------

model_config = ModelConfig()

tables_to_combine = [
    "first_quarter",
    "second_quarter_first",
    "second_quarter_second",
    "second_quarter_third",
    "second_quarter_fourth",
    "third_quarter",
    "fourth_quarter_first",
    "fourth_quarter_second",
    "fourth_quarter_third",
    "fourth_quarter_fourth",
]

tables_to_combine = ["us_continental_telemetry_" + t for t in tables_to_combine]

# Find last version and add one
file_in_s3_location = get_s3_client(
    "samsara-databricks-playground-read"
).list_objects_v2(
    Bucket="samsara-databricks-playground",
    Prefix=f"stop_sign_detection/output/us_continental_telemetry",
)[
    "Contents"
]

versions = [
    f["Key"].split("/")[3]
    for f in file_in_s3_location
    if "stop_locations_v" in f["Key"]
]
versions = [int(v[(v.index("v") + 1) :]) for v in versions]

version = max(versions) + 1

all_data = spark.table(f"stopsigns.combined_preds_{tables_to_combine[0]}_all")

cssi = CloseStopSignIdentifier(tables_to_combine[0], model_config.MODEL_THRESHOLD)
to_exclude = cssi.get_close_stops_to_remove()

for i in range(1, len(tables_to_combine)):
    all_data = all_data.unionByName(
        spark.table(f"stopsigns.combined_preds_{tables_to_combine[i]}_all")
    )
    # pull stops to remove for each region
    cssi = CloseStopSignIdentifier(tables_to_combine[i], model_config.MODEL_THRESHOLD)
    to_exclude = to_exclude.unionByName(cssi.get_close_stops_to_remove())

all_data = all_data.select(
    "way_id",
    "node_id",
    "bearing_dir",
    "latitude",
    "longitude",
    "bearing",
    "reverse_bearing",
    "hex_id_res_3",
    "is_ground_truth",
    "telemetry_conf",
    "histogram_s3_link",
    "trace_count",
    "trip_still_preds",
    "s3urls",
    "combined_prediction",
)

all_data.withColumn("file_version", F.lit(str(version)),).withColumn(
    "model_id",
    F.lit(model_config.MODEL_ID),
).write.format("delta").mode("append").partitionBy("file_version").saveAsTable(
    "stopsigns.combined_preds_us_continental_telemetry_all_with_file_version"
)

thresh = 16
core_sampled_data = (
    spark.read.csv(
        "dbfs:/mnt/samsara-databricks-playground/stop_signs/dh_pace_stop_signs.csv",
        header=True,
        inferSchema=True,
    )
    .filter(F.col("avg_speed") >= thresh)
    .select("way_id", "node_id", "avg_speed")
)
all_data_with_speed_filter = all_data.join(
    core_sampled_data, ["node_id", "way_id"], how="left"
).filter(F.col("avg_speed").isNull())
all_data = all_data_with_speed_filter.drop("avg_speed")
all_data = all_data.filter(F.col("combined_prediction") >= model_config.MODEL_THRESHOLD)

# Removing Known bad stop signs where one ground truth is associated with multiple intersections
# https://paper.dropbox.com/doc/Stop-Sign-Debugging-Notes--BHKr8c8BpHX~3dUZO4K~fsbkAg-ZeJ9RnEniFzahGpUTHhtC
all_data = all_data.filter(
    ~((F.col("node_id") == 65330470) & (F.col("way_id") == 286499161))
)
all_data = all_data.filter(
    ~(
        (F.col("node_id") == 65305544)
        & (F.col("way_id") == 255170236)
        & (F.col("bearing_dir") == -1)
    )
)
all_data = all_data.filter((F.col("node_id") != 6248840170))
all_data = all_data.filter((F.col("node_id") != 65315125))
all_data = all_data.filter((F.col("node_id") != 1735964624))
all_data = all_data.filter((F.col("node_id") != 65315304))
all_data = all_data.filter((F.col("node_id") != 5445996449))

csv_file_columns = [
    "way_id",
    "node_id",
    "bearing_dir",
    "latitude",
    "longitude",
    "bearing",
    "reverse_bearing",
    "combined_prediction",
]
predicted_stops = all_data.select(csv_file_columns)

predicted_stops = predicted_stops.join(
    to_exclude,
    [
        "node_id",
        "way_id",
        "bearing",
        "bearing_dir",
    ],
    "left",
)

# Write CSV For Eng
predicted_stops.select(csv_file_columns).coalesce(1).write.mode("overwrite").format(
    "csv"
).save(
    f"s3://samsara-databricks-playground/stop_sign_detection/output/us_continental_telemetry/stop_locations_v{version}",
    header="true",
)

# Writing for DS storage
predicted_stops = predicted_stops.withColumn(
    "file_version",
    F.lit(str(version)),
).withColumn(
    "model_id",
    F.lit(model_config.MODEL_ID),
)

# Write Predicted stop signs
predicted_stops_table_cols = csv_file_columns + ["file_version", "model_id"]
predicted_stops.where(F.col("removal_reasons").isNull()).select(
    predicted_stops_table_cols
).write.format("delta").mode("append").partitionBy("file_version").saveAsTable(
    "stopsigns.predicted_stopsigns_output"
)

# write excluded stop signs
predicted_stops_table_cols = csv_file_columns + ["file_version", "model_id"]
predicted_stops.where(F.col("removal_reasons").isNotNull()).write.format("delta").mode(
    "append"
).partitionBy("file_version").saveAsTable("stopsigns.excluded_stopsigns")

# combine CSV Files
file_in_s3_location = get_s3_client(
    "samsara-databricks-playground-read"
).list_objects_v2(
    Bucket="samsara-databricks-playground",
    Prefix=f"stop_sign_detection/output/us_continental_telemetry/stop_locations_v{version}",
)[
    "Contents"
]
for file in file_in_s3_location:
    cur_filename = file["Key"].split("/")[-1]
    if cur_filename.startswith("part-"):
        dbutils.fs.mv(
            f"dbfs:/mnt/samsara-databricks-playground/stop_sign_detection/output/us_continental_telemetry/stop_locations_v{version}/{cur_filename}",
            f"dbfs:/mnt/samsara-databricks-playground/stop_sign_detection/output/us_continental_telemetry/stop_locations_v{version}/us_preds_all_v{version}.csv",
        )

q = f"""
    WITH previous_version AS (
    SELECT
    DISTINCT
    node_id,
    way_id,
    bearing_dir
    FROM stopsigns.predicted_stopsigns_output
    WHERE file_version = '{version-1}'
    ),
    current_version AS (
    SELECT
    DISTINCT
    node_id,
    way_id,
    bearing_dir
    FROM stopsigns.predicted_stopsigns_output
    WHERE file_version = '{version}'
    )
    SELECT
    SUM(IF(cv.node_id IS NULL, 1, 0)) AS total_stopsigns_removed,
    SUM(IF(pv.node_id IS NULL, 1, 0)) AS total_stopsigns_added,
    SUM(IF(pv.node_id IS NOT NULL AND cv.node_id IS NOT NULL, 1, 0)) AS total_stopsigns_unchanged,
    SUM(IF(cv.node_id IS NOT NULL, 1, 0)) AS total_stopsigns,
    SUM(IF(cv.node_id IS NULL AND pv.node_id IS NULL, 1, 0)) AS both_null_stopsigns,
    '{model_config.json()}' AS model_config,
    '{version}' AS file_version,
    current_timestamp() AS end_ts
    FROM current_version cv
    FULL OUTER JOIN previous_version pv
    ON pv.way_id = cv.way_id
    AND pv.node_id = cv.node_id
    AND pv.bearing_dir = cv.bearing_dir
"""
spark.sql(q).write.format("delta").mode("append").saveAsTable(
    "stopsigns.file_summary_stats"
)
