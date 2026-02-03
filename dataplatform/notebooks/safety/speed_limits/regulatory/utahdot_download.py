# Databricks notebook source
# MAGIC %md
# MAGIC # Dataset Download

# COMMAND ----------

from typing import List, Tuple
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import (
    col,
    lit,
    array,
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/regulatory/network_utils

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/regulatory/dataset_utils

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/map_match_algorithm

# COMMAND ----------

# Utah dataset API enpoints, 2 steps
# 1. fetch the total count first
# 2. fetch the dataset in batches using the total count
# Overall Reference: https://data-uplan.opendata.arcgis.com/datasets/uplan::speed-limit-signs-1/api

COUNT_URL = "https://maps.udot.utah.gov/central/rest/services/Assets/FI_Signs/MapServer/6/query?where=1%3D1&outFields=*&returnCountOnly=true&outSR=4326&f=json"
total_count = get_total_count_from_arcgis_dataset_url(COUNT_URL)

print(f"Claimed dataset size: {total_count} rows")

BASE_URL = "https://maps.udot.utah.gov/central/rest/services/Assets/FI_Signs/MapServer/6/query?where=1%3D1&outFields=ID,LEGEND,COLL_DATE&outSR=4326&f=json"

fetch_results = fetch_dataset_in_batches(BASE_URL, total_count, 5, 0)

print(f"Real dataset size: {len(fetch_results)} rows")

# Create dataframe
schema = StructType(
    [
        StructField(
            "attributes",
            StructType(
                [
                    StructField("ID", StringType(), True),
                    StructField("LEGEND", StringType(), True),
                    StructField("COLL_DATE", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "geometry",
            StructType(
                [
                    StructField("x", DoubleType(), True),
                    StructField("y", DoubleType(), True),
                ]
            ),
            True,
        ),
    ]
)
spark = SparkSession.builder.getOrCreate()
raw_json_df = spark.createDataFrame(fetch_results, schema=schema)

# COMMAND ----------

# Extracting the columns we need
# for Utah, dataset_way_id is just the ID
dataset_df = (
    raw_json_df.select(
        col("attributes.ID").alias("dataset_way_id"),
        col("attributes.LEGEND").alias("raw_speed_limit"),
        lit("mph").alias("raw_speed_limit_unit"),
        convert_speed_limit_to_milliknots_udf(
            col("raw_speed_limit"), col("raw_speed_limit_unit")
        ).alias("speed_limit_milliknots"),
        col("attributes.COLL_DATE").alias("edit_date"),
        array(col("geometry.x"), col("geometry.y")).alias("coordinate"),
    )
    .filter(col("raw_speed_limit").isNotNull())
    .filter(col("coordinate").isNotNull())
)

# COMMAND ----------

today_date = datetime.today().strftime("%Y%m%d")
table_name_raw = f"safety_map_data.regulatory_utahdot_raw_{today_date}"
dataset_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(table_name_raw)
print(f"Raw table {table_name_raw} is now written on S3")

# COMMAND ----------

dataset_df = spark.table(
    table_name_raw
)  # Load the table from S3 to avoid re-running the UDFs above during map-matching
dataset_osm_matching_result, matched_ways = do_map_match(dataset_df, ["usa"])

# COMMAND ----------

# This table is for FS team to use
table_name_mapmatched_with_date = (
    f"safety_map_data.regulatory_utahdot_map_matched_{today_date}"
)
table_name_matchedways_with_date = (
    f"safety_map_data.regulatory_utahdot_matched_ways_{today_date}"
)
dataset_osm_matching_result.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(table_name_mapmatched_with_date)

matched_ways.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(table_name_matchedways_with_date)

print(f"Map-matched table {table_name_mapmatched_with_date} is now written on S3")
print(f"Matched ways table {table_name_matchedways_with_date} is now written on S3")
