# Databricks notebook source
from typing import List, Tuple
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import (
    col,
    size,
    lit,
    concat_ws,
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/regulatory/network_utils

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/regulatory/dataset_utils

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/map_match_algorithm

# COMMAND ----------

# NA API enpoints, 2 steps
# 1. fetch the total count first
# 2. fetch the dataset in batches using the total count
# Overall Reference: https://geodata.bts.gov/datasets/usdot::north-american-roads/api

COUNT_URL = "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_North_American_Roads/FeatureServer/0/query?where=1%3D1&outFields=*&returnCountOnly=true&outSR=4326&f=json"
total_count = get_total_count_from_arcgis_dataset_url(COUNT_URL)
print(f"Claimed dataset size: {total_count} rows")

BASE_URL = "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_North_American_Roads/FeatureServer/0/query?where=1%3D1&outFields=ID,SPEEDLIM,JURISNAME,ROADNAME&outSR=4326&f=json"

fetch_results = fetch_dataset_in_batches(
    BASE_URL, total_count, 5, 1000
)  # rate-limit is unknown on document, but it's around 1000ms during exploration

print(f"Real dataset size: {len(fetch_results)} rows")

# Create dataframe
schema = StructType(
    [
        StructField(
            "attributes",
            StructType(
                [
                    StructField("ID", StringType(), True),
                    StructField("SPEEDLIM", StringType(), True),
                    StructField("JURISNAME", StringType(), True),
                    StructField("ROADNAME", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "geometry",
            StructType(
                [
                    StructField(
                        "paths", ArrayType(ArrayType(ArrayType(DoubleType()))), True
                    )
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
dataset_df = (
    raw_json_df.select(
        col("attributes.ID").alias("dataset_way_id"),
        col("attributes.SPEEDLIM").alias("raw_speed_limit"),
        lit("kph").alias("raw_speed_limit_unit"),
        convert_speed_limit_to_milliknots_udf(
            col("raw_speed_limit"), col("raw_speed_limit_unit")
        ).alias("speed_limit_milliknots"),
        flatten_nested_coordinates_udf(col("geometry.paths")).alias("coordinates"),
    )
    .filter(col("raw_speed_limit").isNotNull())
    .filter(size(col("coordinates")) > 0)
)

# COMMAND ----------

today_date = datetime.today().strftime("%Y%m%d")
table_name_raw = f"safety_map_data.regulatory_nadot_raw_{today_date}"
dataset_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(table_name_raw)
print(f"Raw table {table_name_raw} is now written on S3")

# COMMAND ----------

# MAGIC %md
# MAGIC # TODO: Map Matching with OSM wayids

# COMMAND ----------

dataset_df = spark.table(table_name_raw)
dataset_osm_matching_result, matched_ways = do_interpolate_and_map_match(
    dataset_df, ["usa", "mex", "can"]
)

# COMMAND ----------

# This table is for FS team to use
table_name_mapmatched_with_date = (
    f"safety_map_data.regulatory_nadot_map_matched_{today_date}"
)
table_name_matchedways_with_date = (
    f"safety_map_data.regulatory_nadot_matched_ways_{today_date}"
)

dataset_osm_matching_result.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(table_name_mapmatched_with_date)

matched_ways.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(table_name_matchedways_with_date)

print(f"Map-matched table {table_name_mapmatched_with_date} is now written on S3")
print(f"Matched ways table {table_name_matchedways_with_date} is now written on S3")
