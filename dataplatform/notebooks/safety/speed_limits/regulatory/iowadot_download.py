# Databricks notebook source
# MAGIC %md
# MAGIC # Dataset Download

# COMMAND ----------

from typing import List, Tuple
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import (
    col,
    size,
    lit,
    concat_ws,
)


# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/regulatory/network_utils

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/regulatory/dataset_utils

# COMMAND ----------

# DBTITLE 1,test locally
# MAGIC %run /backend/safety/speed_limits/map_match_algorithm

# COMMAND ----------

# DBTITLE 1,calling the helper to download in batches
# IOWA API enpoints, 2 steps
# 1. fetch the total count first
# 2. fetch the dataset in batches using the total count
# Overall Reference: https://data.iowadot.gov/datasets/f07494c9bc6048d8a34c50af400f2264/

COUNT_URL = "https://gis.iowadot.gov/agshost/rest/services/RAMS/Road_Network/FeatureServer/0/query?where=1%3D1&outFields=*&returnCountOnly=true&outSR=4326&f=json"
total_count = get_total_count_from_arcgis_dataset_url(COUNT_URL)
print(f"Claimed dataset size: {total_count} rows")

BASE_URL = "https://gis.iowadot.gov/agshost/rest/services/RAMS/Road_Network/FeatureServer/0/query?where=1%3D1&outFields=ROUTEID,FROMMEASURE,TOMEASURE,SPEED_LIMIT,EDITDATE&outSR=4326&f=json"

fetch_results = fetch_dataset_in_batches(BASE_URL, total_count, 5, 0)

print(f"Real dataset size: {len(fetch_results)} rows")

spark = SparkSession.builder.getOrCreate()
raw_json_df = spark.createDataFrame(fetch_results)

# COMMAND ----------

# Extracting the columns we need
# for IOWA, dataset_way_id is the concatenation of ROUTEID, FROMMEASURE, TOMEASURE with the separator "_"
dataset_df = (
    raw_json_df.select(
        concat_ws(
            "_",
            col("attributes.ROUTEID"),
            col("attributes.FROMMEASURE"),
            col("attributes.TOMEASURE"),
        ).alias("dataset_way_id"),
        col("attributes.SPEED_LIMIT").alias("raw_speed_limit"),
        lit("mph").alias("raw_speed_limit_unit"),
        convert_speed_limit_to_milliknots_udf(
            col("raw_speed_limit"), col("raw_speed_limit_unit")
        ).alias("speed_limit_milliknots"),
        col("attributes.EDITDATE").alias("edit_date"),
        flatten_nested_coordinates_udf(col("geometry.paths")).alias("coordinates"),
    )
    .filter(col("raw_speed_limit").isNotNull())
    .filter(size(col("coordinates")) > 0)
)

# COMMAND ----------

today_date = datetime.today().strftime("%Y%m%d")
table_name_raw = f"safety_map_data.regulatory_iowadot_raw_{today_date}"
dataset_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(table_name_raw)
print(f"Raw table {table_name_raw} is now written on S3")

# COMMAND ----------

# DBTITLE 1,invoke the helper func to interpolate and map match
dataset_df = spark.table(
    table_name_raw
)  # read the table from S3 to avoid re-running the UDFs above during map-matching
dataset_osm_matching_result, matched_ways = do_interpolate_and_map_match(
    dataset_df, ["usa"]
)

# COMMAND ----------

# This table is for FS team to use
table_name_mapmatched_with_date = (
    f"safety_map_data.regulatory_iowadot_map_matched_{today_date}"
)
table_name_matchedways_with_date = (
    f"safety_map_data.regulatory_iowadot_matched_ways_{today_date}"
)

dataset_osm_matching_result.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(table_name_mapmatched_with_date)

matched_ways.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(table_name_matchedways_with_date)

print(f"Map-matched table {table_name_mapmatched_with_date} is now written on S3")
print(f"Matched ways table {table_name_matchedways_with_date} is now written on S3")
