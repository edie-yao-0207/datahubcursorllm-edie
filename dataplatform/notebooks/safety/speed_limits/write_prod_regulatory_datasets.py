# Databricks notebook source
# MAGIC %run /backend/safety/speed_limits/utils_calling_conventions

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/utils_dataset_write

# COMMAND ----------

ARG_TILE_VERSION = "tile_version"
ARG_OSM_DATASET_VERSION = "osm_dataset_version"
ARG_SPEED_LIMIT_DATASET_NAME = "speed_limit_dataset_name"
ARG_SPEED_LIMIT_DATASET_VERSION = "speed_limit_dataset_version"

"""
* ARG_TILE_VERSION:
Integer tile version
"""
dbutils.widgets.text(ARG_TILE_VERSION, "")
tile_version = dbutils.widgets.get(ARG_TILE_VERSION)
print(f"{ARG_TILE_VERSION}: {tile_version}")

"""
* ARG_OSM_DATASET_VERSION:
String osm dataset version
"""
dbutils.widgets.text(ARG_OSM_DATASET_VERSION, "")
osm_dataset_version = dbutils.widgets.get(ARG_OSM_DATASET_VERSION)
print(f"{ARG_OSM_DATASET_VERSION}: {osm_dataset_version}")

"""
* ARG_SPEED_LIMIT_DATASET_NAME:
String dataset name
"""
dbutils.widgets.text(ARG_SPEED_LIMIT_DATASET_NAME, "")
speed_limit_dataset_name = dbutils.widgets.get(ARG_SPEED_LIMIT_DATASET_NAME)
print(f"{ARG_SPEED_LIMIT_DATASET_NAME}: {speed_limit_dataset_name}")

"""
* ARG_SPEED_LIMIT_DATASET_VERSION:
Integer dataset version
"""
dbutils.widgets.text(ARG_SPEED_LIMIT_DATASET_VERSION, "")
speed_limit_dataset_version = dbutils.widgets.get(ARG_SPEED_LIMIT_DATASET_VERSION)
print(f"{ARG_SPEED_LIMIT_DATASET_VERSION}: {speed_limit_dataset_version}")

# COMMAND ----------

from enum import Enum
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_extract, lit

# COMMAND ----------


def find_latest_mapmatched_table_by_dataset_name(dataset_name: str) -> str:
    """
    Given a prefix, return the latest mapmatched table,
    e.g. "regulatory_iowadot_map_matched_" returns "regulatory_iowadot_map_matched_20241129"
    """
    tables_df = spark.sql("SHOW TABLES IN safety_map_data")
    pattern = (
        f"{SPEED_LIMIT_DATASET_NAME_TO_MAPMATCHED_TABLE_NAME[dataset_name]}(\\d{{8}})"
    )

    # Extract dates and filter relevant tables
    tables_with_dates_df = tables_df.withColumn(
        "date", regexp_extract(col("tableName"), pattern, 1)
    ).filter(col("date").isNotNull())

    # Order by date and get the latest table
    latest_table_df = tables_with_dates_df.orderBy(col("date").desc()).limit(1)

    latest_table_name = latest_table_df.collect()[0]["tableName"]
    return f"safety_map_data.{latest_table_name}"


# COMMAND ----------


def backfill_country_and_state_code_if_necessary(
    dataset_name: str, dataset_mapmatched_df, df: DataFrame
) -> DataFrame:
    """
    dataset_mapmatched_df doesn't have tomtom_country_code and tomtom_state_code, so we backfill the tomtom_country_code and tomtom_state_code for a dataset by finding the osm_way_ids from the mapmatched table
    """
    datasets_without_country_state_code = {
        "iowa_dot": {"tomtom_country_code": "USA", "tomtom_state_code": "IA"}
    }
    if dataset_name not in datasets_without_country_state_code:
        return df

    country_code = datasets_without_country_state_code[dataset_name][
        "tomtom_country_code"
    ]
    state_code = datasets_without_country_state_code[dataset_name]["tomtom_state_code"]

    # Filter rows where osm_way_id appears in dataset_mapmatched_df
    filtered_df = df.join(
        dataset_mapmatched_df.select("osm_way_id").distinct(),
        on="osm_way_id",
        how="inner",
    )

    # Backfill the country and state codes
    backfilled_df = filtered_df.withColumn(
        "tomtom_country_code", lit(country_code)
    ).withColumn("tomtom_state_code", lit(state_code))

    # Select rows that do not need backfilling
    non_backfilled_df = df.join(filtered_df, on="osm_way_id", how="left_anti")

    # Combine the non-backfilled rows with the backfilled rows
    final_df = non_backfilled_df.unionByName(backfilled_df)

    return final_df


# COMMAND ----------


def evaluate_max_speed_limits_for_dataset(
    dataset_name: str,
) -> DataFrame:
    """
    Join the dataset mapmatched table with tomtomosm resolved table to compute max(dataset, tomtom, osm) for every way_id
    """
    tomtom_osm_resolved_table_name = find_latest_tomtom_osm_resolved_speed_limit_table()
    dataset_mapmatched_table_name = find_latest_mapmatched_table_by_dataset_name(
        dataset_name
    )

    print(
        f"Joining {tomtom_osm_resolved_table_name} with {dataset_mapmatched_table_name}"
    )

    if not DBX_TABLE.is_table_exists(tomtom_osm_resolved_table_name):
        exit_notebook(f"Missing Map Matched Table {tomtom_osm_resolved_table_name}")
    if not DBX_TABLE.is_table_exists(dataset_mapmatched_table_name):
        exit_notebook(f"Missing Map Matched Table {dataset_mapmatched_table_name}")

    tomtom_osm_resolved_df = spark.table(tomtom_osm_resolved_table_name)
    dataset_mapmatched_df = spark.table(dataset_mapmatched_table_name)

    # Join the DataFrames on "osm_way_id"
    joined_df = tomtom_osm_resolved_df.join(
        dataset_mapmatched_df, on="osm_way_id", how="outer"
    )

    # Calculate the maximum speed limit
    max_speed_limit_df = joined_df.withColumn(
        "tomtom_osm_vs_regulatory",
        select_max_speed_limit_amongst_osm_tomtom_decoupled_speed_limits_udf(
            lit(dataset_name),
            col("speed_limit_milliknots"),
            col("data_source"),
            col("osm_tomtom_updated_passenger_limit"),
        ),
    )

    max_speed_limit_df = max_speed_limit_df.withColumn(
        "max_speed_limit", col("tomtom_osm_vs_regulatory.speed_limit")
    ).withColumn("data_source", col("tomtom_osm_vs_regulatory.data_source"))
    max_speed_limit_df = max_speed_limit_df.drop("tomtom_osm_vs_regulatory")

    extracted_df = max_speed_limit_df.select(
        "osm_way_id",
        "max_speed_limit",
        "data_source",
        "tomtom_commercial_vehicle_speed_map",
        "tomtom_country_code",
        "tomtom_state_code",
    )

    result_df = backfill_country_and_state_code_if_necessary(
        dataset_name, dataset_mapmatched_df, extracted_df
    )

    return result_df


# COMMAND ----------

"""
    Main logic starts from here,
    1. we evaluate the max speed limits for the given dataset
        - find the latest tomtom_osm_resolved_speed_limits table
        - find the latest mapmatched table for the given regulatory dataset
        - then find the max(tomtom, osm, dataset) for every way_id
    2. store the result as a dataset resolved table to be used in the next steps
        - e.g. safety_map_data.regulatory_iowadot_resolved_speed_limits_20250206
    3. find the latest deployed speed limit value on every wayid that currently lives inside speedlimitsdb
    4. create the backend csv - this only contains the delta to write into speedlimitsdb
    5. create the tilegen csv - this contains all data to generate speed limit tiles & client tiles
    6. persist the backend csv on S3
    7. persist the tilegen csv on S3
"""
today_date = datetime.today().strftime("%Y%m%d")
resolved_limits_table_name = f"safety_map_data.{SPEED_LIMIT_DATASET_NAME_TO_RESOLVED_TABLE_NAME[speed_limit_dataset_name]}{today_date}"

max_speed_limit_df = evaluate_max_speed_limits_for_dataset(speed_limit_dataset_name)
print(f"Total rows with max(dataset, tomtom, osm): {max_speed_limit_df.count()}")

max_speed_limit_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(resolved_limits_table_name)
print(f"Done storing the table {resolved_limits_table_name} on S3")

# COMMAND ----------

# Find out the latest speed limit value on every wayid that lives inside speedlimitsdb
create_deployed_speed_limits_fw_tiles(
    SPEED_LIMIT_DATASET_NAME_TO_DB_ORG_ID[speed_limit_dataset_name]
)
backend_df = create_speed_limits_backend_csv(
    resolved_limits_table_name,
    speed_limit_dataset_name,
    tile_version,
    speed_limit_dataset_version,
)

# COMMAND ----------

tilegen_df = create_speed_limits_tilegen_csv(resolved_limits_table_name)

# COMMAND ----------

# For storing the records in speedlimitsdb, we use tile_version and speed_limit_dataset_version
persist_speed_limits_backend_csv(
    backend_df, speed_limit_dataset_name, tile_version, speed_limit_dataset_version
)

# COMMAND ----------

# For storing the records in speed limit tiles & client tiles, we use tile_version and speed_limit_dataset_version beause we store them on S3
persist_speed_limits_tilegen_csv(
    tilegen_df, speed_limit_dataset_name, tile_version, speed_limit_dataset_version
)

# COMMAND ----------

exit_notebook()
