# Databricks notebook source

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/utils_calling_conventions

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/utils_dataset_write

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import col, regexp_extract, lit
import json

# COMMAND ----------
ARG_TILE_VERSION = "tile_version"
ARG_OSM_DATASET_VERSION = "osm_dataset_version"
ARG_OSM_REGIONS_VERSION_MAP = "osm_regions_versions"
ARG_TOMTOM_VERSION = "tomtom_version"
ARG_SPEED_LIMIT_DATASET_NAME = "speed_limit_dataset_name"
ARG_SPEED_LIMIT_DATASET_VERSION = "speed_limit_dataset_version"

"""
* ARG_TOMTOM_VERSION:
String tomtom version
"""
dbutils.widgets.text(ARG_TOMTOM_VERSION, "")
tomtom_version = dbutils.widgets.get(ARG_TOMTOM_VERSION)
print(f"{ARG_TOMTOM_VERSION}: {tomtom_version}")

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
* ARG_OSM_REGIONS_VERSION_MAP:
String osm regions to version
"""
dbutils.widgets.text(ARG_OSM_REGIONS_VERSION_MAP, "")
osm_regions_to_version_json = dbutils.widgets.get(ARG_OSM_REGIONS_VERSION_MAP)
osm_regions_to_version = json.loads(osm_regions_to_version_json)
print(f"{ARG_OSM_REGIONS_VERSION_MAP}: {osm_regions_to_version}")

"""
* ARG_SPEED_LIMIT_DATASET_NAME:
String speed limit dataset name
"""
dbutils.widgets.text(ARG_SPEED_LIMIT_DATASET_NAME, "")
speed_limit_dataset_name = dbutils.widgets.get(ARG_SPEED_LIMIT_DATASET_NAME)
print(f"{ARG_SPEED_LIMIT_DATASET_NAME}: {speed_limit_dataset_name}")

"""
* ARG_SPEED_LIMIT_DATASET_VERSION:
String speed limit dataset version
"""
dbutils.widgets.text(ARG_SPEED_LIMIT_DATASET_VERSION, "")
speed_limit_dataset_version = dbutils.widgets.get(ARG_SPEED_LIMIT_DATASET_VERSION)
print(f"{ARG_SPEED_LIMIT_DATASET_VERSION}: {speed_limit_dataset_version}")

# COMMAND ----------


def get_deleted_rows(resolved_limits_df):
    ds_limits_to_delete = (
        spark.table("deployed_global_limits_fw_tiles")
        .filter("data_source = 1 or data_source = 2")
        .alias("ds_limits")
        .join(
            resolved_limits_df.alias("resolved"),
            col("ds_limits.way_id") == col("resolved.osm_way_id"),
            how="leftanti",
        )
        .createOrReplaceTempView("ds_limits_to_delete")
    )
    ds_limits_to_delete = spark.sql(
        """
    select
    way_id as osm_way_id,
    null as tomtom_way_id,
    null as osm_maxspeed,
    null as tomtom_maxspeed,
    null as osm_highway,
    null as tomtom_commercial_vehicle_speed_map,
    null as tomtom_maxspeed_unit,
    null as tomtom_country_code,
    null as tomtom_state_code,
    0 as matched_h3_size,
    0 as osm_interp_h3_size,
    0 as tomtom_interp_h3_size,
    "0 mph" as osm_tomtom_updated_passenger_limit,
    true as should_upload_passenger_limit
    from ds_limits_to_delete
    """
    )
    print(f"adding rows for deleted limits: {ds_limits_to_delete.count()} rows")
    return ds_limits_to_delete


# COMMAND ----------
def evaluate_updated_limits_for_dataset(
    osm_version: str, region: str, tomtom_version: str
):
    # e.g. safety_map_data.osm_20240619__tomtom_202406__usa__map_match_decoupled
    map_match_table_name = f"{DBX_TABLE.osm_tomtom_map_match(osm_version, region, tomtom_version)}_decoupled"

    if not DBX_TABLE.is_table_exists(map_match_table_name):
        exit_notebook(f"Missing Map Matched Table {map_match_table_name}")

    map_match_df = spark.table(map_match_table_name)

    map_match_df = map_match_df.withColumn(
        "osm_vs_tomtom",
        osm_tomtom_speed_selector_udf(
            col("osm_maxspeed"), col("tomtom_maxspeed"), col("tomtom_maxspeed_unit")
        ),
    )
    map_match_df = map_match_df.withColumn(
        "osm_tomtom_updated_passenger_limit", col("osm_vs_tomtom.speed_limit")
    ).withColumn("data_source", col("osm_vs_tomtom.data_source"))
    map_match_df = map_match_df.drop("osm_vs_tomtom")

    map_match_df = map_match_df.withColumn(
        "should_upload_passenger_limit",
        should_upload_limit_udf(
            col("osm_maxspeed"),
            col("osm_raw_maxspeed"),
            col("osm_tomtom_updated_passenger_limit"),
        ),
    )
    map_match_df = map_match_df.withColumn(
        "tomtom_commercial_vehicle_speed_map",
        unitize_tomtom_commercial_speed_limits_udf(
            col("tomtom_maxspeed_unit"), col("tomtom_commercial_vehicle_speed_map")
        ),
    )

    # Only write updated limits
    map_match_df = map_match_df.filter(
        "should_upload_passenger_limit = true OR tomtom_commercial_vehicle_speed_map IS NOT NULL"
    )

    return map_match_df


# COMMAND ----------
"""
* Adding the date to the table name to avoid overwriting the previous day's table
e.g. osm_can_20240619_eur_20240619_mex_20240619_usa_20240619__tomtom_202406__resolved_speed_limits_decoupled
TODO: This is a temporary solution. We should use a more robust way to handle this.
NOTE: The current regulatory prod write is calling the resolved tabble without
the date, so we need to make sure this is consistent in the future.
"""
resolved_limits_table = f"{DBX_TABLE.osm_tomtom_resolved_speed_limits(osm_regions_to_version, tomtom_version)}_decoupled"
resolved_limits_tmp_table = f"{resolved_limits_table}_tmp"


# COMMAND ----------

if not DBX_TABLE.is_table_exists(resolved_limits_table):
    # First, compute the new rows and write to a temp table.
    for region, osm_version in osm_regions_to_version.items():
        print(f"evaluating {region}")
        map_match_df = evaluate_updated_limits_for_dataset(
            osm_version, region, tomtom_version
        )
        print(f"total rows: {map_match_df.count()}")
        map_match_df.write.mode("append").option("overwriteSchema", "True").saveAsTable(
            resolved_limits_tmp_table
        )
        # Copy the temp table to the final table
    print("copying data from tmp table to final table")
    temp_df = spark.table(resolved_limits_tmp_table)
    temp_df.write.mode("append").option("overwriteSchema", "True").saveAsTable(
        resolved_limits_table
    )

    # Compute the rows to delete
    limits_to_delete = get_deleted_rows(temp_df)
    limits_to_delete.write.mode("append").option("overwriteSchema", "True").saveAsTable(
        resolved_limits_table
    )
    # Drop the temp table.
    spark.sql(f"drop table if exists {resolved_limits_tmp_table}")

# COMMAND ----------

# Set the datasource to be TomTom
speed_limit_data_source = SpeedLimitDataSource.VENDOR_TOM_TOM
# TODO: make this as a variable that will be passed from Airflow, version=0 for now.
backend_df = create_decoupled_tomtom_backend_csv(
    resolved_limits_table,
    speed_limit_data_source,
    tile_version,
    speed_limit_dataset_version,
)
display(backend_df)

# COMMAND ----------

tilegen_df = create_decoupled_tomtom_tilegen_csv(resolved_limits_table)

# COMMAND ----------

persist_speed_limits_backend_csv(
    tilegen_df, tile_version, speed_limit_dataset_name, speed_limit_dataset_version
)

# COMMAND ----------


persist_speed_limits_tilegen_csv(
    tilegen_df, tile_version, speed_limit_dataset_name, speed_limit_dataset_version
)

# COMMAND ----------

exit_notebook()


# COMMAND ----------
