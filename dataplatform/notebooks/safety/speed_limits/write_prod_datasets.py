# Databricks notebook source
# MAGIC %run /backend/backend/databricks_data_alerts/alerting_system

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/utils_calling_conventions

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/utils_dataset_write

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/map_match_utils

# COMMAND ----------

"""
This notebook takes in the following args:
* ARG_OSM_REGIONS_VERSION_MAP:
JSON dict from region -> version_id
"""
dbutils.widgets.text(ARG_OSM_REGIONS_VERSION_MAP, "")
osm_regions_to_version = dbutils.widgets.get(ARG_OSM_REGIONS_VERSION_MAP)
if len(osm_regions_to_version) > 0:
    osm_regions_to_version = json.loads(osm_regions_to_version)
else:
    osm_regions_to_version = {}
print(f"{ARG_OSM_REGIONS_VERSION_MAP}: {osm_regions_to_version}")

"""
* ARG_TOMTOM_VERSION:
Must be specified in YYMM000 format
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
* ARG_TOMTOM_DATASET_VERSION:
TomTom dataset version for incremental releases (default: 0)
"""
dbutils.widgets.text(ARG_TOMTOM_DATASET_VERSION, "0")
tomtom_dataset_version = int(dbutils.widgets.get(ARG_TOMTOM_DATASET_VERSION))
print(f"{ARG_TOMTOM_DATASET_VERSION}: {tomtom_dataset_version}")


# COMMAND ----------

if len(osm_regions_to_version) == 0:
    exit_notebook("osm regions to version map must be specified")
if len(tomtom_version) == 0:
    exit_notebook("tomtom version must be specified")
if len(tile_version) == 0:
    exit_notebook("tile version must be specified")

# COMMAND ----------

# TODO Validate that we won't be overwriting any data for this tile version.
config.setCurrentPendingVersion(tile_version)

# COMMAND ----------

from pyspark.sql.functions import col


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


def evaluate_updated_limits_for_dataset(
    osm_version: str, region: str, tomtom_version: str
):
    map_match_table_name = DBX_TABLE.osm_tomtom_map_match(
        osm_version, region, tomtom_version
    )

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

# Spark doesn't like it when we append to the same table currently being read from.
# Write to a temp table, then copy the data to the final output table.
resolved_limits_table = DBX_TABLE.osm_tomtom_resolved_speed_limits(
    osm_regions_to_version, tomtom_version
)
resolved_limits_tmp_table = f"{resolved_limits_table}_tmp"

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

backend_df = create_backend_csv(resolved_limits_table, speed_limit_data_source)

display(backend_df)

# COMMAND ----------

validate_backend_csv(backend_df)

# COMMAND ----------

backend_csl_df = create_backend_csl_csv(resolved_limits_table, speed_limit_data_source)

display(backend_csl_df)

# COMMAND ----------

validate_backend_csv(backend_csl_df)

# COMMAND ----------

# MAGIC %python
# MAGIC tilegen_df = create_tilegen_csv(resolved_limits_table)

# COMMAND ----------

emails = []
alert_name = "safety_mapdata_tilegen_rows"
slack_channels = ["safety-platform-alerts"]
execute_alert(
    "select 1",
    emails,
    slack_channels,
    alert_name,
    f"Writing production datasets for `v{tile_version}`\nosm_regions_versions: `{osm_regions_to_version}`\ntomtom_version: `{tomtom_version}`\nbackend rows: `{backend_df.count()}`\ntilegen rows: `{tilegen_df.count()}`",
)

# COMMAND ----------

persist_tilegen_csv(tilegen_df)

# COMMAND ----------

persist_speed_limits_backend_csv(
    backend_df, "tomtom", int(tile_version), tomtom_dataset_version
)

# COMMAND ----------

persist_speed_limits_backend_csl_csv(
    backend_csl_df, "tomtom", int(tile_version), tomtom_dataset_version
)


# COMMAND ----------

exit_notebook()


# COMMAND ----------
