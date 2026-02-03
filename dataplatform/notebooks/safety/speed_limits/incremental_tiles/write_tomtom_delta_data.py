# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/utils_calling_conventions

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/utils_dataset_write

# COMMAND ----------

import json
import time
from pyspark.sql.functions import col, lit, udf, struct, explode
from pyspark.sql.types import (
    ArrayType,
    StructType,
    StructField,
    LongType,
    IntegerType,
    StringType,
)

# COMMAND ----------

# Standard arguments and values

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

ARG_TILE_VERSION = "tile_version"
"""
* ARG_TILE_VERSION:
Must be specified in YYMM000 format
"""
dbutils.widgets.text(ARG_TILE_VERSION, "9999", "Tile Version")
tile_version = dbutils.widgets.get(ARG_TILE_VERSION)
print(f"{ARG_TILE_VERSION}: {tile_version}")

ARG_DATASET_VERSION = "dataset_version"
"""
* ARG_DATASET_VERSION:
Dataset version for delta table naming
"""
dbutils.widgets.text(ARG_DATASET_VERSION, "0", "Dataset Version")
dataset_version = dbutils.widgets.get(ARG_DATASET_VERSION)
print(f"{ARG_DATASET_VERSION}: {dataset_version}")


# COMMAND ----------

resolved_limits_table = DBX_TABLE.osm_tomtom_resolved_speed_limits(
    osm_regions_to_version, tomtom_version
)
print(f"resolved_limits_table: {resolved_limits_table}")

# COMMAND ----------
def create_tomtom_combined_delta_data(
    resolved_limits_table: str, dataset_version: str = "0"
):
    """
    create_tomtom_combined_delta_data computes the delta between the resolved table and current max version.
    It creates a single delta table that includes both regular speed limits (vehicle_type = 0) and commercial speed limits.
    It identifies records that have changed or are new compared to the currently deployed data.
    The delta data is written to a new table for downstream processing.

    Args:
        resolved_limits_table: Name of the resolved limits table
        dataset_version: Dataset version for delta table naming (defaults to "0")
    """
    from pyspark.sql.functions import col, lit

    # Get the delta table name using the generic method with dataset version
    delta_table_name = DBX_TABLE.delta_table("tomtom", tile_version, dataset_version)
    print(f"Creating combined delta table: {delta_table_name}")

    # Create the regular speed limits delta dataframe (vehicle_type = 0)
    regular_delta_df = (
        spark.table(resolved_limits_table)
        .filter("osm_tomtom_updated_passenger_limit IS NOT NULL")
        .withColumnRenamed("osm_way_id", "way_id")
        .withColumn(
            "override_speed_limit_milliknots",
            convert_speed_to_milliknots_udf("osm_tomtom_updated_passenger_limit"),
        )
        .withColumn("org_id", lit(-1))
        .withColumn("version_id", lit(tile_version))
        .withColumn("dataset_version", lit(dataset_version))
        .withColumn("created_at_ms", lit(int(round(time.time() * 1000))))
        .withColumn("vehicle_type", lit(0))  # Passenger speed limits
        .withColumnRenamed("tomtom_country_code", "country_code")
        .withColumnRenamed("tomtom_state_code", "state_code")
        .withColumnRenamed("data_source", "source")
        .select(
            "org_id",
            "way_id",
            "version_id",
            "dataset_version",
            "override_speed_limit_milliknots",
            "created_at_ms",
            "source",
            "vehicle_type",
            "country_code",
            "state_code",
        )
    )

    # Create the commercial speed limits delta dataframe
    # First, get the base data with commercial speed limit maps and convert to JSON
    base_df = (
        spark.table(resolved_limits_table)
        .filter("tomtom_commercial_vehicle_speed_map IS NOT NULL")
        .withColumnRenamed("osm_way_id", "way_id")
        .withColumn(
            "commercial_speed_map_json",
            dump_speed_map_udf("tomtom_commercial_vehicle_speed_map"),
        )
        .select(
            "way_id",
            "commercial_speed_map_json",
            "tomtom_country_code",
            "tomtom_state_code",
            "osm_tomtom_updated_passenger_limit",
            "data_source",
        )
    )

    # Define a UDF to explode commercial speed limits into individual records
    # Use the existing function from utils_dataset_write (excludes passenger speed limits)
    explode_csl_udf = udf(
        explode_commercial_speed_limits,
        ArrayType(
            StructType(
                [
                    StructField("way_id", LongType(), False),
                    StructField("vehicle_type", IntegerType(), False),
                    StructField("speed_limit_milliknots", IntegerType(), False),
                ]
            )
        ),
    )

    # Apply the UDF and explode the results
    exploded_df = (
        base_df.withColumn("csl_records", explode_csl_udf(struct("*")))
        .withColumn("csl_record", explode("csl_records"))
        .select(
            "csl_record.*", "tomtom_country_code", "tomtom_state_code", "data_source"
        )
        .filter("speed_limit_milliknots IS NOT NULL")
    )

    # Add the required columns for delta format
    csl_delta_df = (
        exploded_df.withColumn("org_id", lit(-1))
        .withColumn("version_id", lit(tile_version))
        .withColumn("dataset_version", lit(dataset_version))
        .withColumn("override_speed_limit_milliknots", col("speed_limit_milliknots"))
        .withColumn("created_at_ms", lit(int(round(time.time() * 1000))))
        .withColumnRenamed("tomtom_country_code", "country_code")
        .withColumnRenamed("tomtom_state_code", "state_code")
        .withColumnRenamed("data_source", "source")
        .select(
            "org_id",
            "way_id",
            "version_id",
            "dataset_version",
            "override_speed_limit_milliknots",
            "created_at_ms",
            "source",
            "vehicle_type",
            "country_code",
            "state_code",
        )
    )

    # Combine regular and CSL delta dataframes
    combined_delta_df = regular_delta_df.union(csl_delta_df)

    # Join with deployed data to find changes for regular speed limits
    regular_delta_result = (
        combined_delta_df.filter(col("vehicle_type") == 0)
        .alias("resolved")
        .join(
            spark.table("deployed_global_limits_fw_tiles_with_dataset_version").alias(
                "deployed"
            ),
            col("resolved.way_id") == col("deployed.way_id"),
            how="left",
        )
        .where(
            "deployed.way_id IS NULL or (deployed.way_id IS NOT NULL AND deployed.speed_limit_milliknots != resolved.override_speed_limit_milliknots)"
        )
        .where("resolved.override_speed_limit_milliknots IS NOT NULL")
        .select("resolved.*")
    )

    # Join with deployed data to find changes for commercial speed limits
    csl_delta_result = (
        combined_delta_df.filter(col("vehicle_type") != 0)
        .alias("backend_csl")
        .join(
            spark.table("deployed_global_limits_csl_fw_tiles").alias("deployed_csl"),
            (col("backend_csl.way_id") == col("deployed_csl.way_id"))
            & (col("backend_csl.vehicle_type") == col("deployed_csl.vehicle_type")),
            how="left",
        )
        .where(
            "deployed_csl.way_id IS NULL or (deployed_csl.way_id IS NOT NULL AND deployed_csl.speed_limit_milliknots != backend_csl.override_speed_limit_milliknots)"
        )
        .where("backend_csl.override_speed_limit_milliknots IS NOT NULL")
        .select("backend_csl.*")
    )

    # Combine the results
    final_delta_result = regular_delta_result.union(csl_delta_result)

    # Write the delta data to the new table
    print(f"Writing combined delta data to {delta_table_name}")
    final_delta_result.write.mode("overwrite").option(
        "mergeSchema", "true"
    ).saveAsTable(delta_table_name)

    # Print summary statistics
    total_count = final_delta_result.count()
    regular_count = final_delta_result.filter(col("vehicle_type") == 0).count()
    csl_count = final_delta_result.filter(col("vehicle_type") != 0).count()
    print(f"Combined delta table created with {total_count} total records")
    print(f"  - Regular speed limits (vehicle_type=0): {regular_count} records")
    print(f"  - Commercial speed limits: {csl_count} records")

    return delta_table_name


# COMMAND ----------

# Execute the combined delta creation
delta_table = create_tomtom_combined_delta_data(resolved_limits_table, dataset_version)
print(f"Successfully created combined delta table: {delta_table}")

exit_notebook()
