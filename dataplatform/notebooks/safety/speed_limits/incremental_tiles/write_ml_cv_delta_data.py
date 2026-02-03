# Databricks notebook source
# MAGIC %run /backend/safety/speed_limits/utils_calling_conventions

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/utils_dataset_write

# COMMAND ----------

# MAGIC %pip install --upgrade "protobuf>=6.31.1"

# COMMAND ----------

ARG_TILE_VERSION = "tile_version"
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

ARG_OSM_DATASET_VERSION = "osm_dataset_version"
"""
* ARG_OSM_DATASET_VERSION:
String OSM dataset version (format: YYYYMMDD)
"""
dbutils.widgets.text(ARG_OSM_DATASET_VERSION, "")
osm_dataset_version = dbutils.widgets.get(ARG_OSM_DATASET_VERSION)
print(f"{ARG_OSM_DATASET_VERSION}: {osm_dataset_version}")

ARG_TEST_DATASET_TABLE_NAME = "arg_test_dataset_table_name"
"""
* ARG_TEST_DATASET_TABLE_NAME:
String test dataset table name
"""
dbutils.widgets.text(ARG_TEST_DATASET_TABLE_NAME, "9999")
test_dataset_table_name = dbutils.widgets.get(ARG_TEST_DATASET_TABLE_NAME)
print(f"{ARG_TEST_DATASET_TABLE_NAME}: {test_dataset_table_name}")

ARG_PREVIOUS_RESOLVED_LIMITS_TABLE = "arg_previous_resolved_limits_table"
"""
* ARG_PREVIOUS_RESOLVED_LIMITS_TABLE:
Optional string previous resolved speed limits table name. If provided, will use this instead of finding the latest.
"""
dbutils.widgets.text(ARG_PREVIOUS_RESOLVED_LIMITS_TABLE, "9999")
previous_resolved_limits_table_name_override = dbutils.widgets.get(
    ARG_PREVIOUS_RESOLVED_LIMITS_TABLE
)
print(
    f"{ARG_PREVIOUS_RESOLVED_LIMITS_TABLE}: {previous_resolved_limits_table_name_override}"
)


# COMMAND ----------

import re
import json
import time
from base64 import b64encode
from datetime import datetime
from pyspark.sql.functions import udf, col, regexp_extract, lit, when
from pyspark.sql import Row, DataFrame
from pyspark.sql.types import (
    IntegerType,
    StructField,
    StructType,
    StringType,
)
from speed_limit_overrides_pb2 import CV_SPEED_SIGN

# COMMAND ----------


ML_CV = "ml_cv"
ML_SPEED_LIMIT_SOURCE_VERIFIED_DATASOURCE = {
    0: CV_SPEED_SIGN,
}


# COMMAND ----------

speed_limit_source_schema = StructType(
    [
        StructField("verified_data_source", IntegerType(), True),
        StructField("proto", StringType(), True),
    ]
)


def select_ml_verified_speed_limit_source(
    data_source: str, speed_limit_source: int
) -> Row:
    """Why we import here instead of at the top?

    When we use a UDF in PySpark, our driver program (where DBX notebook code is running) needs to send that function
    to all the worker nodes in the Spark cluster. The workers are the ones that actually execute the function on
    different partitions of our data. To send the Python function select_ml_verified_speed_limit_source across the
    network to the workers, Spark must first serialize it. Serialization (using a library called cloudpickle) is the
    process of converting the function and any objects it depends on into a stream of bytes.

    However, the SpeedLimitMetadata and VerifiedSpeedLimitDataSourceStatus classes are defined in a low-level
    protobuf file and the 'Descriptors' are in C that are non-serializable, so it will fail to run.

    So, we import the protobuf classes here to avoid serializing the protobuf classes, every DBX worker will import
    the protobuf classes locally to
    """
    from speed_limit_overrides_pb2 import (
        SpeedLimitMetadata,
        VerifiedSpeedLimitDataSourceStatus,
    )

    """
    Select the ml verified data source if the data source is ml_cv
    """
    if data_source != ML_CV:
        return Row(None, None)

    if speed_limit_source not in ML_SPEED_LIMIT_SOURCE_VERIFIED_DATASOURCE:
        return Row(None, None)

    verified_data_source = ML_SPEED_LIMIT_SOURCE_VERIFIED_DATASOURCE[speed_limit_source]
    metadata = SpeedLimitMetadata()
    verified_source = VerifiedSpeedLimitDataSourceStatus()
    verified_source.source_type = verified_data_source
    verified_source.is_verified = True
    metadata.verified_data_sources.append(verified_source)
    proto_bytes = metadata.SerializeToString()

    # Convert binary to base64 string
    proto_base64 = b64encode(proto_bytes).decode("utf-8")

    return Row(verified_data_source, proto_base64)


select_ml_verified_speed_limit_source_udf = udf(
    select_ml_verified_speed_limit_source, speed_limit_source_schema
)

assert select_ml_verified_speed_limit_source("iowa_dot", 1) == Row(None, None)
assert select_ml_verified_speed_limit_source(ML_CV, 1) == Row(None, None)
assert select_ml_verified_speed_limit_source(ML_CV, 0) == Row(CV_SPEED_SIGN, "CgQIARAB")


# COMMAND ----------

mkt_to_kph_schema = StructType(
    [
        StructField("speed_limit", StringType(), True),
        StructField("data_source", StringType(), True),
    ]
)


def convert_speed_to_milliknots_kph(
    dataset_name: str, speed_limit_milliknots: int
) -> Row:
    if speed_limit_milliknots is not None:
        return Row(f"{int(round(speed_limit_milliknots * 0.001852))} kph", dataset_name)
    return Row(None, dataset_name)


convert_speed_to_milliknots_kph_udf = udf(
    convert_speed_to_milliknots_kph, mkt_to_kph_schema
)

assert convert_speed_to_milliknots_kph("ml_cv", 10000) == Row("19 kph", "ml_cv")
assert convert_speed_to_milliknots_kph("ml_cv", None) == Row(None, "ml_cv")

# COMMAND ----------


def find_latest_mapmatched_table_by_dataset_name(dataset_name: str) -> str:
    """
    Given a prefix, return the latest mapmatched table,
    e.g. "ml_speed_limit_data_created_on_" returns "ml_speed_limit_data_created_on_20240301_v1"
    """
    tables_df = spark.sql("SHOW TABLES IN dojo")
    pattern_prefix = SPEED_LIMIT_DATASET_NAME_TO_MAPMATCHED_TABLE_NAME[dataset_name]
    pattern = rf"{pattern_prefix}(\d{{4}}_\d{{2}}_\d{{2}})_v(\d+)"

    # Extract version and filter relevant tables
    tables_with_version_df = tables_df.withColumn(
        "version", regexp_extract(col("tableName"), pattern, 2).cast("int")
    ).filter(col("version").isNotNull())

    # Order by version and get the latest table
    latest_table_df = tables_with_version_df.orderBy(col("version").desc()).limit(1)
    latest_table_rows = latest_table_df.collect()
    if not latest_table_rows:
        # No matching table found
        raise ValueError(f"No matching table found for {dataset_name}")
    else:
        latest_table_name = latest_table_df.collect()[0]["tableName"]
        return f"dojo.{latest_table_name}"


# COMMAND ----------


def evaluate_max_speed_limits_for_dataset(
    dataset_mapmatched_table_name: str,
    speed_limit_dataset_name: str,
    osm_dataset_version: str,
):
    """
    Join the dataset mapmatched table with tomtomosm resolved table:
    - for all sources, use max(tomtom, osm, mlcv)
    - Includes deleted way_ids (with 0 speed_limit_milliknots) so max() will pick TomTom if present, else 0
    """
    tomtom_osm_resolved_table_name = (
        find_tomtom_osm_resolved_speed_limit_table_by_version(osm_dataset_version)
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

    # Replace "US" with "USA" in the country column
    # Here I added a country value check instead of directly replacing "US" with "USA"
    # in case in the future we have a country value other than "US"
    dataset_mapmatched_df = dataset_mapmatched_df.withColumn(
        "country", when(col("country") == "US", "USA").otherwise(col("country"))
    )

    # Join the DataFrames on "osm_way_id"
    joined_df = tomtom_osm_resolved_df.join(
        dataset_mapmatched_df, on="osm_way_id", how="outer"
    )

    # Fallback for tomtom_country_code
    # Only use country from dataset_mapmatched_df if it exists
    if "country" in dataset_mapmatched_df.columns:
        joined_df = joined_df.withColumn(
            "tomtom_country_code",
            when(
                (col("tomtom_country_code").isNull())
                | (col("tomtom_country_code") == ""),
                col("country"),
            ).otherwise(col("tomtom_country_code")),
        )
    # If country column doesn't exist, keep original tomtom_country_code (no action needed)

    # Fallback for tomtom_state_code - check for state_code or state columns
    # First check which columns exist in the dataset_mapmatched_df
    dataset_columns = dataset_mapmatched_df.columns

    # Create fallback logic based on available columns
    if "state_code" in dataset_columns:
        # state_code column exists
        joined_df = joined_df.withColumn(
            "tomtom_state_code",
            when(
                (col("tomtom_state_code").isNull()) | (col("tomtom_state_code") == ""),
                when(
                    (col("state_code").isNotNull()) & (col("state_code") != ""),
                    col("state_code"),
                ).otherwise(lit(None)),
            ).otherwise(col("tomtom_state_code")),
        )
    elif "state" in dataset_columns:
        # state column exists
        joined_df = joined_df.withColumn(
            "tomtom_state_code",
            when(
                (col("tomtom_state_code").isNull()) | (col("tomtom_state_code") == ""),
                when(
                    (col("state").isNotNull()) & (col("state") != ""),
                    col("state"),
                ).otherwise(lit(None)),
            ).otherwise(col("tomtom_state_code")),
        )
    # If neither column exists, keep original tomtom_state_code (no action needed)

    # For all sources (cv and telematics), use max(tomtom, osm, mlcv)
    max_speed_limit_df = joined_df.withColumn(
        "tomtom_osm_vs_ml_cv",
        select_max_speed_limit_amongst_osm_tomtom_decoupled_speed_limits_udf(
            lit(speed_limit_dataset_name),
            col("speed_limit_milliknots"),
            col("data_source"),
            col("osm_tomtom_updated_passenger_limit"),
        ),
    )

    max_speed_limit_df = max_speed_limit_df.withColumn(
        "max_speed_limit", col("tomtom_osm_vs_ml_cv.speed_limit")
    ).withColumn("data_source", col("tomtom_osm_vs_ml_cv.data_source"))
    max_speed_limit_df = max_speed_limit_df.drop("tomtom_osm_vs_ml_cv")

    # Store the ml verified data source if the data_source of the speed limit is ml_cv
    max_speed_limit_df = max_speed_limit_df.withColumn(
        "ml_verified_speed_limit_source",
        select_ml_verified_speed_limit_source_udf(
            col("data_source"), col("speed_limit_source")
        ),
    )
    max_speed_limit_df = max_speed_limit_df.withColumn(
        "verified_data_source",
        col("ml_verified_speed_limit_source").getField("verified_data_source"),
    ).withColumn("proto", col("ml_verified_speed_limit_source").getField("proto"))
    max_speed_limit_df = max_speed_limit_df.drop("ml_verified_speed_limit_source")

    count_cv_verified_speed_limits = max_speed_limit_df.filter(
        "verified_data_source IS NOT NULL"
    ).count()
    count_unverified_speed_limits = max_speed_limit_df.filter(
        "verified_data_source IS NULL"
    ).count()

    print(
        f"Number of wayIds with a CV verified speed limit: {count_cv_verified_speed_limits} "
    )
    print(
        f"Number of wayIds with an unverified speed limit: {count_unverified_speed_limits} "
    )

    # Add org_id assignment based on the actual data_source used
    # If data_source is "ml_cv" or "global_override", use -3 (master data org_id)
    # Otherwise, default to -1 for tomtom/osm
    result_df = max_speed_limit_df.withColumn(
        "org_id",
        when(
            (col("data_source") == ML_CV) | (col("data_source") == GLOBAL_OVERRIDE),
            lit(SPEED_LIMIT_DATASET_NAME_TO_DB_ORG_ID[speed_limit_dataset_name]),
        ).otherwise(lit(-1)),
    ).select(
        "osm_way_id",
        "max_speed_limit",
        "data_source",
        "tomtom_commercial_vehicle_speed_map",
        "tomtom_country_code",
        "tomtom_state_code",
        "verified_data_source",
        "proto",
        "org_id",
    )

    return result_df


# COMMAND ----------


def apply_global_overrides(
    max_speed_limit_df: DataFrame,
    global_overrides_table: str,
) -> DataFrame:
    """
    Apply global speed limit overrides to the max speed limit DataFrame.

    Rules:
    - If verified_data_source IS NOT NULL (cv_verified), keep original speed limit
    - If current speed limit is zero or null, keep original (don't apply override)
    - Otherwise, take max(current_speed_limit, global_override)

    Args:
        max_speed_limit_df: DataFrame with columns including osm_way_id, max_speed_limit,
                           data_source, verified_data_source
        global_overrides_table: Name of the global overrides table

    Returns:
        DataFrame with global overrides applied where applicable
    """
    if not global_overrides_table or global_overrides_table.strip() == "":
        print("No global overrides table specified, skipping global overrides")
        return max_speed_limit_df

    if not DBX_TABLE.is_table_exists(global_overrides_table):
        print(
            f"Global overrides table {global_overrides_table} does not exist, skipping"
        )
        return max_speed_limit_df

    global_overrides_df = spark.table(global_overrides_table)
    override_count = global_overrides_df.count()
    print(f"Loaded {override_count} global overrides from {global_overrides_table}")

    if override_count == 0:
        print("No global overrides to apply")
        return max_speed_limit_df

    # Join with global overrides on way_id
    joined_df = max_speed_limit_df.alias("speed").join(
        global_overrides_df.select(
            col("way_id").alias("go_way_id"),
            col("global_override_speed_limit"),
            col("global_override_milliknots"),
        ).alias("go"),
        col("speed.osm_way_id") == col("go.go_way_id"),
        "left",
    )

    # Apply global override logic:
    # - If verified_data_source IS NOT NULL (cv_verified), keep original
    # - Otherwise, compare in kph and take max
    result_df = (
        joined_df.withColumn(
            "current_speed_kph",
            # Extract numeric value from max_speed_limit (e.g., "65 kph" -> 65, "40 mph" -> 64)
            when(
                col("max_speed_limit").contains("kph"),
                regexp_extract(col("max_speed_limit"), r"(\d+)", 1).cast("int"),
            )
            .when(
                col("max_speed_limit").contains("mph"),
                (
                    regexp_extract(col("max_speed_limit"), r"(\d+)", 1).cast("int")
                    * 1.60934
                ).cast("int"),
            )
            .otherwise(lit(None)),
        )
        .withColumn(
            "override_speed_kph",
            # Extract numeric value from global_override_speed_limit (always "XX kph")
            regexp_extract(col("global_override_speed_limit"), r"(\d+)", 1).cast("int"),
        )
        .withColumn(
            # Determine if we should use the global override
            "use_global_override",
            (
                col("verified_data_source").isNull()  # Not cv_verified
                & col("go_way_id").isNotNull()  # Has a global override
                & col("current_speed_kph").isNotNull()  # Has a resolved speed limit
                & (col("current_speed_kph") > 0)  # Resolved speed limit is non-zero
                & (
                    col("override_speed_kph") > col("current_speed_kph")
                )  # Override is higher
            ),
        )
        .withColumn(
            # Apply the override to max_speed_limit
            "max_speed_limit",
            when(
                col("use_global_override"), col("global_override_speed_limit")
            ).otherwise(col("max_speed_limit")),
        )
        .withColumn(
            # Update data_source when using global override
            "data_source",
            when(col("use_global_override"), lit(GLOBAL_OVERRIDE)).otherwise(
                col("data_source")
            ),
        )
        .withColumn(
            # Update org_id when using global override (must be -3 for master data)
            "org_id",
            when(
                col("use_global_override"),
                lit(SPEED_LIMIT_DATASET_NAME_TO_DB_ORG_ID[ML_CV]),
            ).otherwise(col("org_id")),
        )
    )
    # Note: verified_data_source and proto are not cleared because:
    # - Global overrides only apply when verified_data_source IS NULL
    # - Proto is only set for ML CV verified entries (which are never overridden)
    # - So entries that get global overrides already have proto = NULL

    # Count how many overrides were applied
    applied_count = result_df.filter(col("use_global_override")).count()
    print(f"Applied global overrides to {applied_count} way_ids")

    # Clean up temporary columns and select original schema
    result_df = result_df.select(
        "osm_way_id",
        "max_speed_limit",
        "data_source",
        "tomtom_commercial_vehicle_speed_map",
        "tomtom_country_code",
        "tomtom_state_code",
        "verified_data_source",
        "proto",
        "org_id",
    )

    return result_df


# COMMAND ----------


def create_ml_cv_combined_delta_data(
    table_name: str,
    speed_limit_dataset_name: str,
    tile_version: int,
    dataset_version: int,
    previous_table_name: str,
) -> DataFrame:
    """
    create_speed_limits_backend_csv uses dataset_version instead of mono_version.
    """
    current_ms = int(round(time.time() * 1000))
    current_resolved_limits_df = (
        spark.table(table_name)
        .filter("max_speed_limit IS NOT NULL")
        .withColumnRenamed("osm_way_id", "way_id")
        .withColumn("version_id", lit(tile_version))
        .withColumn("dataset_version", lit(dataset_version))
        .withColumn(
            "override_speed_limit_milliknots",
            convert_speed_to_milliknots_udf("max_speed_limit"),
        )
        .withColumn(
            "commercial_vehicle_speed_map",
            dump_speed_map_udf("tomtom_commercial_vehicle_speed_map"),
        )
        .withColumn("created_at_ms", lit(current_ms))
        .withColumn("user_id", lit(0))  # System-generated speed limit data
        .withColumnRenamed("data_source", "source")
        .withColumnRenamed("tomtom_country_code", "country_code")
        .withColumnRenamed("tomtom_state_code", "state_code")
        .select(
            "org_id",
            "way_id",
            "version_id",
            "dataset_version",
            "override_speed_limit_milliknots",
            "commercial_vehicle_speed_map",
            "created_at_ms",
            "user_id",
            "source",
            "country_code",
            "state_code",
            "proto",
        )
    )
    if previous_table_name is not None and int(dataset_version) != 0:
        previous_df = (
            spark.table(previous_table_name)
            .filter("max_speed_limit IS NOT NULL")
            .withColumnRenamed("osm_way_id", "way_id")
            .withColumn(
                "override_speed_limit_milliknots",
                convert_speed_to_milliknots_udf("max_speed_limit"),
            )
            .select(
                "way_id",
                "override_speed_limit_milliknots",
                "proto",
                "data_source",
            )
        )

        # Find way_ids that changed (additions or updates)
        joined_df = (
            current_resolved_limits_df.alias("current")
            .join(
                previous_df.alias("previous"),
                col("current.way_id") == col("previous.way_id"),
                how="left",
            )
            .where(
                "previous.way_id IS NULL or (previous.way_id IS NOT NULL AND (previous.override_speed_limit_milliknots != current.override_speed_limit_milliknots OR previous.proto IS DISTINCT FROM current.proto))"
            )
            .where("current.override_speed_limit_milliknots IS NOT NULL")
        )

        # Compute counts for additions and updates
        count_null_previous = joined_df.filter("previous.way_id IS NULL").count()
        count_updates = joined_df.filter("previous.way_id IS NOT NULL").count()
        print(f"Number of wayids to add with a new speed limit: {count_null_previous}")
        print(f"Number of wayids to update with a new speed limit: {count_updates}")

        result_df = joined_df.select("current.*")

        # Find way_ids that need soft-deletion (in previous with non-zero speed but NOT in current)
        # Only soft-delete entries that had a valid (non-zero) speed limit in previous
        soft_delete_way_ids = (
            previous_df.filter("override_speed_limit_milliknots > 0")
            .select("way_id")
            .distinct()
            .alias("previous")
            .join(
                current_resolved_limits_df.select("way_id").distinct().alias("current"),
                col("previous.way_id") == col("current.way_id"),
                how="leftanti",  # way_ids in previous but NOT in current
            )
        )

        count_soft_deletes = soft_delete_way_ids.count()
        print(f"Number of wayids to soft-delete: {count_soft_deletes}")

        if count_soft_deletes > 0:
            # Create soft-delete rows with speed_limit = 0
            soft_delete_df = (
                soft_delete_way_ids.withColumn("org_id", lit(None).cast("int"))
                .withColumn("version_id", lit(tile_version))
                .withColumn("dataset_version", lit(dataset_version))
                .withColumn("override_speed_limit_milliknots", lit(0))
                .withColumn("commercial_vehicle_speed_map", lit(None).cast("string"))
                .withColumn("created_at_ms", lit(current_ms))
                .withColumn("user_id", lit(0))
                .withColumn("source", lit(speed_limit_dataset_name))
                .withColumn("country_code", lit(None).cast("string"))
                .withColumn("state_code", lit(None).cast("string"))
                .withColumn("proto", lit(None).cast("string"))
                .select(result_df.columns)
            )

            # Union soft-delete rows with delta
            result_df = result_df.union(soft_delete_df)

        # Validate final result contains expected columns
        expected_columns = {
            "org_id",
            "way_id",
            "version_id",
            "dataset_version",
            "override_speed_limit_milliknots",
            "commercial_vehicle_speed_map",
            "created_at_ms",
            "user_id",
            "source",
            "country_code",
            "state_code",
            "proto",
        }
        actual_columns = set(result_df.columns)
        assert (
            expected_columns == actual_columns
        ), f"Final delta table schema mismatch. Expected: {expected_columns}, Got: {actual_columns}"
    else:
        # If no previous table exists or dataset version is 0, use all current data as delta
        if int(dataset_version) == 0:
            print(
                "Dataset version is 0, skipping delta comparison and using all current data"
            )
        else:
            print("No previous table exists, using all current data as delta")
        result_df = current_resolved_limits_df.alias("current")
        result_df = result_df.select("current.*")

    print(f"Total number of way ids in the delta table: {result_df.count()}")
    # Get the delta table name using the generic method with dataset version
    delta_table_name = DBX_TABLE.delta_table("ml_cv", tile_version, dataset_version)

    # Write the delta data to the new table
    print(f"Writing combined delta data to {delta_table_name}")
    result_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
        delta_table_name
    )

    return delta_table_name


# COMMAND ----------

"""
    Main logic starts from here,
    1. we evaluate the max speed limits for the given dataset
        - find the latest tomtom_osm_resolved_speed_limits table
        - find the latest mapmatched table for the given ml_cv datasets
        - find the appropiate speed limit for the given dataset with tomtom and osm
            - for all sources (cv and telematics), use max(tomtom, osm, mlcv)
    2. store the result as a dataset resolved table to be used in the next steps
        - e.g. safety_map_data.ml_cv_resolved_speed_limits_{generated_date}
    3. create the delta table for the ml_cv dataset by comparing with previous resolved table
        - includes soft-deletion rows for way_ids no longer in current resolved table
"""
print("Starting write prod ml cv datasets step")

# Find the latest resolved speed limit table for the given dataset
# Use override if provided, otherwise find the latest
if previous_resolved_limits_table_name_override != "9999":
    previous_resolved_limits_table_name = previous_resolved_limits_table_name_override
    print(
        f"Using provided previous resolved limits table: {previous_resolved_limits_table_name}"
    )
else:
    previous_resolved_limits_table_name = (
        find_latest_incremental_dataset_resolved_speed_limit_table(
            speed_limit_dataset_name
        )
    )
    print(f"Found latest resolved limits table: {previous_resolved_limits_table_name}")

# Find the latest mapmatched table for the given ml_cv datasets
# If test_dataset_table_name is not "9999", use the test dataset table name
if test_dataset_table_name != "9999":
    dataset_mapmatched_table_name = f"dojo.{test_dataset_table_name}"
else:
    dataset_mapmatched_table_name = find_latest_mapmatched_table_by_dataset_name(
        speed_limit_dataset_name
    )

print(f"The latest mapmatched ml_cv dataset is {dataset_mapmatched_table_name}")
# Evaluate the speed limits for the given mapmatched table
max_speed_limit_df = evaluate_max_speed_limits_for_dataset(
    dataset_mapmatched_table_name, speed_limit_dataset_name, osm_dataset_version
)

today_date = datetime.today().strftime("%Y%m%d")

# Apply global overrides
# Global overrides are applied to non-cv_verified speed limits, taking max(current, override)
# The global overrides table is named by today's date (computed earlier in the pipeline)
global_overrides_table_name = f"safety_map_data.global_overrides_{today_date}"
print(f"Looking for global overrides table: {global_overrides_table_name}")
max_speed_limit_df = apply_global_overrides(
    max_speed_limit_df, global_overrides_table_name
)

resolved_limits_table_name = f"safety_map_data.{SPEED_LIMIT_DATASET_NAME_TO_RESOLVED_TABLE_NAME[speed_limit_dataset_name]}_incremental_{today_date}"


print(f"Total rows in resolved table: {max_speed_limit_df.count()}")
max_speed_limit_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(resolved_limits_table_name)
print(f"Done storing the table {resolved_limits_table_name} on S3")

# COMMAND ----------

delta_table_name = create_ml_cv_combined_delta_data(
    resolved_limits_table_name,
    speed_limit_dataset_name,
    tile_version,
    speed_limit_dataset_version,
    previous_resolved_limits_table_name,
)


print(f"Done writing the delta data to {delta_table_name}")


# COMMAND ----------

exit_notebook()

# COMMAND ----------
