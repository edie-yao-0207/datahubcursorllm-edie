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
ARG_USE_DEPLOYED_SPEED_LIMITS_WITH_VERSION_CAP = (
    "use_deployed_speed_limits_with_version_cap"
)
ARG_TILE_VERSION_CAP = "tile_version_cap"
ARG_DATASET_VERSION_CAP = "dataset_version_cap"
ARG_OSM_DATASET_VERSION = "osm_dataset_version"
ARG_TEST_DATASET_TABLE_NAME = "test_dataset_table_name"

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

"""
* ARG_USE_DEPLOYED_SPEED_LIMITS_WITH_VERSION_CAP:
Boolean flag to use deployed speed limits with version cap
"""
dbutils.widgets.text(ARG_USE_DEPLOYED_SPEED_LIMITS_WITH_VERSION_CAP, "")
use_deployed_speed_limits_with_version_cap = (
    dbutils.widgets.get(ARG_USE_DEPLOYED_SPEED_LIMITS_WITH_VERSION_CAP).lower()
    == "true"
)
print(
    f"{ARG_USE_DEPLOYED_SPEED_LIMITS_WITH_VERSION_CAP}: {use_deployed_speed_limits_with_version_cap}"
)

"""
* ARG_TILE_VERSION_CAP:
Integer tile version cap
"""
dbutils.widgets.text(ARG_TILE_VERSION_CAP, "9999")
tile_version_cap = int(dbutils.widgets.get(ARG_TILE_VERSION_CAP))
print(f"{ARG_TILE_VERSION_CAP}: {tile_version_cap}")

"""
* ARG_DATASET_VERSION_CAP:
Integer dataset version cap
"""
dbutils.widgets.text(ARG_DATASET_VERSION_CAP, "9999")
dataset_version_cap = int(dbutils.widgets.get(ARG_DATASET_VERSION_CAP))
print(f"{ARG_DATASET_VERSION_CAP}: {dataset_version_cap}")

"""
* ARG_OSM_DATASET_VERSION:
String osm dataset version
"""
dbutils.widgets.text(ARG_OSM_DATASET_VERSION, "")
osm_dataset_version = dbutils.widgets.get(ARG_OSM_DATASET_VERSION)
print(f"{ARG_OSM_DATASET_VERSION}: {osm_dataset_version}")

"""
* ARG_TEST_DATASET_TABLE_NAME:
String test dataset table name
"""
dbutils.widgets.text(ARG_TEST_DATASET_TABLE_NAME, "9999")
test_dataset_table_name = dbutils.widgets.get(ARG_TEST_DATASET_TABLE_NAME)
print(f"{ARG_TEST_DATASET_TABLE_NAME}: {test_dataset_table_name}")
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
):
    """
    Join the dataset mapmatched table with tomtomosm resolved table:
    - for all sources, use max(tomtom, osm, mlcv)
    """
    tomtom_osm_resolved_table_name = (
        find_tomtom_osm_resolved_speed_limit_table_by_version(osm_dataset_version)
    )

    print(f"Using OSM dataset version: {osm_dataset_version}")
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
    joined_df = joined_df.withColumn(
        "tomtom_country_code",
        when(
            (col("tomtom_country_code").isNull()) | (col("tomtom_country_code") == ""),
            col("country"),
        ).otherwise(col("tomtom_country_code")),
    )

    # Fallback for tomtom_state_code
    joined_df = joined_df.withColumn(
        "tomtom_state_code",
        when(
            (col("tomtom_state_code").isNull()) | (col("tomtom_state_code") == ""),
            col("state_code"),
        ).otherwise(col("tomtom_state_code")),
    )

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

    result_df = max_speed_limit_df.select(
        "osm_way_id",
        "max_speed_limit",
        "data_source",
        "tomtom_commercial_vehicle_speed_map",
        "tomtom_country_code",
        "tomtom_state_code",
        "verified_data_source",
        "proto",
    )

    return result_df


# COMMAND ----------


def get_deleted_rows_for_ml_cv(resolved_limits_df, speed_limit_dataset_name: str):
    """
    Find ML/CV way_ids that exist in deployed speedlimitsdb but are no longer ML/CV winners.
    This includes: (1) ways removed from dojo, and (2) ways that lost the max() comparison to TomTom/OSM.
    These need to be represented as "0 mph" rows in the resolved table for proper FW tile generation.

    This matches the backend soft-delete logic exactly.
    """
    # Filter resolved table to only ML/CV winners (matching backend_df logic at line 408)
    ml_cv_winners_df = resolved_limits_df.filter(
        f"data_source = '{speed_limit_dataset_name}'"
    )

    ml_cv_limits_to_delete = (
        spark.table("deployed_speed_limits_fw_tiles")
        .filter(
            f"data_source = {DATASET_NAME_TO_DATASOURCE[speed_limit_dataset_name].value}"
        )  # ML_CV data_source = 4
        .filter("speed_limit_milliknots > 0")  # Exclude already-soft-deleted entries
        .alias("ml_cv_limits")
        .join(
            ml_cv_winners_df.alias("resolved"),
            col("ml_cv_limits.way_id") == col("resolved.osm_way_id"),
            how="leftanti",  # way_ids in deployed but NOT in ML/CV winners
        )
        .createOrReplaceTempView("ml_cv_limits_to_delete")
    )
    ml_cv_limits_to_delete = spark.sql(
        """
    select
        way_id as osm_way_id,
        "0 mph" as max_speed_limit,
        "ml_cv" as data_source,
        null as tomtom_commercial_vehicle_speed_map,
        null as tomtom_country_code,
        null as tomtom_state_code,
        null as verified_data_source,
        null as proto
    from ml_cv_limits_to_delete
    """
    )
    print(
        f"adding rows for deleted ML/CV limits: {ml_cv_limits_to_delete.count()} rows"
    )
    return ml_cv_limits_to_delete


# COMMAND ----------


def create_speed_limits_backend_csv_with_verified_speed_limit_source_delta(
    table_name: str,
    speed_limit_dataset_name: str,
    tile_version: int,
    dataset_version: int,
) -> DataFrame:
    """
    create_speed_limits_backend_csv uses dataset_version instead of mono_version.
    """
    current_ms = int(round(time.time() * 1000))
    backend_df = (
        spark.table(table_name)
        .filter("max_speed_limit IS NOT NULL")
        .filter(f"data_source = '{speed_limit_dataset_name}'")  # filters out tomtom/osm
        .withColumn(
            "org_id",
            lit(SPEED_LIMIT_DATASET_NAME_TO_DB_ORG_ID[speed_limit_dataset_name]),
        )  # e.g. -2 for iowa
        .withColumnRenamed("osm_way_id", "way_id")
        .withColumn("version_id", lit(tile_version))
        .withColumn("dataset_version", lit(dataset_version))
        .withColumn(
            "override_speed_limit_milliknots",
            convert_speed_to_milliknots_udf("max_speed_limit"),
        )
        .filter(
            "override_speed_limit_milliknots > 0"
        )  # Exclude 0-value speed limits (reserved for soft-deletion)
        .withColumn("created_at_ms", lit(current_ms))
        .withColumn("user_id", lit(0))  # System-generated speed limit data
        .withColumn(
            "data_source",
            lit(DATASET_NAME_TO_DATASOURCE[speed_limit_dataset_name].value),
        )  # e.g. 3 for iowa
        .select(
            "org_id",
            "way_id",
            "version_id",
            "dataset_version",
            "override_speed_limit_milliknots",
            "created_at_ms",
            "user_id",
            "data_source",
            "proto",
        )
    )

    # Only include updated rows
    joined_df = (
        backend_df.alias("backend")
        .join(
            spark.table("deployed_speed_limits_fw_tiles").alias("deployed"),
            col("backend.way_id") == col("deployed.way_id"),
            how="left",
        )
        .where(
            "deployed.way_id IS NULL or (deployed.way_id IS NOT NULL AND (deployed.speed_limit_milliknots != backend.override_speed_limit_milliknots OR deployed.proto IS DISTINCT FROM backend.proto))"
        )
        .where("backend.override_speed_limit_milliknots IS NOT NULL")
    )

    # Log the counts for the specified conditions
    count_null_deployed = joined_df.filter("deployed.way_id IS NULL").count()
    count_updates = joined_df.filter("deployed.way_id IS NOT NULL").count()
    print(f"Number of wayids to add with a new speed limit: {count_null_deployed}")
    print(f"Number of wayids to update with a new speed limit: {count_updates}")
    result_df = joined_df.select("backend.*")

    # Extract soft-delete rows from resolved table (already computed by get_deleted_rows_for_ml_cv)
    # These are the "0 mph" entries that were appended to the resolved table for FW tiles
    deletion_df = (
        spark.table(table_name)
        .filter(
            f"max_speed_limit = '0 mph' AND data_source = '{speed_limit_dataset_name}'"
        )
        .withColumn(
            "org_id",
            lit(SPEED_LIMIT_DATASET_NAME_TO_DB_ORG_ID[speed_limit_dataset_name]),
        )
        .withColumnRenamed("osm_way_id", "way_id")
        .withColumn("version_id", lit(tile_version))
        .withColumn("dataset_version", lit(dataset_version))
        .withColumn("override_speed_limit_milliknots", lit(0))
        .withColumn("created_at_ms", lit(current_ms))
        .withColumn("user_id", lit(0))
        .withColumn(
            "data_source",
            lit(DATASET_NAME_TO_DATASOURCE[speed_limit_dataset_name].value),
        )
        .select(
            "org_id",
            "way_id",
            "version_id",
            "dataset_version",
            "override_speed_limit_milliknots",
            "created_at_ms",
            "user_id",
            "data_source",
            "proto",
        )
    )

    count_deletions = deletion_df.count()
    print(f"Number of wayids to soft-delete (no longer in dataset): {count_deletions}")

    # Validate soft deletion logic
    if count_deletions > 0:
        # Assert all deletion rows have override_speed_limit_milliknots = 0
        assert (
            deletion_df.filter("override_speed_limit_milliknots != 0").count() == 0
        ), "Soft deletion failed: some deletion rows do not have speed_limit = 0"

        # Assert all deletion rows have correct org_id
        expected_org_id = SPEED_LIMIT_DATASET_NAME_TO_DB_ORG_ID[
            speed_limit_dataset_name
        ]
        assert (
            deletion_df.filter(f"org_id != {expected_org_id}").count() == 0
        ), f"Soft deletion failed: some deletion rows have incorrect org_id (expected {expected_org_id})"

        # Assert all deletion rows have correct data_source
        expected_data_source = DATASET_NAME_TO_DATASOURCE[
            speed_limit_dataset_name
        ].value
        assert (
            deletion_df.filter(f"data_source != {expected_data_source}").count() == 0
        ), f"Soft deletion failed: some deletion rows have incorrect data source (expected {expected_data_source})"

    # Union deletion rows with the existing delta (additions/updates)
    result_df = result_df.union(deletion_df)

    print(f"Overall number of wayids to upsert to speedlimitsdb: {result_df.count()}")
    return result_df


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
    3. find the latest deployed speed limit value on every wayid that currently lives inside speedlimitsdb
    4. create the backend csv - this only contains the delta to write into speedlimitsdb
    5. create the tilegen csv - this contains all data to generate speed limit tiles & client tiles
    6. persist the backend csv on S3
    7. persist the tilegen csv on S3
"""
print("Starting write prod ml cv datasets step")
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
    dataset_mapmatched_table_name
)

today_date = datetime.today().strftime("%Y%m%d")
resolved_limits_table_name = f"safety_map_data.{SPEED_LIMIT_DATASET_NAME_TO_RESOLVED_TABLE_NAME[speed_limit_dataset_name]}_{today_date}"

# Find out the latest speed limit value on every wayid that lives inside speedlimitsdb
if use_deployed_speed_limits_with_version_cap:
    create_deployed_speed_limits_fw_tiles_with_version_cap(
        SPEED_LIMIT_DATASET_NAME_TO_DB_ORG_ID[speed_limit_dataset_name],
        tile_version_cap,
        dataset_version_cap,
    )
else:
    create_deployed_speed_limits_fw_tiles(
        SPEED_LIMIT_DATASET_NAME_TO_DB_ORG_ID[speed_limit_dataset_name]
    )

# Compute the rows to delete (following TomTom pattern)
# This ensures deleted rows appear in FW tiles with "0 mph"
limits_to_delete = get_deleted_rows_for_ml_cv(
    max_speed_limit_df, speed_limit_dataset_name
)

# Union the resolved limits with deletion rows before writing
max_speed_limit_df_with_deletions = max_speed_limit_df.union(limits_to_delete)

print(
    f"Total rows with mlcv (including deletions): {max_speed_limit_df_with_deletions.count()}"
)
max_speed_limit_df_with_deletions.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(resolved_limits_table_name)
print(f"Done storing the table {resolved_limits_table_name} on S3")

# COMMAND ----------

# Create backend_df after appending deleted rows
backend_df = create_speed_limits_backend_csv_with_verified_speed_limit_source_delta(
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
persist_speed_limits_tilegen_csv(
    tilegen_df, speed_limit_dataset_name, tile_version, speed_limit_dataset_version
)

# COMMAND ----------
exit_notebook()
