# Databricks notebook source
# MAGIC %pip install /Volumes/s3/dataplatform-deployed-artifacts/wheels/service_credentials-1.0.1-py3-none-any.whl

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/commercial_speed_limits_builder

# COMMAND ----------

import contextlib
import csv
from enum import Enum
import io
import json
import sqlite3
import time
from typing import Dict, Iterator, List

import boto3
from boto3.s3.transfer import TransferConfig
from delta.tables import *
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import col, expr, lit, udf, struct, explode
from pyspark.sql.types import (
    IntegerType,
    StringType,
    ArrayType,
    StructType,
    StructField,
    LongType,
)
import service_credentials  # required for ssm cloud credentials


class WriterConfig:
    """
    WriterConfig is meant to be used as a global singleton to specify shared config values.

    pendingVersion should always be specified prior to writing a dataset, since this ensures
    that the dataset is correctly ingested to the desired version.
    """

    pendingVersion = None

    def __init__(self):
        pass

    def getCurrentPendingVersion(self):
        if self.pendingVersion is None:
            raise Exception(
                "No pending version set. Specify a pending version by calling setCurrentPendingVersion()"
            )
        return self.pendingVersion

    def setCurrentPendingVersion(self, version: int):
        self.pendingVersion = version
        return True


# Initialize a global config.
config = WriterConfig()


class SpeedLimitDataSource(Enum):
    """
    SpeedLimitDataSource is synced up with the speedlimitsproto.SpeedLimitDataSource in the backend repo.
    This is used for correct attribution for the dataset when it is reviewed later on.
    This should be consistent with the proto enum in
    https://github.com/samsara-dev/backend/blob/master/go/src/samsaradev.io/safety/speedlimitproto/speed_limit_overrides.proto#L9
    """

    UNSET = 0
    DATA_SCIENCE_INFERENCE = 1
    VENDOR_TOM_TOM = 2
    VENDOR_IOWA_DOT = 3
    VENDOR_ML_CV = 4
    GLOBAL_OVERRIDE = 5


# COMMAND ----------

# DBTITLE 1,Latest Global Limits (To Be Joined with New FW Limits)
# MAGIC %sql
# MAGIC create or replace temporary view deployed_global_limits_fw_tiles as
# MAGIC with max as (
# MAGIC   select org_id, way_id, max(version_id) as version_id
# MAGIC   from speedlimitsdb.speed_limit_overrides
# MAGIC   where org_id = -1
# MAGIC   group by org_id, way_id
# MAGIC )
# MAGIC select
# MAGIC   o.org_id,
# MAGIC   o.way_id,
# MAGIC   o.version_id,
# MAGIC   CONCAT(CAST(round(o.override_speed_limit_milliknots / 868.97624190065,0) as INTEGER), " mph") as speed_limit_mph,
# MAGIC   o.override_speed_limit_milliknots as speed_limit_milliknots,
# MAGIC   o.created_at_ms,
# MAGIC   o.data_source
# MAGIC from speedlimitsdb.speed_limit_overrides as o
# MAGIC inner join max as m
# MAGIC on m.org_id = o.org_id
# MAGIC and m.way_id = o.way_id
# MAGIC and m.version_id = o.version_id
# MAGIC where o.org_id = -1;

# COMMAND ----------

# DBTITLE 1,Latest Global Limits with Dataset Version (To Be Joined with New FW Limits)
# MAGIC %sql
# MAGIC create or replace temporary view deployed_global_limits_fw_tiles_with_dataset_version as
# MAGIC with max_version_info as (
# MAGIC   select org_id, way_id, version_id, dataset_version
# MAGIC   from (
# MAGIC     select org_id, way_id, version_id, COALESCE(dataset_version, 0) as dataset_version,
# MAGIC       ROW_NUMBER() OVER (
# MAGIC         PARTITION BY org_id, way_id
# MAGIC         ORDER BY version_id DESC, COALESCE(dataset_version, 0) DESC
# MAGIC       ) AS rn
# MAGIC     from speedlimitsdb.speed_limit_overrides
# MAGIC     where org_id = -1
# MAGIC   ) ranked_versions
# MAGIC   where rn = 1
# MAGIC )
# MAGIC select
# MAGIC   o.org_id,
# MAGIC   o.way_id,
# MAGIC   o.version_id,
# MAGIC   COALESCE(o.dataset_version, 0) as dataset_version,
# MAGIC   CONCAT(CAST(round(o.override_speed_limit_milliknots / 868.97624190065,0) as INTEGER), " mph") as speed_limit_mph,
# MAGIC   o.override_speed_limit_milliknots as speed_limit_milliknots,
# MAGIC   o.created_at_ms,
# MAGIC   o.data_source
# MAGIC from speedlimitsdb.speed_limit_overrides as o
# MAGIC inner join max_version_info as m
# MAGIC on m.org_id = o.org_id
# MAGIC and m.way_id = o.way_id
# MAGIC and m.version_id = o.version_id
# MAGIC and COALESCE(m.dataset_version, 0) = COALESCE(o.dataset_version, 0)
# MAGIC where o.org_id = -1;

# COMMAND ----------

# DBTITLE 1,Latest Global Commercial Speed Limits (To Be Joined with New CSL FW Limits)
# MAGIC %sql
# MAGIC create or replace temporary view deployed_global_limits_csl_fw_tiles as
# MAGIC with max_version_info as (
# MAGIC   select org_id, way_id, vehicle_type, version_id, dataset_version
# MAGIC   from (
# MAGIC     select org_id, way_id, vehicle_type, version_id, COALESCE(dataset_version, 0) as dataset_version,
# MAGIC       ROW_NUMBER() OVER (
# MAGIC         PARTITION BY org_id, way_id, vehicle_type
# MAGIC         ORDER BY version_id DESC, COALESCE(dataset_version, 0) DESC
# MAGIC       ) AS rn
# MAGIC     from speedlimitsdb.commercial_speed_limit_overrides
# MAGIC     where org_id = -1
# MAGIC   ) ranked_versions
# MAGIC   where rn = 1
# MAGIC )
# MAGIC select
# MAGIC   o.org_id,
# MAGIC   o.way_id,
# MAGIC   o.vehicle_type,
# MAGIC   o.version_id,
# MAGIC   COALESCE(o.dataset_version, 0) as dataset_version,
# MAGIC   CONCAT(CAST(round(o.override_speed_limit_milliknots / 868.97624190065,0) as INTEGER), " mph") as speed_limit_mph,
# MAGIC   o.override_speed_limit_milliknots as speed_limit_milliknots,
# MAGIC   o.created_at_ms,
# MAGIC   o.data_source
# MAGIC from speedlimitsdb.commercial_speed_limit_overrides as o
# MAGIC inner join max_version_info as m
# MAGIC on m.org_id = o.org_id
# MAGIC and m.way_id = o.way_id
# MAGIC and m.vehicle_type = o.vehicle_type
# MAGIC and m.version_id = o.version_id
# MAGIC and COALESCE(m.dataset_version, 0) = COALESCE(o.dataset_version, 0)
# MAGIC where o.org_id = -1;

# COMMAND ----------


def convert_speed_to_milliknots(unitized_speed_limit: str):
    """expects: speed limit with unit"""
    comp = unitized_speed_limit.split(" ")
    if len(comp) == 0:
        print(unitized_speed_limit, comp)
        return None
    limit = comp[0]
    unit = comp[1]
    if unit == "mph":
        return int(round(int(limit) * 868.97624190065, 0))
    if unit == "kph":
        return int(round(int(limit) * 539.957269941, 0))
    return None


assert convert_speed_to_milliknots("20 kph") == 10799
assert convert_speed_to_milliknots("20 mph") == 17380
convert_speed_to_milliknots_udf = udf(convert_speed_to_milliknots, IntegerType())


def process_commercial_speed_limits(
    way_id, commercial_speed_map_json, country_code, state_code, passenger_speed_limit
):
    """
    Process commercial speed limits and return a list of records.
    Each record contains: way_id, vehicle_type, speed_limit_milliknots
    """
    records = []

    try:
        # Deserialize the JSON commercial speed limit map
        commercial_speed_map = json.loads(commercial_speed_map_json)
        if commercial_speed_map is None:
            return records

        # Use the existing function to build the commercial speed limit map
        csl_speed_map = build_commercial_speed_limit_map(
            passenger_speed_limit,
            commercial_speed_map_json,
            country_code,
            state_code,
            include_passenger_speed_limit=False,  # Exclude passenger speed limits for backend CSV
            skip_passenger_speed_comparison=True,  # Include all CSL limits even if they match passenger
        )

        # Create individual records for each vehicle type and speed limit
        for vehicle_type_str, speed_limit_kmph in csl_speed_map.items():
            # Convert kmph to unitized format for milliknots conversion
            speed_limit_mph = speed_limit_kmph / KPH_IN_MPH  # Convert kmph to mph
            unitized_speed_limit = f"{int(round(speed_limit_mph))} mph"

            # Convert to milliknots
            speed_limit_milliknots = convert_speed_to_milliknots(unitized_speed_limit)

            if speed_limit_milliknots is not None:
                records.append(
                    {
                        "way_id": way_id,
                        "vehicle_type": int(vehicle_type_str),
                        "speed_limit_milliknots": speed_limit_milliknots,
                    }
                )

    except (json.JSONDecodeError, Exception) as e:
        # Skip records with invalid JSON or other errors
        print(f"Error processing commercial speed limits for way_id {way_id}: {e}")
        pass

    return records


def explode_commercial_speed_limits(row):
    """
    Explode commercial speed limits into individual records.
    Each record will have: way_id, vehicle_type, speed_limit_milliknots
    """

    return process_commercial_speed_limits(
        row.way_id,
        row.commercial_speed_map_json,  # Use the JSON string column
        row.tomtom_country_code,
        row.tomtom_state_code,
        row.osm_tomtom_updated_passenger_limit,
    )


def create_backend_csl_csv(table_name, data_source: SpeedLimitDataSource):
    """
    create_backend_csl_csv makes spark table that will feed into csvs used by ingestion path
    for commercial speed limits. It extracts individual commercial speed limit records from
    the source data and creates separate rows for each vehicle type and speed limit combination.
    """
    current_ms = int(round(time.time() * 1000))

    # First, get the base data with commercial speed limit maps and convert to JSON
    base_df = (
        spark.table(table_name)
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
        )
    )

    # Define a UDF to explode commercial speed limits into individual records
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
        .select("csl_record.*")
        .filter("speed_limit_milliknots IS NOT NULL")
    )

    # Add the required columns for backend CSV format
    backend_csl_df = (
        exploded_df.withColumn("org_id", lit(-1))
        .withColumn("version_id", lit(config.getCurrentPendingVersion()))
        .withColumn("dataset_version", lit(0))
        .withColumn("override_speed_limit_milliknots", col("speed_limit_milliknots"))
        .withColumn("created_at_ms", lit(current_ms))
        .withColumn("data_source", lit(data_source.value))
        .select(
            "org_id",
            "way_id",
            "version_id",
            "dataset_version",
            "override_speed_limit_milliknots",
            "created_at_ms",
            "data_source",
            "vehicle_type",
        )
    )

    # Only include updated rows - compare with existing commercial speed limit overrides
    return (
        backend_csl_df.alias("backend_csl")
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


def create_backend_csv(table_name, data_source: SpeedLimitDataSource):
    """
    create_backend_csv makes spark table that will feed into csvs used by ingestion path
    """
    current_ms = int(round(time.time() * 1000))

    backend_df = (
        spark.table(table_name)
        .filter("osm_tomtom_updated_passenger_limit IS NOT NULL")
        .withColumn("org_id", lit(-1))
        .withColumnRenamed("osm_way_id", "way_id")
        .withColumn("version_id", lit(config.getCurrentPendingVersion()))
        .withColumn("dataset_version", lit(0))
        .withColumn(
            "override_speed_limit_milliknots",
            convert_speed_to_milliknots_udf("osm_tomtom_updated_passenger_limit"),
        )
        .withColumn("created_at_ms", lit(current_ms))
        .withColumn("data_source", lit(data_source.value))
        .select(
            "org_id",
            "way_id",
            "version_id",
            "dataset_version",
            "override_speed_limit_milliknots",
            "created_at_ms",
            "data_source",
        )
    )
    # Only include updated rows
    # The additional AND filter is so we don't add any rows that only have changes b/c of commercial speed limit
    return (
        backend_df.alias("backend")
        .join(
            spark.table("deployed_global_limits_fw_tiles").alias("deployed"),
            col("backend.way_id") == col("deployed.way_id"),
            how="left",
        )
        .where(
            "deployed.way_id IS NULL or (deployed.way_id IS NOT NULL AND deployed.speed_limit_milliknots != backend.override_speed_limit_milliknots)"
        )
        .where("backend.override_speed_limit_milliknots IS NOT NULL")
        .select("backend.*")
    )


def validate_backend_csv(backend_sdf):
    """
    Validate that the backend loader csvs have no speed limits that are not 0 or >= 1000 milliknots
    before storing them to s3.
    """
    dataset_df = backend_sdf.toPandas()
    invalid_rows = dataset_df[
        (dataset_df["override_speed_limit_milliknots"] < 1000)
        & (dataset_df["override_speed_limit_milliknots"] != 0)
    ]
    if len(invalid_rows) > 0:
        raise Exception(
            f"Found {len(invalid_rows)} rows with backend speed limits overrides invalid (should be >= 1000 or 0 milliknots)"
        )


def dump_speed_map(speed_map):
    return json.dumps(speed_map)


dump_speed_map_udf = udf(dump_speed_map, StringType())


def create_tilegen_csv(table_name):
    """
    Used by tile generation code path
    """
    new_dataset_limits_df = (
        spark.table(table_name)
        .withColumnRenamed("osm_tomtom_updated_passenger_limit", "speed_limit")
        .withColumnRenamed("osm_way_id", "way_id")
        .withColumn(
            "commercial_vehicle_speed_map",
            dump_speed_map_udf("tomtom_commercial_vehicle_speed_map"),
        )
        .withColumnRenamed("tomtom_country_code", "country_code")
        .withColumnRenamed("tomtom_state_code", "state_code")
        .withColumnRenamed("data_source", "source")
        .select(
            "way_id",
            "speed_limit",
            "commercial_vehicle_speed_map",
            "country_code",
            "state_code",
            "source",
        )
    )
    spark.table(table_name).createOrReplaceTempView("tmp_fw_dataset")

    # Select currently deployed limits that are not covered by the new dataset.
    query = f"""
    select
        dep.way_id,
        dep.speed_limit_mph as speed_limit,
        null as commercial_vehicle_speed_map,
        null as country_code,
        null as state_code,
        null as source
    from deployed_global_limits_fw_tiles as dep
    left join {table_name} as new
    on dep.way_id = new.osm_way_id
    where new.osm_way_id is null
    -- Ignore deleted data science limits
    -- Since each iteration of the TomTom dataset contains new limits,
    -- we can ignore previously deployed TomTom limits.
    AND dep.data_source <> 1
    AND dep.data_source <> 2
    """
    currently_deployed_limits_df = spark.sql(query)

    # Merge deployed limits (excluding limits that will be updated by the dataset) with new dataset limits.
    return currently_deployed_limits_df.union(new_dataset_limits_df)


def create_customer_csv(table_name, org_id):
    """
    create_customer_csv takes in a table containing ("way_id, "speed_limit_mph") and generates a csv with customer overrides.
    """
    current_ms = int(round(time.time() * 1000))
    return (
        spark.table(table_name)
        .select("way_id", "speed_limit_mph")
        .withColumn("org_id", lit(org_id))
        .withColumn("version_id", lit(config.getCurrentPendingVersion()))
        .withColumn(
            "override_speed_limit_milliknots_conv",
            col("speed_limit_mph") * lit(868.97624190065),
        )
        .withColumn(
            "override_speed_limit_milliknots",
            expr("CAST(round(override_speed_limit_milliknots_conv,0) AS INTEGER)"),
        )
        .withColumn("created_at_ms", lit(current_ms))
        .withColumn("user_id", lit(0))
        .withColumn("data_source", lit(SpeedLimitDataSource.UNSET.value))
        .select(
            "org_id",
            "way_id",
            "version_id",
            "override_speed_limit_milliknots",
            "created_at_ms",
            "user_id",
            "data_source",
        )
    )


# COMMAND ----------

boto3_session = boto3.Session(
    botocore_session=dbutils.credentials.getServiceCredentialsProvider(
        "samsara-safety-map-data-sources-readwrite"
    )
)
s3 = boto3_session.resource("s3")
client = boto3_session.client("s3")
bucket = "samsara-safety-map-data-sources"


def make_global_csv_key(version, filename):
    """
    makes an s3 key for storing global speed limit override files.
    """
    return f"maptiles/speed-limit-csvs/{version}/{filename}"


def make_backend_csl_csv_key(version, filename):
    """
    makes an s3 key for storing backend commercial speed limit override files.
    """
    return f"maptiles/csl/speed-limit-csvs/{version}/{filename}"


def make_customer_csv_key(org_id, filename):
    """
    makes an s3 key for storing customr speed limit override files.
    """
    return f"maptiles/speed-limit-csvs/customer-overrides/{org_id}/{filename}"


dark_launch_csv_key = "maptiles/speed-limit-csvs/dark-launch.csv"


def persist_backend_csv(dataset_sdf):
    """
    Store backend loader csvs. This will have extra cols like org_id for ingestion.
    These csvs will be directly loaded into speedlimitsdb using the load from s3 command.
    """
    dataset_df = dataset_sdf.toPandas()

    # Partition DF into separate files
    MAX_ROWS = 200000
    list_of_dfs = [
        dataset_df.loc[i : i + MAX_ROWS - 1, :]
        for i in range(0, len(dataset_df), MAX_ROWS)
    ]

    VERSION = config.getCurrentPendingVersion()
    now_ms = int(round(time.time() * 1000))
    for i in range(0, len(list_of_dfs)):
        key = make_global_csv_key(VERSION, f"{now_ms}-{i}.csv")

        stream = io.StringIO()
        list_of_dfs[i].to_csv(stream, index=False)

        obj = s3.Object(bucket, key)
        obj.put(Body=stream.getvalue(), ACL="bucket-owner-full-control")


def persist_backend_csl_csv(dataset_sdf):
    """
    Store backend loader csvs. This will have extra cols like org_id for ingestion.
    These csvs will be directly loaded into speedlimitsdb using the load from s3 command.
    """
    dataset_df = dataset_sdf.toPandas()

    # Partition DF into separate files
    MAX_ROWS = 200000
    list_of_dfs = [
        dataset_df.loc[i : i + MAX_ROWS - 1, :]
        for i in range(0, len(dataset_df), MAX_ROWS)
    ]

    VERSION = config.getCurrentPendingVersion()
    now_ms = int(round(time.time() * 1000))
    for i in range(0, len(list_of_dfs)):
        key = make_backend_csl_csv_key(VERSION, f"{now_ms}-{i}.csv")

        stream = io.StringIO()
        list_of_dfs[i].to_csv(stream, index=False)

        obj = s3.Object(bucket, key)
        obj.put(Body=stream.getvalue(), ACL="bucket-owner-full-control")


def persist_tilegen_csv(dataset_sdf):
    """
    persists a tilegen csv and sqlitedb used for tile generation.
    """
    dataset_df = dataset_sdf.toPandas()

    local_file_path = f"/local_disk0/tmp/dataset_df.csv"

    dataset_df.to_csv(
        local_file_path, index=False, quoting=csv.QUOTE_NONE, escapechar="\\"
    )

    VERSION = config.getCurrentPendingVersion()
    key = make_global_csv_key(VERSION, "tilegen-combined.csv")

    _multi_part_upload(s3, bucket, key, local_file_path)

    # Write to sqlite db for access in tilegeneration code
    SPEED_LIMIT_EXPORT_DB = "/local_disk0/tmp/speed_limit_export.sqlite3"
    con = sqlite3.connect(SPEED_LIMIT_EXPORT_DB)
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS speed_limits;")
    cur.execute(
        """
        CREATE TABLE speed_limits (
            way_id BIGINT NOT NULL,
            speed_limit VARCHAR(20),
            commercial_vehicle_speed_map TEXT,
            country_code VARCHAR(8),
            state_code VARCHAR(2),
            source VARCHAR(10)
        );
        """
    )
    con.commit()
    CHUNKSIZE = 60000
    # Read from the local file we just wrote, not from DBFS mount
    with open(local_file_path) as csvfile:
        reader = csv.DictReader(csvfile, escapechar="\\")
        count = 0
        rows_to_write = []
        for row in reader:
            sqlite_row = (
                int(row["way_id"]),
                row["speed_limit"],
                row["commercial_vehicle_speed_map"],
                row["country_code"],
                row["state_code"],
                row["source"],
            )
            rows_to_write.append(sqlite_row)
            if count % CHUNKSIZE == 0:
                _insert_many(cur, con, rows_to_write)
                rows_to_write = []
                print(f"completed {count}")
            count += 1

        print(f"insert the last batch of rows: {count % CHUNKSIZE}")
        _insert_many(cur, con, rows_to_write)

    # Sqlite inserts get exponentially slower when an index is present on the table as the number of rows becomes large.
    # Since our use-case is write-once read-only, we can add the index after inserting all rows.
    cur.execute("CREATE INDEX way_id_idx ON speed_limits(way_id)")
    cur.close()
    print("uploading sqlitedb")
    client.upload_file(
        SPEED_LIMIT_EXPORT_DB,
        bucket,
        make_global_csv_key(VERSION, "speed_limit_export.sqlite3"),
        {"ACL": "bucket-owner-full-control"},
    )


def _insert_many(cursor, conn, rows_to_write):
    cursor.executemany(
        "INSERT OR IGNORE INTO speed_limits (way_id, speed_limit, commercial_vehicle_speed_map, country_code, state_code, source) VALUES (?, ?, ?, ?, ?, ?);",
        rows_to_write,
    )
    conn.commit()


"""
Persists the whole TomTom way data set to S3 as a sqlite
DB. Currently, this pushes out 74 GB of data and takes
more than 24 hours to run. We should only run for
debugging purposes.
"""


def persist_tomtom_to_sqlite(
    df: DataFrame,
    tile_version: str,
    is_tomtom_decoupled: bool,
    tomtom_dataset_version: str,
):
    table_name = "tomtom_full_ways"

    # Create sqlite db for access in tilegeneration code
    local_file_path = f"/local_disk0/tmp/{table_name}.sqlite3"
    columns = {
        "tomtom_way_id": "BIGINT NOT NULL",
        "wkt": "TEXT",
        "speed_limit": "VARCHAR(20)",
        "slippy_x": "INTEGER NOT NULL",
        "slippy_y": "INTEGER NOT NULL",
    }

    iterator = _time_iterator_creation(df)
    with contextlib.closing(sqlite3.connect(local_file_path)) as db:
        cur = db.cursor()
        _write_to_sqlite(db, cur, table_name, columns, iterator)

        now = time.time()
        cur.execute(
            f"CREATE INDEX slippy_coordinates_idx ON {table_name}(slippy_x, slippy_y)"
        )
        end = time.time()
        print(f"Creating db index took {(end - now) / 60} minutes")

    base_key = f"maptiles/speed-limit-csvs/{tile_version}"
    if is_tomtom_decoupled and tomtom_dataset_version != "":
        base_key = (
            f"maptiles/speed-limit-csvs/tomtom/{tile_version}/{tomtom_dataset_version}"
        )
    _multi_part_upload(
        s3, bucket, f"{base_key}/debug/{table_name}.sqlite3", local_file_path
    )


"""
Persists the OSM to TomTom matches to S3 as a
Sqlite db. This is meant for debugging purposes.
"""


def persist_osm_to_tomtom_matches(
    df: DataFrame,
    tile_version: str,
    is_tomtom_decoupled: bool,
    tomtom_dataset_version: str,
):
    table_name = "osm_to_tomtom_matches"
    local_file_path = f"/local_disk0/tmp/{table_name}.sqlite3"
    columns = {
        "osm_way_id": "BIGINT NOT NULL",
        "tomtom_way_id": "BIGINT",
        "is_selected": "INTEGER",
    }

    iterator = _time_iterator_creation(df)
    with contextlib.closing(sqlite3.connect(local_file_path)) as db:
        cur = db.cursor()
        _write_to_sqlite(db, cur, table_name, columns, iterator)

        now = time.time()
        cur.execute(f"CREATE INDEX way_id_idx ON {table_name}(osm_way_id)")
        end = time.time()
        print(f"Creating db index took {(end - now) / 60} minutes")

    # Upload Sqlite from Local to S3
    base_key = f"maptiles/speed-limit-csvs/{tile_version}"
    if is_tomtom_decoupled and tomtom_dataset_version != "":
        base_key = (
            f"maptiles/speed-limit-csvs/tomtom/{tile_version}/{tomtom_dataset_version}"
        )
    _multi_part_upload(
        s3, bucket, f"{base_key}/debug/{table_name}.sqlite3", local_file_path
    )


def _time_iterator_creation(df: DataFrame) -> Iterator[Row]:
    print("Generating dataframe iterator")
    now = time.time()
    iterator = df.toLocalIterator()
    end = time.time()
    print(f"Generating iterator took {(end-now)/ 60} minutes")
    return iterator


def _write_to_sqlite(
    db: sqlite3.Connection,
    cur: sqlite3.Cursor,
    table_name: str,
    columns: Dict[str, str],
    iterator,
):
    now = time.time()
    print("Writing data to sqlite file on disk")
    cur.execute(f"DROP TABLE IF EXISTS {table_name};")

    create_sql = _create_table_sql(table_name, columns)
    cur.execute(create_sql)

    insert_sql = _insert_sql(table_name, columns.keys())
    cur.executemany(insert_sql, iterator)
    db.commit()
    end = time.time()
    print("Completed writing data to sqlite file on disk")
    print(f"Total time: {(end - now) / 60} minutes")


# helper methods for uploading sqlite dbs to s3
def _insert_sql(table_name: str, columns: List[str]):
    question_marks = ",".join(["?"] * len(columns))
    columns_str = ",".join(columns)
    sql = (
        f"INSERT OR IGNORE INTO {table_name} ({columns_str}) VALUES ({question_marks});"
    )
    return sql


def _create_table_sql(table_name: str, columns: Dict[str, str]) -> str:
    columns_str = ",".join(
        [f"{column_name} {attrs}" for column_name, attrs in columns.items()]
    )
    create_table_sql = f"CREATE TABLE {table_name} ({columns_str});"
    return create_table_sql


def _multi_part_upload(s3, bucket, key, file_path):
    """
    We perform a multipart upload since S3 has a 5GB limit on any
    file upload. S3 has built in functionality to break up a file
    and transmit it in parallel.
    """
    print("Starting multipart upload to S3")
    now = time.time()

    obj = s3.Object(bucket, key)

    gb = 1024**3
    # Defaults: https://boto3.amazonaws.com/v1/documentation/api/latest/_modules/boto3/s3/transfer.html#TransferConfig
    config = TransferConfig(
        # increasing the chunksize from the default of 8MB to 1 GB
        multipart_chunksize=gb,
    )
    obj.upload_file(
        file_path, Config=config, ExtraArgs={"ACL": "bucket-owner-full-control"}
    )

    end = time.time()
    print(f"Completed multipart upload to s3, it took: {(end - now)/60} minutes")


def persist_dark_launch_csv(dataset_sdf):
    """
    persists a csv used for dark launching speed limits.
    """
    dataset_df = dataset_sdf.toPandas()
    dataset_df.head()

    key = dark_launch_csv_key

    stream = io.StringIO()
    dataset_df.to_csv(stream, index=False)

    obj = s3.Object(bucket, key)
    obj.put(Body=stream.getvalue(), ACL="bucket-owner-full-control")


def persist_customer_csv(dataset_sdf):
    """
    persists a csv for customer speed limit overrides. All rows in the passed
    in sdf must belong to the same org.
    """
    # Infer the org_id and assert data only exists for a single org.
    org_ids = dataset_sdf.select("org_id").distinct().rdd.map(lambda r: r[0]).collect()
    assert len(org_ids) == 1
    org_id = org_ids[0]
    print(org_id)

    dataset_df = dataset_sdf.toPandas()
    dataset_df.head()

    now_ms = int(round(time.time() * 1000))
    key = make_customer_csv_key(org_id, f"{now_ms}-0.csv")

    stream = io.StringIO()
    dataset_df.to_csv(stream, index=False)

    obj = s3.Object(bucket, key)
    obj.put(Body=stream.getvalue(), ACL="bucket-owner-full-control")


# COMMAND ----------

# MAGIC %md
# MAGIC Below functions are new for decoupled speed limits datasets integration

# COMMAND ----------

IOWA_DOT = "iowa_dot"
ML_CV = "ml_cv"
GLOBAL_OVERRIDE = "global_override"

SPEED_LIMIT_DATASET_NAME_TO_MAPMATCHED_TABLE_NAME = {
    IOWA_DOT: "regulatory_iowadot_map_matched_",
    ML_CV: "ml_speed_limit_data_created_on_",
}

SPEED_LIMIT_DATASET_NAME_TO_DB_ORG_ID = {IOWA_DOT: -2, ML_CV: -3}

SPEED_LIMIT_DATASET_NAME_TO_RESOLVED_TABLE_NAME = {
    IOWA_DOT: "regulatory_iowadot_resolved_speed_limits_",
    ML_CV: "ml_cv_resolved_speed_limits_",
}

DATASET_NAME_TO_DATASOURCE = {
    IOWA_DOT: SpeedLimitDataSource.VENDOR_IOWA_DOT,
    ML_CV: SpeedLimitDataSource.VENDOR_ML_CV,
    GLOBAL_OVERRIDE: SpeedLimitDataSource.GLOBAL_OVERRIDE,
    "tomtom": SpeedLimitDataSource.VENDOR_TOM_TOM,
}


# COMMAND ----------


def create_deployed_speed_limits_fw_tiles(org_id):
    """
    create_deployed_speed_limits_fw_tiles creates a temporary view for
    a deployed speed limits dataset from speedlimitsdb.

    Note: On Databricks for the nullable column 'proto', it is a base64 string from DB,
    but it is being displayed and used as a hex string.

    We need to convert it back to a base64 string so the delta finding logic will work
    """

    query = f"""
    CREATE OR REPLACE TEMPORARY VIEW deployed_speed_limits_fw_tiles AS
        WITH max_version_info AS (
            SELECT org_id, way_id, version_id, dataset_version
            FROM (
                SELECT org_id, way_id, version_id, dataset_version,
                    ROW_NUMBER() OVER (
                        PARTITION BY org_id, way_id
                        ORDER BY version_id DESC, dataset_version DESC
                    ) AS rn
                FROM speedlimitsdb.speed_limit_overrides
                WHERE org_id = {org_id}
            ) ranked_versions
            WHERE rn = 1
        )
        SELECT
            o.org_id,
            o.way_id,
            o.version_id,
            o.dataset_version,
            CONCAT(CAST(ROUND(o.override_speed_limit_milliknots / 868.97624190065, 0) AS INTEGER), ' mph') AS speed_limit_mph,
            o.override_speed_limit_milliknots AS speed_limit_milliknots,
            o.created_at_ms,
            o.data_source,
            base64(unhex(o.proto)) as proto
        FROM speedlimitsdb.speed_limit_overrides AS o
        INNER JOIN max_version_info AS m
            ON o.org_id = m.org_id
            AND o.way_id = m.way_id
            AND o.version_id = m.version_id
            AND o.dataset_version = m.dataset_version
        WHERE o.org_id = {org_id};
    """
    spark.sql(query)


# COMMAND ----------


def create_deployed_speed_limits_fw_tiles_with_version_cap(
    org_id, version_cap: int, dataset_version_cap: int
):
    """
    create_deployed_speed_limits_fw_tiles_with_version_cap creates a temporary view for
    a deployed speed limits dataset from speedlimitsdb with a version cap.

    This is used for ml-cv datasets to only include speed limits that are within the version cap.

    Args:
        org_id: Organization ID (e.g., -3 for ML_CV)
        version_cap: Maximum version_id to include (e.g., 34)
        dataset_version_cap: Maximum dataset_version to include (e.g., 0)
    """

    query = f"""
    CREATE OR REPLACE TEMPORARY VIEW deployed_speed_limits_fw_tiles AS
        WITH max_version_info AS (
            SELECT org_id, way_id, version_id, dataset_version
            FROM (
                SELECT org_id, way_id, version_id, dataset_version,
                    ROW_NUMBER() OVER (
                        PARTITION BY org_id, way_id
                        ORDER BY version_id DESC, dataset_version DESC
                    ) AS rn
                FROM speedlimitsdb.speed_limit_overrides
                WHERE org_id = {org_id}
                    AND version_id >= 0
                    AND (version_id < {version_cap} 
                    OR (version_id = {version_cap} AND dataset_version < {dataset_version_cap}))
            ) ranked_versions
            WHERE rn = 1
        )
        SELECT
            o.org_id,
            o.way_id,
            o.version_id,
            o.dataset_version,
            CONCAT(CAST(ROUND(o.override_speed_limit_milliknots / 868.97624190065, 0) AS INTEGER), ' mph') AS speed_limit_mph,
            o.override_speed_limit_milliknots AS speed_limit_milliknots,
            o.created_at_ms,
            o.data_source,
            base64(unhex(o.proto)) as proto
        FROM speedlimitsdb.speed_limit_overrides AS o
        INNER JOIN max_version_info AS m
            ON o.org_id = m.org_id
            AND o.way_id = m.way_id
            AND o.version_id = m.version_id
            AND o.dataset_version = m.dataset_version
        WHERE o.org_id = {org_id};
    """
    spark.sql(query)


# COMMAND ----------


def create_speed_limits_backend_csv(
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
        .withColumn("created_at_ms", lit(current_ms))
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
            "data_source",
        )
    )
    # Only include updated rows
    joined_df = backend_df.alias("backend").join(
        spark.table("deployed_speed_limits_fw_tiles").alias("deployed"),
        col("backend.way_id") == col("deployed.way_id"),
        how="left",
    )

    # Log the counts for the specified conditions
    count_null_deployed = joined_df.filter("deployed.way_id IS NULL").count()
    count_not_null_diff_speed = joined_df.filter(
        "deployed.way_id IS NOT NULL AND deployed.speed_limit_milliknots != backend.override_speed_limit_milliknots"
    ).count()
    print(f"Number of wayids to add with a new speed limit: {count_null_deployed}")
    print(
        f"Number of wayids to update with a new speed limit: {count_not_null_diff_speed}"
    )

    result_df = (
        joined_df.where(
            "deployed.way_id IS NULL or (deployed.way_id IS NOT NULL AND deployed.speed_limit_milliknots != backend.override_speed_limit_milliknots)"
        )
        .where("backend.override_speed_limit_milliknots IS NOT NULL")
        .select("backend.*")
    )
    print(f"Overall number of wayids to upsert to speedlimitsdb: {result_df.count()}")
    return result_df


# COMMAND ----------


def create_speed_limits_tilegen_csv(table_name: str) -> DataFrame:
    """
    create_speed_limits_tilegen_csv is a clone of create_tilegen_csv,
    but the query is different due to different table name

    Used by tile generation code path
    """
    new_dataset_limits_df = (
        spark.table(table_name)
        .withColumnRenamed("max_speed_limit", "speed_limit")
        .withColumnRenamed("osm_way_id", "way_id")
        .withColumn(
            "commercial_vehicle_speed_map",
            dump_speed_map_udf("tomtom_commercial_vehicle_speed_map"),
        )
        .withColumnRenamed("tomtom_country_code", "country_code")
        .withColumnRenamed("tomtom_state_code", "state_code")
        .withColumnRenamed("data_source", "source")
        .select(
            "way_id",
            "speed_limit",
            "commercial_vehicle_speed_map",
            "country_code",
            "state_code",
            "source",
        )
    )

    # Select currently deployed limits that are not covered by the new dataset.
    query = f"""
    select
        deployed.way_id,
        deployed.speed_limit_mph as speed_limit,
        null as commercial_vehicle_speed_map,
        null as country_code,
        null as state_code,
        null as source
    from deployed_speed_limits_fw_tiles as deployed
    left join {table_name} as new
    on deployed.way_id = new.osm_way_id
    where new.osm_way_id is null
    """
    currently_deployed_limits_df = spark.sql(query)

    # Merge deployed limits (excluding limits that will be updated by the dataset) with new dataset limits.
    return currently_deployed_limits_df.union(new_dataset_limits_df)


# COMMAND ----------


def make_speed_limits_csv_key(
    dataset_name: str, tile_version: int, dataset_version: int, filename: str
) -> str:
    """
    folder path example: samsara-safety-map-data-sources/maptiles/speed-limit-csvs/iowa_dot/33/1
    """
    return f"maptiles/speed-limit-csvs/{dataset_name}/{tile_version}/{dataset_version}/{filename}"


def make_speed_limits_csl_csv_key(
    dataset_name: str, tile_version: int, dataset_version: int, filename: str
) -> str:
    """
    folder path example: samsara-safety-map-data-sources/maptiles/csl/speed-limit-csvs/tomtom/35/1
    """
    return f"maptiles/csl/speed-limit-csvs/{dataset_name}/{tile_version}/{dataset_version}/{filename}"


# COMMAND ----------


def persist_speed_limits_backend_csv(
    dataset_sdf, dataset_name: str, tile_version: int, dataset_version: int
):
    dataset_df = dataset_sdf.toPandas()

    # Partition DF into separate files
    MAX_ROWS = 200000
    list_of_dfs = [
        dataset_df.loc[i : i + MAX_ROWS - 1, :]
        for i in range(0, len(dataset_df), MAX_ROWS)
    ]

    now_ms = int(round(time.time() * 1000))
    for i in range(0, len(list_of_dfs)):
        s3_key = make_speed_limits_csv_key(
            dataset_name, tile_version, dataset_version, f"{now_ms}-{i}.csv"
        )

        stream = io.StringIO()
        list_of_dfs[i].to_csv(stream, index=False)

        obj = s3.Object(bucket, s3_key)
        obj.put(Body=stream.getvalue(), ACL="bucket-owner-full-control")
        print(f"Persisted CSV {s3_key} on S3")


# COMMAND ----------


def persist_speed_limits_backend_csl_csv(
    dataset_sdf, dataset_name: str, tile_version: int, dataset_version: int
):
    """
    Store backend loader CSL csvs with incremental directory structure.
    These csvs will be directly loaded into speedlimitsdb using the load from s3 command.
    """
    dataset_df = dataset_sdf.toPandas()

    # Partition DF into separate files
    MAX_ROWS = 200000
    list_of_dfs = [
        dataset_df.loc[i : i + MAX_ROWS - 1, :]
        for i in range(0, len(dataset_df), MAX_ROWS)
    ]

    now_ms = int(round(time.time() * 1000))
    for i in range(0, len(list_of_dfs)):
        s3_key = make_speed_limits_csl_csv_key(
            dataset_name, tile_version, dataset_version, f"{now_ms}-{i}.csv"
        )

        stream = io.StringIO()
        list_of_dfs[i].to_csv(stream, index=False)

        obj = s3.Object(bucket, s3_key)
        obj.put(Body=stream.getvalue(), ACL="bucket-owner-full-control")
        print(f"Persisted CSL CSV {s3_key} on S3")


# COMMAND ----------


def persist_speed_limits_tilegen_csv(
    dataset_sdf, dataset_name: str, tile_version: int, dataset_version: int
):
    """
    persist_speed_limits_tilegen_csv is a clone of persist_tilegen_csv, but it persists the CSV and sqlite in a new S3 directory

    We need a CSV to generate a sqlite file, the sqlite file will be used by python tile generation code
    """
    dataset_df = dataset_sdf.toPandas()

    # New setup to S3 upload, according to https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5689307/How+To+Access+Cloud+Services+in+Databricks+SQS+SSM+etc.#How-To-Use-Cloud-Credentials
    boto3_session = boto3.Session(
        botocore_session=dbutils.credentials.getServiceCredentialsProvider(
            "samsara-safety-map-data-sources-readwrite"
        )
    )
    s3_client = boto3_session.client("s3")

    # /tmp/ is the temporary folder in DBX Volumes, it only persists in memory
    os.makedirs(f"/tmp/decoupled_speed_limits_{dataset_name}/", exist_ok=True)
    local_csv_path = f"/tmp/decoupled_speed_limits_{dataset_name}/dataset_df.csv"

    dataset_df.to_csv(
        local_csv_path, index=False, quoting=csv.QUOTE_NONE, escapechar="\\"
    )

    # Upload CSV to S3, and name it as tilegen-combined.csv
    csv_s3_key = make_speed_limits_csv_key(
        dataset_name, tile_version, dataset_version, "tilegen-combined.csv"
    )
    # The current _multi_part_upload func doesn't work via UC cluster, but
    # given that the tilegen-combined.csv is just ~2.5GB with the speed limits
    # covering the entire world that exceed the S3 upload limit of 5G, so let's
    # start with a simple upload for now for decoupled speed limits datasets
    s3_client.upload_file(
        local_csv_path,
        bucket,
        csv_s3_key,
        {"ACL": "bucket-owner-full-control"},
    )
    print("finshed uploading tilegen-combined.csv")

    # Write to sqlite db that will be used in python tile-generation code
    local_sqlite_path = (
        f"/tmp/decoupled_speed_limits_{dataset_name}/speed_limit_export.sqlite3"
    )

    con = sqlite3.connect(local_sqlite_path)
    cur = con.cursor()
    cur.execute(f"DROP TABLE IF EXISTS speed_limits;")
    cur.execute(
        f"""
        CREATE TABLE speed_limits (
            way_id BIGINT NOT NULL,
            speed_limit VARCHAR(20),
            commercial_vehicle_speed_map TEXT,
            country_code VARCHAR(8),
            state_code VARCHAR(2),
            source VARCHAR(10)
        );
        """
    )
    con.commit()

    CHUNKSIZE = 60000
    with open(local_csv_path) as csvfile:
        reader = csv.DictReader(csvfile, escapechar="\\")
        count = 0
        rows_to_write = []
        for row in reader:
            sqlite_row = (
                int(row["way_id"]),
                row["speed_limit"],
                row["commercial_vehicle_speed_map"],
                row["country_code"],
                row["state_code"],
                row["source"],
            )
            rows_to_write.append(sqlite_row)
            if count % CHUNKSIZE == 0:
                _insert_many(cur, con, rows_to_write)
                rows_to_write = []
                print(f"completed {count}")
            count += 1

        print(f"insert the last batch of rows: {count % CHUNKSIZE}")
        _insert_many(cur, con, rows_to_write)

    # Sqlite inserts get exponentially slower when an index is present on the table as the number of rows becomes large.
    # Since our use-case is write-once read-only, we can add the index after inserting all rows.
    cur.execute(f"CREATE INDEX way_id_idx ON speed_limits(way_id)")
    cur.close()
    print("start uploading sqlitedb")

    # Upload the sqlite to S3
    s3_client.upload_file(
        local_sqlite_path,
        bucket,
        make_speed_limits_csv_key(
            dataset_name, tile_version, dataset_version, "speed_limit_export.sqlite3"
        ),
        {"ACL": "bucket-owner-full-control"},
    )

    print("finshed uploading sqlitedb")


# COMMAND ----------


def create_decoupled_tomtom_backend_csv(
    table_name: str,
    data_source: SpeedLimitDataSource,
    tile_version: int,
    dataset_version: int,
):
    """
    create_decoupled_tomtom_backend_csv is a clone of create_backend_csv, it makes
    a backend csv for decoupled tomtom datasets.

    Here we should only store the speed limits from tomtom into speedlimitsdb,
    and not the speed limits from osm. <-- although osm/tmtom are in same org_id = -1

    Also here the dataset_version is the relative version which would refresh from zero
    every time the tile_version is updated.
    """
    current_ms = int(round(time.time() * 1000))

    backend_df = (
        spark.table(table_name)
        .filter("osm_tomtom_updated_passenger_limit IS NOT NULL")
        .withColumn("org_id", lit(-1))
        .withColumnRenamed("osm_way_id", "way_id")
        .withColumn("version_id", lit(tile_version))
        .withColumn("dataset_version", lit(dataset_version))
        .withColumn(
            "override_speed_limit_milliknots",
            convert_speed_to_milliknots_udf("osm_tomtom_updated_passenger_limit"),
        )
        .withColumn("created_at_ms", lit(current_ms))
        .withColumn("data_source", lit(data_source.value))
        .select(
            "org_id",
            "way_id",
            "version_id",
            "dataset_version",
            "override_speed_limit_milliknots",
            "created_at_ms",
            "data_source",
        )
    )
    # Only include updated rows
    # The additional AND filter is so we don't add any rows that only have changes b/c of commercial speed limit
    return (
        backend_df.alias("backend")
        .join(
            spark.table("deployed_global_limits_fw_tiles").alias("deployed"),
            col("backend.way_id") == col("deployed.way_id"),
            how="left",
        )
        .where(
            "deployed.way_id IS NULL or (deployed.way_id IS NOT NULL AND deployed.speed_limit_milliknots != backend.override_speed_limit_milliknots)"
        )
        .where("backend.override_speed_limit_milliknots IS NOT NULL")
        .select("backend.*")
    )


def create_decoupled_tomtom_tilegen_csv(table_name):
    """
    create_decoupled_tomtom_tilegen_csv is a clone of create_tilegen_csv, but it makes
    a tilegen csv for decoupled tomtom datasets.
    """
    new_dataset_limits_df = (
        spark.table(table_name)
        .withColumnRenamed("osm_tomtom_updated_passenger_limit", "speed_limit")
        .withColumnRenamed("osm_way_id", "way_id")
        .withColumn(
            "commercial_vehicle_speed_map",
            dump_speed_map_udf("tomtom_commercial_vehicle_speed_map"),
        )
        .withColumnRenamed("tomtom_country_code", "country_code")
        .withColumnRenamed("tomtom_state_code", "state_code")
        .withColumnRenamed("data_source", "source")
        .select(
            "way_id",
            "speed_limit",
            "commercial_vehicle_speed_map",
            "country_code",
            "state_code",
            "source",
        )
    )
    spark.table(table_name).createOrReplaceTempView("tmp_fw_dataset")

    # Select currently deployed limits that are not covered by the new dataset.
    query = f"""
    select
        dep.way_id,
        dep.speed_limit_mph as speed_limit,
        null as commercial_vehicle_speed_map,
        null as country_code,
        null as state_code,
        null as source
    from deployed_global_limits_fw_tiles as dep
    left join {table_name} as new
    on dep.way_id = new.osm_way_id
    where new.osm_way_id is null
    -- Ignore deleted data science limits
    -- Since each iteration of the TomTom dataset contains new limits,
    -- we can ignore previously deployed TomTom limits.
    AND dep.data_source <> 1
    AND dep.data_source <> 2
    """
    currently_deployed_limits_df = spark.sql(query)

    # Merge deployed limits (excluding limits that will be updated by the dataset) with new dataset limits.
    return currently_deployed_limits_df.union(new_dataset_limits_df)
